"""FastAPI 应用入口：展品登记、读数上报（幂等 + 存证收据）、剂量时段查询、展陈开放日历、重复档案归并、RFID 盘点对账。"""
from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.responses import FileResponse

from .calendar import (
    CalendarConfig,
    delete_calendar,
    load_calendar,
    open_intervals,
    save_calendar,
)
from .corrections import correct_receipt, voids_for_rows
from .db import get_conn, init_db
from .arbitration import (
    arbitrate,
    authoritative_points,
    build_timeline_out,
    list_versions,
    load_priority_config,
    put_priority_config,
)
from .diagnose import build_events_from_groups, intersect_window, sort_events
from .dose import compute_dose
from .evacuation import EvacuationGraph, EvacuationGraphError, plan_evacuation
from .inventory import load_round, reconcile_round
from .merge import commit_merge, create_preview, get_canonical_or_404, resolve_canonical
from .receipts import (
    ChainVerification,
    load_chain,
    mint_receipt,
    verify_chain_rows,
)
from .schemas import (
    AuthoritativeTimelineOut,
    CalendarConfigIn,
    CalendarExceptionDayOut,
    CalendarOut,
    CalendarWeeklyWindowOut,
    CalendarWindowOut,
    ChainFailureOut,
    ChainReceiptsOut,
    ChainVerifyOut,
    DiagnoseRequest,
    DiagnoseResponse,
    DiagnosisEventOut,
    DoseResponse,
    EvacuationExhibitOut,
    EvacuationLegOut,
    EvacuationRequest,
    EvacuationResponse,
    ExhibitCreate,
    ExhibitOut,
    IngestResponse,
    InventoryRoundOut,
    MergeCommitOut,
    MergeCommitRequest,
    MergePreviewOut,
    MergePreviewRequest,
    ReadingBatch,
    ReadingCorrectionOut,
    ReadingCorrectionRequest,
    ReadingListOut,
    ReadingOut,
    ReadingResult,
    ReceiptContentOut,
    ReceiptListOut,
    ReceiptOut,
    ReconcileRequest,
    ReconcileResponse,
    SnapshotListOut,
    SnapshotOut,
    SnapshotVerifyOut,
    SourcePriorityConfigOut,
    SourcePriorityPut,
    ensure_aware,
    format_local_time,
    parse_local_time,
)
from .snapshots import (
    cleanup_staging,
    create_snapshot,
    get_snapshot,
    list_snapshots,
    snapshot_db_path,
    verify_snapshot,
)


@asynccontextmanager
async def lifespan(_: FastAPI) -> Iterator[None]:
    init_db()
    # 清理上次进程崩溃可能遗留的快照临时文件（*.tmp 不出现在列表中）。
    cleanup_staging()
    yield


app = FastAPI(
    title="馆藏展品光照剂量核算 API",
    version="1.9.0",
    description="登记展品光照上限，接收乱序/补传照度读数，按梯形法累计 lux·h，缺测段不插值；"
                "支持展陈开放日历（IANA 时区 + 每周时段 + 日期例外），剂量可按开放时段裁剪核算；"
                "支持重复展品档案预演归并，来源 ID 归并后成为目标 ID 的永久别名；"
                "支持基于完整时间线的照度采集质量诊断（gap / jump / stuck / timestamp_conflict）；"
                "支持 RFID 馆藏盘点对账（固定窗口内按区域保留最高 RSSI、以最后一个有事件窗口"
                "定位、信号差不足阈值记为位置不明；轮次与规范化请求摘要绑定，重放原结果、"
                "冲突请求 409、并发只固化一份结果；对账不改读数与收据链）；"
                "支持照度上报批次更正：作废存证收据中实际插入且仍归属该档案的读数，"
                "作废与更正记录在同一事务内幂等落库（request_key 重放原结果、换目标 409），"
                "收据链/归并状态/读数归属变化整次 409；收据内容、序号与链哈希不变，"
                "剂量/诊断/读数列表立即排除已作废读数；"
                "支持展品应急撤离编排（有向通道图 + 整数通行秒数 + 并发容量 + 半开封闭时段，"
                "按优先级降序/规范 ID 升序逐件做时变最早到达搜索，占用按 [进入,离开) 计数，"
                "容量满或封闭时等待到首个可行时刻，同时刻按完整路径字典序决胜；"
                "编排为纯只读计算，不写读数、日历、盘点轮次或收据链）；"
                "支持多来源读数仲裁：读数可携带来源 ID 且同时刻多来源读数全部留存，"
                "规范档案维护只增不改、带生效时刻的来源优先级版本（更新须携带 "
                "expected_version，过期 409），按采样时刻适用版本取最高优先级为权威值，"
                "最高级并列且照度不同则该时刻保持冲突、不进入剂量与诊断时间线；"
                "优先级调整只从生效时刻起影响重算，历史收据与原始读数不改写；"
                "归并时合并两边来源配置、冲突逐条裁决不得静默覆盖，"
                "并提供权威时间线查询（旧请求不带 source_id 行为不变）；"
                "每次批量上报在写入读数的同一事务内生成 SHA-256 链式存证收据"
                "（并发下序号唯一连续，失败事务不留收据），支持收据查询与整链校验，"
                "档案归并保留双方既有链、新上报继续接入目标原链；"
                "支持可验证的 SQLite 在线快照（sqlite3 backup 事务一致副本、"
                "临时文件 + SHA-256 清单 + 原子重命名发布、只读连接执行 "
                "integrity_check / foreign_key_check 校验）。",
    lifespan=lifespan,
)


# ---------- 工具 ----------

def to_iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def request_ts_iso(dt: datetime) -> str:
    """请求读数采样时刻的规范化形式：UTC、'Z' 结尾的 ISO 8601。"""
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def round6(x: float) -> float:
    return round(x, 6)


def exhibit_out(row: sqlite3.Row, requested_id: str | None = None) -> ExhibitOut:
    rid = requested_id or row["id"]
    return ExhibitOut(
        requested_id=rid,
        canonical_id=row["id"],
        name=row["name"],
        dose_limit_luxh=row["dose_limit_luxh"],
        max_interval_seconds=row["max_interval_seconds"],
        created_at=row["created_at"],
    )


# ---------- 基础 ----------

@app.get("/", tags=["meta"])
def root() -> dict:
    return {
        "service": "馆藏展品光照剂量核算 API",
        "docs": "/docs",
        "health": "/healthz",
    }


@app.get("/healthz", tags=["meta"])
def healthz() -> dict:
    return {"status": "ok"}


# ---------- 展品 ----------

@app.post("/exhibits", status_code=201, response_model=ExhibitOut, tags=["exhibits"])
def create_exhibit(payload: ExhibitCreate, conn: sqlite3.Connection = Depends(get_conn)) -> ExhibitOut:
    """登记展品：累计光照上限（lux·h）与最大采样间隔（秒）。"""
    exhibit_id = payload.id or uuid.uuid4().hex
    if payload.id is not None:
        alias = conn.execute(
            "SELECT canonical_id FROM exhibit_aliases WHERE alias_id = ?", (exhibit_id,)
        ).fetchone()
        if alias is not None:
            raise HTTPException(
                status_code=409,
                detail=f"展品 ID {exhibit_id} 已永久归并入档案 {alias['canonical_id']}，不可复用",
            )
    try:
        conn.execute(
            "INSERT INTO exhibits (id, name, dose_limit_luxh, max_interval_seconds) VALUES (?, ?, ?, ?)",
            (exhibit_id, payload.name, payload.dose_limit_luxh, payload.max_interval_seconds),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.rollback()
        raise HTTPException(status_code=409, detail=f"展品 ID 已存在: {exhibit_id}")
    return exhibit_out(get_canonical_or_404(conn, exhibit_id))


@app.get("/exhibits", response_model=list[ExhibitOut], tags=["exhibits"])
def list_exhibits(conn: sqlite3.Connection = Depends(get_conn)) -> list[ExhibitOut]:
    rows = conn.execute("SELECT * FROM exhibits ORDER BY created_at, id").fetchall()
    return [exhibit_out(r) for r in rows]


@app.get("/exhibits/{exhibit_id}", response_model=ExhibitOut, tags=["exhibits"])
def get_exhibit(exhibit_id: str, conn: sqlite3.Connection = Depends(get_conn)) -> ExhibitOut:
    return exhibit_out(get_canonical_or_404(conn, exhibit_id), requested_id=exhibit_id)


# ---------- 展陈开放日历 ----------

def calendar_out(conn: sqlite3.Connection, requested_id: str, canonical_id: str) -> CalendarOut:
    row = conn.execute(
        "SELECT updated_at FROM exhibit_calendars WHERE exhibit_id = ?", (canonical_id,)
    ).fetchone()
    assert row is not None  # 调用方已确认日历存在
    cfg = load_calendar(conn, canonical_id)
    assert cfg is not None
    return CalendarOut(
        requested_id=requested_id,
        canonical_id=canonical_id,
        timezone=cfg.timezone,
        weekly=[
            CalendarWeeklyWindowOut(weekday=wd, start=format_local_time(s), end=format_local_time(e))
            for wd in sorted(cfg.weekly)
            for s, e in cfg.weekly[wd]
        ],
        exceptions=[
            CalendarExceptionDayOut(
                date=day.isoformat(),
                windows=[CalendarWindowOut(start=format_local_time(s), end=format_local_time(e))
                         for s, e in cfg.exceptions[day]],
            )
            for day in sorted(cfg.exceptions)
        ],
        updated_at=row["updated_at"],
    )


@app.put("/exhibits/{exhibit_id}/calendar", response_model=CalendarOut, tags=["calendar"])
def put_calendar(
    exhibit_id: str,
    payload: CalendarConfigIn,
    conn: sqlite3.Connection = Depends(get_conn),
) -> CalendarOut:
    """整体设置（全量替换）展品的展陈开放日历。

    - ``timezone`` 为 IANA 时区名；``weekly`` 按本地星期几（1=周一 … 7=周日）
      给出每周开放时段；``exceptions`` 按本地日期整体替换当日每周规则
      （``windows`` 为空表示当日闭馆）；
    - 时段内 ``end`` 不晚于 ``start`` 表示跨午夜到次日同一本地时刻；
      重叠或相接的时段在展开时自动合并；
    - 夏令时展开：不存在的边界顺延到首个有效时刻；重复边界的开始取较早
      瞬间、结束取较晚瞬间；
    - 请求 ID 若是已归并档案的别名，自动解析到规范档案。
    """
    exhibit = get_canonical_or_404(conn, exhibit_id)
    canonical_id = exhibit["id"]

    cfg = CalendarConfig(timezone=payload.timezone)
    for w in payload.weekly:
        win = (parse_local_time(w.start), parse_local_time(w.end))
        day_wins = cfg.weekly.setdefault(w.weekday, [])
        if win not in day_wins:
            day_wins.append(win)
    for exc in payload.exceptions:
        wins: list[tuple[int, int]] = []
        for w in exc.windows:
            win = (parse_local_time(w.start), parse_local_time(w.end))
            if win not in wins:
                wins.append(win)
        cfg.exceptions[exc.date] = wins

    save_calendar(conn, canonical_id, cfg)
    conn.commit()
    return calendar_out(conn, exhibit_id, canonical_id)


@app.get("/exhibits/{exhibit_id}/calendar", response_model=CalendarOut, tags=["calendar"])
def get_calendar(exhibit_id: str, conn: sqlite3.Connection = Depends(get_conn)) -> CalendarOut:
    """读取展品的展陈开放日历；未配置返回 404。别名自动解析到规范档案。"""
    exhibit = get_canonical_or_404(conn, exhibit_id)
    if load_calendar(conn, exhibit["id"]) is None:
        raise HTTPException(status_code=404, detail=f"展品未配置展陈开放日历: {exhibit_id}")
    return calendar_out(conn, exhibit_id, exhibit["id"])


@app.delete("/exhibits/{exhibit_id}/calendar", status_code=204, tags=["calendar"])
def remove_calendar(exhibit_id: str, conn: sqlite3.Connection = Depends(get_conn)) -> Response:
    """删除展品的展陈开放日历（幂等）；删除后 calendar=display 查询将返回 409。"""
    exhibit = get_canonical_or_404(conn, exhibit_id)
    delete_calendar(conn, exhibit["id"])
    conn.commit()
    return Response(status_code=204)


# ---------- 读数上报（幂等 + 存证收据） ----------

@app.post("/exhibits/{exhibit_id}/readings", response_model=IngestResponse, tags=["readings"])
def ingest_readings(
    exhibit_id: str,
    batch: ReadingBatch,
    conn: sqlite3.Connection = Depends(get_conn),
) -> IngestResponse:
    """批量上报照度读数，支持乱序与补传；同一事务内生成链式存证收据。

    请求 ID 若是已归并档案的别名，自动解析到规范档案。

    读数 ID 幂等：同 ID 同数据（含 source_id）重复上报记为 duplicate，不重复
    计入；同 ID 不同数据（时间、照度或来源任一不同）记为 conflict，拒绝写入
    并在结果中标出。

    每条读数可携带 ``source_id``（采集器 / 传感器来源）：同一展品同一采样
    时刻的不同来源读数**全部留存**，互不覆盖；是否进入权威剂量/诊断时间线
    由来源优先级版本在查询时仲裁决定。不带 ``source_id`` 的旧请求行为不变。

    存证：收据按稳定规范序列化记录请求内容与逐条结果，并以 SHA-256
    链哈希衔接上一张收据；并发上报在写锁内分配序号，形成唯一连续顺序；
    事务失败时读数与收据一起回滚，不留收据。
    """
    # BEGIN IMMEDIATE 立即拿写锁：串行化并发上报，收据序号在锁内分配，
    # 保证链上序号唯一且连续；任何失败都整体回滚（读数与收据同生共死）。
    conn.execute("BEGIN IMMEDIATE")
    try:
        exhibit = get_canonical_or_404(conn, exhibit_id)
        canonical_id = exhibit["id"]

        results: list[ReadingResult] = []
        inserted = duplicates = conflicts = 0
        for r in batch.readings:
            ts = r.ts.timestamp()
            try:
                conn.execute(
                    "INSERT INTO readings (exhibit_id, reading_id, ts, lux, source_id) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (canonical_id, r.id, ts, r.lux, r.source_id),
                )
                status = "inserted"
                inserted += 1
            except sqlite3.IntegrityError:
                existing = conn.execute(
                    "SELECT ts, lux, source_id FROM readings "
                    "WHERE exhibit_id = ? AND reading_id = ?",
                    (canonical_id, r.id),
                ).fetchone()
                if (
                    existing is not None
                    and existing["ts"] == ts
                    and existing["lux"] == r.lux
                    and existing["source_id"] == r.source_id
                ):
                    status = "duplicate"
                    duplicates += 1
                else:
                    status = "conflict"
                    conflicts += 1
            results.append(ReadingResult(id=r.id, status=status))

        # 收据与读数同一事务落库：任一失败整体回滚，失败事务不留收据。
        # 仅携带来源时才写入 source_id 键：旧版收据缺该键，哈希仍逐字节可复现。
        receipt_readings = []
        for r in batch.readings:
            item = {"id": r.id, "ts": request_ts_iso(r.ts), "lux": r.lux}
            if r.source_id is not None:
                item["source_id"] = r.source_id
            receipt_readings.append(item)
        # 收据与读数同一事务落库：任一失败整体回滚，失败事务不留收据
        receipt_row = mint_receipt(
            conn,
            chain_id=canonical_id,
            requested_id=exhibit_id,
            readings=receipt_readings,
            results=[{"id": x.id, "status": x.status} for x in results],
            inserted=inserted,
            duplicates=duplicates,
            conflicts=conflicts,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return IngestResponse(
        requested_id=exhibit_id,
        canonical_id=canonical_id,
        inserted=inserted,
        duplicates=duplicates,
        conflicts=conflicts,
        results=results,
        receipt=receipt_out(receipt_row),
    )


@app.get("/exhibits/{exhibit_id}/readings", response_model=ReadingListOut, tags=["readings"])
def list_readings(
    exhibit_id: str,
    start: datetime | None = Query(default=None, description="起始时刻（含），ISO 8601"),
    end: datetime | None = Query(default=None, description="结束时刻（含），ISO 8601"),
    limit: int = Query(default=1000, ge=1, le=100000),
    conn: sqlite3.Connection = Depends(get_conn),
) -> ReadingListOut:
    """按时间顺序列出读数，便于核对与调试；别名自动解析到规范档案。"""
    exhibit = get_canonical_or_404(conn, exhibit_id)
    canonical_id = exhibit["id"]
    sql = ("SELECT * FROM readings WHERE exhibit_id = ? AND voided_at IS NULL")
    params: list = [canonical_id]
    if start is not None:
        sql += " AND ts >= ?"
        params.append(ensure_aware(start).timestamp())
    if end is not None:
        sql += " AND ts <= ?"
        params.append(ensure_aware(end).timestamp())
    sql += " ORDER BY ts, reading_id LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return ReadingListOut(
        requested_id=exhibit_id,
        canonical_id=canonical_id,
        readings=[
            ReadingOut(id=r["reading_id"], ts=to_iso(r["ts"]), lux=r["lux"],
                       source_id=r["source_id"], received_at=r["received_at"])
            for r in rows
        ],
    )


# ---------- 来源优先级版本（多来源仲裁） ----------

@app.put("/exhibits/{exhibit_id}/source-priorities",
         response_model=SourcePriorityConfigOut, tags=["sources"])
def put_source_priorities(
    exhibit_id: str,
    payload: SourcePriorityPut,
    conn: sqlite3.Connection = Depends(get_conn),
) -> SourcePriorityConfigOut:
    """新增一个**来源优先级版本**（只增不改）。

    - 版本携带 ``effective_at`` 生效时刻（含端点）：仅对该时刻及以后采样的
      读数生效，更早的历史采样仍按旧版本仲裁；历史读数与存证收据**绝不
      改写**，调整优先级只从生效时刻起影响查询重算；
    - 必须携带 ``expected_version``（客户端所见当前最新版本号，从未配置为
      0）；与服务端当前版本不一致返回 **409**，需重新读取配置后重试；
    - ``priorities`` 为该版本的**完整**来源优先级清单（整体写入，不增量
      合并）：``priority`` 越大越权威，同值为并列档；同版本内 source_id 不
      得重复（422），同一生效时刻也不得重复建版本（422）；
    - 别名访问自动解析到规范档案。
    """
    return put_priority_config(conn, exhibit_id, payload)


@app.get("/exhibits/{exhibit_id}/source-priorities",
         response_model=SourcePriorityConfigOut, tags=["sources"])
def get_source_priorities(
    exhibit_id: str,
    conn: sqlite3.Connection = Depends(get_conn),
) -> SourcePriorityConfigOut:
    """读取来源优先级配置（全部历史版本，按生效时刻升序）；从未配置时
    ``current_version=0``、``versions=[]``。别名自动解析到规范档案。"""
    return load_priority_config(conn, exhibit_id)


@app.get("/exhibits/{exhibit_id}/authoritative-timeline",
         response_model=AuthoritativeTimelineOut, tags=["sources"])
def get_authoritative_timeline(
    exhibit_id: str,
    start: datetime | None = Query(default=None, description="起始时刻（含），ISO 8601"),
    end: datetime | None = Query(default=None, description="结束时刻（含），ISO 8601"),
    limit: int = Query(default=1000, ge=1, le=100000),
    conn: sqlite3.Connection = Depends(get_conn),
) -> AuthoritativeTimelineOut:
    """逐采样时刻返回**多来源仲裁结果**（权威时间线）。

    - ``authoritative``：按采样时刻生效的优先级版本仲裁出唯一权威照度；
    - ``conflict``：最高优先级一档并列来源照度不一致，该时刻保持冲突，
      **不进入剂量与诊断时间线**；``lux_span`` 给出照度跨度、
      ``candidates`` 给出该时刻留存的全部原始读数用于核查；
    - ``legacy``：该时刻没有任何生效优先级版本，保持旧行为（原始读数各自
      成点；原始照度不一致时 ``lux`` 为 null）；
    - 未列入生效版本的来源（含未标注 source_id 的旧读数）归入最低档，低于
      任何已列来源；同一时刻同照度的并列读数合并为一个权威点，
      ``reading_ids`` 保留全部并列读数 ID；
    - 别名访问自动解析到规范档案。
    """
    start_s = ensure_aware(start).timestamp() if start is not None else None
    end_s = ensure_aware(end).timestamp() if end is not None else None
    if start_s is not None and end_s is not None and end_s <= start_s:
        raise HTTPException(status_code=422, detail="end 必须晚于 start")
    return build_timeline_out(conn, exhibit_id, start_s, end_s, limit)


# ---------- 照度上报存证（收据链查询与整链校验） ----------

def receipt_out(row: sqlite3.Row,
                void_map: dict[tuple[str, int], list[dict]] | None = None) -> ReceiptOut:
    voids = (void_map or {}).get((row["chain_id"], row["seq"]), [])
    return ReceiptOut(
        chain_id=row["chain_id"],
        seq=row["seq"],
        receipt_id=row["receipt_id"],
        prev_hash=row["prev_hash"],
        content_hash=row["content_hash"],
        chain_hash=row["chain_hash"],
        created_at=row["created_at"],
        content=ReceiptContentOut(**json.loads(row["payload_json"])),
        voids=voids,
    )


def _chain_verify_out(chain_id: str, verdict: ChainVerification) -> ChainVerifyOut:
    return ChainVerifyOut(
        chain_id=chain_id,
        ok=verdict.ok,
        checked=verdict.checked,
        total=verdict.total,
        first_failure=ChainFailureOut(**verdict.first_failure)
        if verdict.first_failure is not None else None,
    )


@app.get("/exhibits/{exhibit_id}/receipts", response_model=ReceiptListOut, tags=["receipts"])
def list_receipts(
    exhibit_id: str,
    limit: int = Query(default=1000, ge=1, le=100000),
    conn: sqlite3.Connection = Depends(get_conn),
) -> ReceiptListOut:
    """按链内序号升序列出规范档案的存证收据；别名自动解析到规范档案。"""
    exhibit = get_canonical_or_404(conn, exhibit_id)
    canonical_id = exhibit["id"]
    rows = conn.execute(
        "SELECT * FROM ingest_receipts WHERE chain_id = ? ORDER BY seq ASC LIMIT ?",
        (canonical_id, limit),
    ).fetchall()
    void_map = voids_for_rows(conn, [(r["chain_id"], r["seq"]) for r in rows])
    return ReceiptListOut(
        requested_id=exhibit_id,
        canonical_id=canonical_id,
        chain_id=canonical_id,
        receipts=[receipt_out(r, void_map) for r in rows],
    )


@app.get("/exhibits/{exhibit_id}/receipts/verify", response_model=ChainVerifyOut,
         tags=["receipts"])
def verify_exhibit_receipt_chain(
    exhibit_id: str,
    conn: sqlite3.Connection = Depends(get_conn),
) -> ChainVerifyOut:
    """整链校验规范档案的收据链：序号连续、prev_hash 衔接、内容哈希与链哈希复现。

    返回链标识、已检查且通过的收据数量，以及首个断链或内容不符的位置。
    """
    exhibit = get_canonical_or_404(conn, exhibit_id)
    canonical_id = exhibit["id"]
    return _chain_verify_out(canonical_id, verify_chain_rows(load_chain(conn, canonical_id)))


@app.post("/exhibits/{exhibit_id}/receipts/corrections",
          response_model=ReadingCorrectionOut, tags=["receipts"])
def void_ingest_batch(
    exhibit_id: str,
    payload: ReadingCorrectionRequest,
    conn: sqlite3.Connection = Depends(get_conn),
) -> ReadingCorrectionOut:
    """照度上报批次更正：作废某张存证收据中实际插入的读数。

    - 请求 ID 可用归并别名（自动解析到规范档案）；服务只作废该收据中
      ``inserted`` 且作废时**仍归属该规范档案、内容未变、尚未作废**的读数，
      任一条不满足（收据链/归并状态/读数归属已变化、收据已被更正）整次 **409**，
      不做部分作废；
    - 作废标记与更正记录在**同一个 SQLite 事务**（BEGIN IMMEDIATE）内幂等落库；
      ``request_key`` 为幂等键，同键重放首次结果（``already_committed=true``），
      换目标（规范档案/收据/原因不同）→ 409；
    - 收据内容、序号与链哈希**保持不变**（查询收据时在 ``voids`` 中标出作废）；
      作废读数立即退出剂量查询、采集质量诊断与读数列表，并按剩余时间线重算。
    """
    return correct_receipt(conn, exhibit_id, payload)


@app.get("/exhibits/{exhibit_id}/receipts/{seq}", response_model=ReceiptOut, tags=["receipts"])
def get_receipt(
    exhibit_id: str,
    seq: int,
    conn: sqlite3.Connection = Depends(get_conn),
) -> ReceiptOut:
    """按链内序号查询单张存证收据；别名自动解析到规范档案。"""
    exhibit = get_canonical_or_404(conn, exhibit_id)
    row = conn.execute(
        "SELECT * FROM ingest_receipts WHERE chain_id = ? AND seq = ?",
        (exhibit["id"], seq),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"收据不存在: {exhibit_id} 第 {seq} 张")
    void_map = voids_for_rows(conn, [(row["chain_id"], row["seq"])])
    return receipt_out(row, void_map)


@app.get("/receipts/chains/{chain_id}", response_model=ChainReceiptsOut, tags=["receipts"])
def list_chain_receipts(
    chain_id: str,
    limit: int = Query(default=1000, ge=1, le=100000),
    conn: sqlite3.Connection = Depends(get_conn),
) -> ChainReceiptsOut:
    """按链标识直接读取收据链（不解析别名）。

    档案归并后来源档案的既有链原样保留（来源 ID 已成为别名）；用本接口
    可独立核查归并前的来源链与目标链。
    """
    rows = conn.execute(
        "SELECT * FROM ingest_receipts WHERE chain_id = ? ORDER BY seq ASC LIMIT ?",
        (chain_id, limit),
    ).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"收据链不存在: {chain_id}")
    void_map = voids_for_rows(conn, [(r["chain_id"], r["seq"]) for r in rows])
    return ChainReceiptsOut(
        chain_id=chain_id, receipts=[receipt_out(r, void_map) for r in rows]
    )


@app.get("/receipts/chains/{chain_id}/verify", response_model=ChainVerifyOut, tags=["receipts"])
def verify_chain_by_id(
    chain_id: str,
    conn: sqlite3.Connection = Depends(get_conn),
) -> ChainVerifyOut:
    """按链标识整链校验（不解析别名）；归并后来源链与目标链可分别校验。"""
    rows = load_chain(conn, chain_id)
    if not rows:
        raise HTTPException(status_code=404, detail=f"收据链不存在: {chain_id}")
    return _chain_verify_out(chain_id, verify_chain_rows(rows))


# ---------- 照度采集质量诊断（只读） ----------

@app.post("/exhibits/{exhibit_id}/readings/diagnose", response_model=DiagnoseResponse,
          response_model_exclude_unset=True, tags=["readings"])
def diagnose_readings(
    exhibit_id: str,
    payload: DiagnoseRequest,
    conn: sqlite3.Connection = Depends(get_conn),
) -> DiagnoseResponse:
    """照度采集质量诊断：识别 gap / jump / stuck / timestamp_conflict 事件。

    - 诊断先在规范档案的**原始读数**上按采样时刻做**多来源仲裁**（与剂量
      核算同一份来源优先级版本）：仲裁出唯一权威照度的时刻合并为时间线上
      的一个点（并列读数 ID 全部保留）；**最高优先级并列且照度不同的仲裁
      冲突时刻保持冲突、不进入诊断时间线**，链在其两侧中断（冲突本身请用
      权威时间线查询核查，不作为采集质量事件重复报告）；
    - 对没有生效优先级版本的时刻（含旧请求不带 source_id 的全部数据）保持
      旧行为：同时刻照度相同的多条读数先**合并**为一个时间点，同时刻出现
      **不同照度**则产生 ``timestamp_conflict``，该时刻不落点并中断连续链；
    - 对无歧义读数：相邻间隔**超过**展品 ``max_interval_seconds`` 记为
      ``gap``；间隔合规但变化速率 ``|Δlux|/Δt`` 超过
      ``max_rate_lux_per_second`` 记为 ``jump``；相邻照度差均不超过
      ``stuck_tolerance_lux`` 且首末累计时长达到 ``min_stuck_seconds`` 的
      最大连续段记为 ``stuck``；gap、jump、时间冲突与仲裁冲突都会截断
      stuck 链；
    - 事件**先基于规范档案的完整时间线形成**，再筛选与查询窗口相交的结果：
      缩窄窗口只影响哪些事件被返回，**不改变任何事件的原始边界与相关读数**；
    - 结果按 (起点, 类型) 稳定排序；每个事件给出边界、相关读数 ID 以及
      间隔 / 速率 / 照度跨度等对应指标；
    - 请求 ID 若是已归并档案的别名，自动解析到规范档案；诊断为纯只读操作，
      不写库，也不影响剂量核算结果。
    """
    exhibit = get_canonical_or_404(conn, exhibit_id)
    canonical_id = exhibit["id"]
    start_s = ensure_aware(payload.start).timestamp()
    end_s = ensure_aware(payload.end).timestamp()
    if end_s <= start_s:
        raise HTTPException(status_code=422, detail="end 必须晚于 start")

    # 完整时间线：诊断事件必须在全部已存（未作废）读数上形成，不能只取窗口内数据。
    # 先按生效来源优先级版本仲裁，仲裁冲突时刻不进时间线；无生效版本的时刻
    # 为 legacy，沿用同值合并 / 异值 timestamp_conflict 的旧行为。
    rows = conn.execute(
        "SELECT reading_id, ts, lux, source_id FROM readings "
        "WHERE exhibit_id = ? AND voided_at IS NULL ORDER BY ts, reading_id",
        (canonical_id,),
    ).fetchall()
    groups = arbitrate(
        [(r["reading_id"], r["ts"], r["lux"], r["source_id"]) for r in rows],
        list_versions(conn, canonical_id),
    )
    events = build_events_from_groups(
        groups,
        max_interval_s=exhibit["max_interval_seconds"],
        max_rate_lux_per_s=payload.max_rate_lux_per_second,
        stuck_tol_lux=payload.stuck_tolerance_lux,
        min_stuck_s=payload.min_stuck_seconds,
    )
    events = sort_events(intersect_window(events, start_s, end_s))

    events_out: list[DiagnosisEventOut] = []
    for e in events:
        kwargs: dict = dict(
            type=e.type,
            start=to_iso(e.start),
            end=to_iso(e.end),
            reading_ids=e.reading_ids,
        )
        if e.interval_seconds is not None:
            kwargs["interval_seconds"] = round6(e.interval_seconds)
        if e.rate_lux_per_second is not None:
            kwargs["rate_lux_per_second"] = round6(e.rate_lux_per_second)
        if e.duration_seconds is not None:
            kwargs["duration_seconds"] = round6(e.duration_seconds)
        if e.lux_span is not None:
            kwargs["lux_span"] = round6(e.lux_span)
        events_out.append(DiagnosisEventOut(**kwargs))

    return DiagnoseResponse(
        requested_id=exhibit_id,
        canonical_id=canonical_id,
        start=to_iso(start_s),
        end=to_iso(end_s),
        max_interval_seconds=exhibit["max_interval_seconds"],
        max_rate_lux_per_second=round6(payload.max_rate_lux_per_second),
        stuck_tolerance_lux=round6(payload.stuck_tolerance_lux),
        min_stuck_seconds=round6(payload.min_stuck_seconds),
        events=events_out,
    )


# ---------- 剂量核算 ----------

@app.get("/exhibits/{exhibit_id}/dose", response_model=DoseResponse,
         response_model_exclude_unset=True, tags=["dose"])
def query_dose(
    exhibit_id: str,
    start: datetime = Query(..., description="时段起点（含），ISO 8601"),
    end: datetime = Query(..., description="时段终点（含），ISO 8601"),
    calendar: Literal["display"] | None = Query(
        default=None,
        description="calendar=display：按展陈开放日历裁剪核算；不带该参数保持原有口径",
    ),
    conn: sqlite3.Connection = Depends(get_conn),
) -> DoseResponse:
    """时段剂量核算：窗口剂量、全历史剩余额度、首次越线时刻、窗口缺测时长。

    - known_dose_luxh / unknown_seconds / covered_seconds 只描述所查窗口，
      窗口起止处读取窗口外最近相邻读数，按线性照度裁剪有效梯形段；
    - remaining_luxh / exceeded / first_exceeded_at 依据截至窗口末的
      全历史累计计算，首次越线时刻不因缩窄窗口而改变；
    - 剂量在查询时基于全部已存读数按**来源优先级版本实时仲裁**后重算：
      补传读数或调整优先级版本（仅对新版本生效时刻起的采样）立即生效；
      最高优先级并列且照度不同的冲突时刻保持歧义、不进入积分（两侧读数
      不跨接）；没有生效版本、或旧请求不带 source_id 时行为与旧版一致；
    - 请求 ID 若是已归并档案的别名，自动解析到规范档案；
    - ``calendar=display`` 时仅在开放区间内裁剪梯形积分：窗口内闭馆时长
      单列 closed_seconds，covered_seconds、unknown_seconds、closed_seconds
      之和等于窗口时长；剩余额度与首次越线也按截至窗口末的开放历史计算；
      未配置日历返回 409。
    """
    exhibit = get_canonical_or_404(conn, exhibit_id)
    canonical_id = exhibit["id"]
    start_s = ensure_aware(start).timestamp()
    end_s = ensure_aware(end).timestamp()
    if end_s <= start_s:
        raise HTTPException(status_code=422, detail="end 必须晚于 start")

    # 窗口口径只算 [start, end]，但需要：
    # 1) 截至 end 的全部历史读数——remaining/exceeded/first_exceeded_at 按全历史累计；
    # 2) end 之后最近一条读数——窗口末端可向其线性裁剪。
    # 窗口起点之前的历史读数本身就在 1) 中，窗口起点处同样可线性裁剪。
    # 仲裁按时刻进行，必须取全该时刻的读数集合，因此不按读数行做时间裁剪外
    # 的额外限制（窗口裁剪在计算阶段完成）。
    versions = list_versions(conn, canonical_id)
    raw_rows = conn.execute(
        "SELECT reading_id, ts, lux, source_id FROM readings "
        "WHERE exhibit_id = ? AND voided_at IS NULL ORDER BY ts ASC",
        (canonical_id,),
    ).fetchall()
    groups = arbitrate(
        [(r["reading_id"], r["ts"], r["lux"], r["source_id"]) for r in raw_rows],
        versions,
    )
    all_points, break_ts = authoritative_points(groups)
    # 截至窗口末的历史点（冲突时刻已不在点序列中）
    points = [(ts, lux) for ts, lux in all_points if ts <= end_s]
    after = [(ts, lux) for ts, lux in all_points if ts > end_s][:1]
    readings = points + after

    # 冲突时刻只关心会截断积分段的：落在窗口及历史相关范围内即可
    # （compute_dose 按段内开区间判断，多余的冲突点不影响正确性）。
    breaks = break_ts

    intervals: list[tuple[float, float]] | None = None
    if calendar == "display":
        cfg = load_calendar(conn, canonical_id)
        if cfg is None:
            raise HTTPException(
                status_code=409,
                detail=f"展品未配置展陈开放日历，无法按 calendar=display 核算: {exhibit_id}",
            )
        # 开放区间须覆盖截至窗口末的开放历史：从最早读数（或窗口起点，
        # 取更早者）的前一个本地日期展开到窗口末。
        first_ts = readings[0][0] if readings else start_s
        intervals = open_intervals(cfg, min(start_s, first_ts), end_s)

    result = compute_dose(
        readings=readings,
        start_s=start_s,
        end_s=end_s,
        max_interval_s=exhibit["max_interval_seconds"],
        dose_limit_luxh=exhibit["dose_limit_luxh"],
        open_intervals=intervals,
        break_ts=breaks,
    )
    # reading_count 口径：legacy（无生效版本）时刻与旧版一致，统计原始读数
    # 条数；仲裁权威点每个时刻计 1；仲裁冲突时刻不进时间线，计 0。
    reading_count = 0
    for g in groups:
        if not (start_s <= g.ts <= end_s):
            continue
        if g.legacy:
            reading_count += len(g.candidates)
        elif g.status == "point":
            reading_count += 1
    response = DoseResponse(
        requested_id=exhibit_id,
        canonical_id=canonical_id,
        start=to_iso(start_s),
        end=to_iso(end_s),
        dose_limit_luxh=exhibit["dose_limit_luxh"],
        known_dose_luxh=round6(result.known_dose_luxh),
        remaining_luxh=round6(result.remaining_luxh),
        exceeded=result.exceeded,
        first_exceeded_at=to_iso(result.first_exceeded_at) if result.first_exceeded_at is not None else None,
        unknown_seconds=round6(result.unknown_seconds),
        covered_seconds=round6(result.covered_seconds),
        reading_count=reading_count,
    )
    if result.closed_seconds is not None:
        response.closed_seconds = round6(result.closed_seconds)
    return response


# ---------- 重复档案归并 ----------

@app.post("/merges/preview", response_model=MergePreviewOut, status_code=200,
          tags=["merge"])
def preview_merge(
    payload: MergePreviewRequest,
    conn: sqlite3.Connection = Depends(get_conn),
) -> MergePreviewOut:
    """预演把 source_id 合入 target_id。

    - 目标档案的名称与剂量配置（dose_limit_luxh / max_interval_seconds）保持不变；
    - ``metadata`` 逐字段列出两边差异；
    - ``collisions`` 列出同读数 ID 的碰撞：时间、照度一致的标 ``duplicate``，
      提交时自动去重；内容不同的标 ``conflict``，提交时必须逐条裁决；
    - 返回的 ``token`` 绑定预演时刻两边的元数据与读数集合版本。
    """
    return create_preview(conn, payload.source_id, payload.target_id)


@app.post("/merges/commit", response_model=MergeCommitOut, status_code=200,
          tags=["merge"])
def commit_merge_endpoint(
    payload: MergeCommitRequest,
    conn: sqlite3.Connection = Depends(get_conn),
) -> MergeCommitOut:
    """按预演令牌提交归并。

    - 预演后任一档案发生写入、修改或删除 → 409，事务回滚，不产生部分变更；
    - 所有 ``conflict`` 碰撞必须逐条给出 keep_target / keep_source /
      rename_source 裁决；两边**来源优先级配置**的分歧必须逐条给出
      keep_target / keep_source 裁决，冲突不得静默覆盖；未解决不得提交（422）；
    - 提交前分别校验来源与目标的既有存证收据链，断链或内容不符 → 409；
    - 整个归并在单个 SQLite 事务内完成；归并后的优先级配置在目标档案按
      裁决结果合并（历史读数与收据不改写）；
    - 重复使用同一令牌返回首次提交的原结果（``already_committed=true``）；
    - 归并后来源 ID 成为目标 ID 的永久别名，查询、读数上报与来源优先级
      配置都解析到目标档案；双方既有收据链原样保留（``receipt_chains``），
      新上报继续接入目标原链。
    """
    return commit_merge(
        conn, payload.token, payload.resolutions, payload.priority_resolutions
    )


# ---------- SQLite 在线快照（创建 / 列表 / 下载 / 校验） ----------

@app.post("/snapshots", response_model=SnapshotOut, status_code=201, tags=["snapshots"])
def create_online_snapshot() -> SnapshotOut:
    """创建一份事务一致的 SQLite 在线快照。

    - 使用 ``sqlite3.Connection.backup`` 在线热备，得到事务一致的数据库副本，
      先写临时文件；完成后计算 SHA-256，并生成包含快照编号、创建时间、页数、
      字节数的清单，db 与清单都通过原子重命名发布；
    - 同一时刻只允许一个创建请求执行，其余返回 **409**；
    - 失败或未完成的临时文件不会出现在快照列表中。
    """
    return create_snapshot()


@app.get("/snapshots", response_model=SnapshotListOut, tags=["snapshots"])
def list_online_snapshots() -> SnapshotListOut:
    """按编号升序列出已发布快照；未完成 / 失败文件一律不出现。"""
    return SnapshotListOut(snapshots=list_snapshots())


@app.get("/snapshots/{seq}", response_model=SnapshotOut, tags=["snapshots"])
def get_online_snapshot(seq: int) -> SnapshotOut:
    """查询单个快照清单；未发布（含失败、未完成）返回 404。"""
    snap = get_snapshot(seq)
    if snap is None:
        raise HTTPException(status_code=404, detail=f"快照不存在或尚未发布完成: #{seq}")
    return snap


@app.get("/snapshots/{seq}/download", tags=["snapshots"])
def download_online_snapshot(seq: int) -> FileResponse:
    """下载快照数据库文件（单一自包含 .db）；未发布返回 404。"""
    path = snapshot_db_path(seq)
    if path is None:
        raise HTTPException(status_code=404, detail=f"快照不存在或尚未发布完成: #{seq}")
    return FileResponse(
        path=str(path),
        media_type="application/vnd.sqlite3",
        filename=f"lightdose-snapshot-{seq}.db",
    )


@app.get("/snapshots/{seq}/verify", response_model=SnapshotVerifyOut, tags=["snapshots"])
def verify_online_snapshot(seq: int) -> SnapshotVerifyOut:
    """在**独立只读连接**上逐项校验快照，不修改线上数据库也不修改快照。

    依次核对：文件 SHA-256、字节数、页数，并执行
    ``PRAGMA integrity_check`` 与 ``PRAGMA foreign_key_check``，
    返回每一项的结果；任一项不通过则整体 ``ok=false``。
    """
    result = verify_snapshot(seq)
    if result is None:
        raise HTTPException(status_code=404, detail=f"快照不存在或尚未发布完成: #{seq}")
    return result


# ---------- RFID 馆藏盘点对账 ----------

@app.post("/inventory-rounds/{round_id}/reconcile",
          response_model=ReconcileResponse, tags=["inventory"])
def reconcile_inventory_round(
    round_id: str,
    payload: ReconcileRequest,
    conn: sqlite3.Connection = Depends(get_conn),
) -> ReconcileResponse:
    """执行一轮 RFID 馆藏盘点对账并固化结果。

    定位口径：

    - 从 ``start_at`` 起按 ``window_seconds`` 划分固定时间窗；早于开始
      时刻的读头事件忽略（计数见 ``events_before_start_count``）；
    - 每件展品在每个窗口、每个区域只保留**最高 RSSI**；
    - 以**最后一个有事件的窗口**判定位置：窗口内仅一个区域直接定位；
      多个区域时最强与次强 RSSI 之差**严格小于** ``rssi_delta_threshold``
      记为**位置不明**（恰好等于阈值仍可定位）；

    结果四组互斥、按规范 ID 稳定排序：``missing``（缺失）、
    ``misplaced``（错位）、``unexpected``（意外出现）、
    ``unknown_position``（位置不明，含不在预期清单中的歧义标签）。

    幂等与冲突：

    - 请求中的展品 ID（预期清单与读头事件）先经归并别名解析为规范档案；
      解析后同一规范档案在预期清单中出现多次 → **422**；不存在 → 404；
    - ``round_id`` 与规范化请求摘要绑定：**相同请求**重放返回首次结果
      （``already_committed=true``），**不同请求**返回 **409**；
    - 并发提交同一 ``round_id`` 时只有一份结果能固化，竞争方按请求异同
      得到回放或 409；
    - 对账不改写照度读数，也不生成或改写存证收据链。
    """
    result, replayed = reconcile_round(conn, round_id, payload)
    return ReconcileResponse(**result.model_dump(), already_committed=replayed)


@app.get("/inventory-rounds/{round_id}", response_model=InventoryRoundOut,
         tags=["inventory"])
def get_inventory_round(
    round_id: str,
    conn: sqlite3.Connection = Depends(get_conn),
) -> InventoryRoundOut:
    """取回已固化的盘点对账结果；轮次尚未对账返回 404。"""
    result = load_round(conn, round_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"盘点轮次不存在或尚未对账: {round_id}")
    return result


# ---------- 展品应急撤离编排（纯只读计算） ----------

@app.post("/evacuation-plan", response_model=EvacuationResponse, tags=["evacuation"])
def plan_exhibit_evacuation(
    payload: EvacuationRequest,
    conn: sqlite3.Connection = Depends(get_conn),
) -> EvacuationResponse:
    """为多件展品编排应急撤离路径与时序（纯只读计算，不落库）。

    请求给出：

    - 计划起点 ``start_at``（绝对时刻）、有向通道图（``nodes`` / ``edges``）、
      出口 ``exits``；每条通道给出整数通行秒数 ``travel_seconds``、并发容量
      ``capacity`` 与半开封闭时段 ``closures``（相对计划起点的 ``[start,end)``）；
    - 待撤离展品：ID、起点节点与优先级。展品 ID 先经归并别名**解析为规范档案**；
      不存在 → 404；解析后重复、图非法（重复节点/通道、未知端点或出口）、
      或某展品起点无可达出口的有向路径 → **422**。

    编排口径：

    - 按**优先级降序、规范 ID 升序**逐件编排；每件展品在已编排展品的占用
      计划上做一次**时变最早到达搜索**，选出最早到达的出口与完整路径；
    - 通道占用按半开区间 ``[进入, 离开)`` 计数（恰好在他人离开时刻进入不
      冲突）；容量按窗口内**同一时刻的实际并发**判断（多条占用错峰、任一
      瞬间并发未满时仍可进入，而非按与新窗口重叠的占用条数计数），只有加入
      新占用会使某瞬间达到容量、或进入时刻落在封闭时段 ``[cs, ce)`` 内时，
      才等待到**首个可行时刻**；每次因容量或封闭推进候选时刻后都重新检查
      两类约束（容量释放时刻与封闭结束时刻取较晚者），不会跳过更早空档；
      封闭只约束“进入”动作，进入后跨过封闭开始时刻允许；
    - 到达时间相同的多个方案按**完整路径（节点序列）字典序**决胜；
    - 响应返回每段通道的进入/离开时刻（绝对 ISO 8601 + 相对秒）、该段等待
      秒数与每件展品的总完成时间。编排不写照度读数、不写日历、不写盘点
      轮次，也不生成或改写存证收据链。
    """
    start_dt = ensure_aware(payload.start_at)
    start_epoch = start_dt.timestamp()
    start_iso = to_iso(start_epoch)

    def leg_iso(offset: int) -> str:
        return to_iso(float(start_epoch + offset))

    # 构图（结构合法性已由 pydantic 模型层校验，这里再经纯函数模块兜底）
    try:
        graph = EvacuationGraph.build(
            nodes=list(payload.nodes),
            raw_edges=[
                (e.from_node, e.to_node, e.travel_seconds, e.capacity,
                 [(c.start, c.end) for c in e.closures])
                for e in payload.edges
            ],
            exits=list(payload.exits),
        )
    except EvacuationGraphError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # 展品 ID 先解析为规范档案：不存在 → 404；解析后重复 → 422
    exhibits: list[tuple[str, str, str, int]] = []
    seen_canonical: set[str] = set()
    duplicates: list[str] = []
    unknown: list[str] = []
    for item in payload.exhibits:
        canonical = resolve_canonical(conn, item.exhibit_id)
        if canonical is None:
            unknown.append(item.exhibit_id)
            continue
        if canonical in seen_canonical:
            duplicates.append(f"{item.exhibit_id}（规范档案 {canonical}）")
            continue
        seen_canonical.add(canonical)
        exhibits.append((item.exhibit_id, canonical, item.start_node, item.priority))
    if unknown:
        raise HTTPException(status_code=404, detail=f"展品不存在: {unknown}")
    if duplicates:
        raise HTTPException(
            status_code=422,
            detail=f"撤离展品重复（别名解析后同一规范档案出现多次）: {duplicates}",
        )

    try:
        planned = plan_evacuation(graph, exhibits)
    except EvacuationGraphError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    results: list[EvacuationExhibitOut] = []
    total_completion = 0
    for p in planned:
        legs_out = [
            EvacuationLegOut(
                **{"from": edge.u, "to": edge.v},
                travel_seconds=edge.travel,
                enter_offset_seconds=enter,
                leave_offset_seconds=leave,
                enter_at=leg_iso(enter),
                leave_at=leg_iso(leave),
                wait_seconds=wait,
            )
            for edge, enter, leave, wait in p.route.legs
        ]
        total_wait = sum(wait for _e, _en, _lv, wait in p.route.legs)
        total_completion = max(total_completion, p.route.arrival)
        results.append(EvacuationExhibitOut(
            requested_id=p.requested_id,
            canonical_id=p.canonical_id,
            start_node=p.start_node,
            priority=p.priority,
            exit_node=p.route.path[-1],
            path=list(p.route.path),
            legs=legs_out,
            wait_seconds=total_wait,
            completion_offset_seconds=p.route.arrival,
            completed_at=leg_iso(p.route.arrival),
        ))

    return EvacuationResponse(
        start_at=start_iso,
        results=results,
        total_completion_offset_seconds=total_completion,
    )
