"""多来源读数仲裁：来源优先级版本、按采样时刻的权威时间线。

语义约定：
- 每条上报读数可携带 ``source_id``（采集器 / 传感器来源）；同一展品同一采样
  时刻的不同来源读数**全部留存**（读数行不覆盖、不删除），仅在查询时仲裁出
  权威时间线。
- 规范档案维护**只增不改**的来源优先级版本：每个版本携带生效时刻
  ``effective_at``（含端点）与一份完整来源优先级清单（``priority`` 越大越
  权威）。某采样时刻适用的版本为 ``effective_at`` 不晚于该时刻的最新版本；
  没有任何版本生效时保持旧行为（见下 ``legacy``）。
- 仲裁规则（时刻内）：
  * 读数来源出现在生效版本中 → 取其优先级；未列入版本的来源（含未标注
    source_id 的旧读数）→ 归入**最低档**（低于任何已列来源）；
  * 取在场最高优先级一档的读数为权威候选；
  * 候选照度全部相同 → 该时刻为**权威点**，照度取该值，全部并列读数 ID
    与来源 ID 保留（同一时刻同值多读数先合并为一个点）；
  * 候选照度不同 → 该时刻**保持冲突**（最高优先级并列且照度不一致），
    冲突时刻**不进入剂量与诊断时间线**（剂量不可跨过它积分，诊断链在其
    两侧中断），但读数原样留存，并可通过权威时间线查询显式核查。
- **无生效版本**（``active version is None``）的时刻为 ``legacy``：此时所有
  读数无论是否标注来源都处于同一隐式档位，行为与旧版完全一致——原始读数
  各自成点、逐对进入梯形积分（同时刻间隔为 0），诊断沿用
  同值合并 / 异值 ``timestamp_conflict`` 口径。旧请求不带 ``source_id``
  因此天然保持原行为。
- 调整优先级（写入新版本）只**从生效时刻起**改变查询时的仲裁结果：历史
  读数行与存证收据永不改写；早于新生效时刻的采样仍按旧版本仲裁。
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone

from fastapi import HTTPException

from .merge import get_canonical_or_404
from .schemas import (
    AuthoritativeTimelineOut,
    AuthoritativeTimelinePointOut,
    SourcePriorityConfigOut,
    SourcePriorityPut,
    SourcePriorityVersionOut,
    TimelineCandidateOut,
    ensure_aware,
)


# ---------- 数据结构 ----------

@dataclass(frozen=True)
class PriorityEntry:
    source_id: str
    priority: int


@dataclass
class PriorityVersion:
    """一个来源优先级版本：effective_at 起（含）生效的一份完整优先级清单。"""

    version: int                    # 版本号（= source_priority_versions.id，全局连续）
    effective_at: float             # 生效时刻，Unix 秒（UTC）
    created_at: str
    entries: list[PriorityEntry] = field(default_factory=list)

    def priority_of(self, source_id: str | None) -> int | None:
        """来源在本版本中的优先级；未列入（含 source_id 为 None）→ None（最低档）。"""
        if source_id is None:
            return None
        for e in self.entries:
            if e.source_id == source_id:
                return e.priority
        return None


@dataclass
class TimelineCandidate:
    """同一采样时刻留存的一条原始读数。"""

    reading_id: str
    ts: float
    lux: float
    source_id: str | None


@dataclass
class TimelineGroup:
    """一个采样时刻的仲裁结果。

    - ``legacy=True``：该时刻无生效优先级版本，保持旧行为：``candidates``
      各自作为原始点进入剂量/诊断（同时刻间隔为 0），诊断再按同值合并 /
      异值 timestamp_conflict 处理；
    - ``legacy=False`` 且 ``status="point"``：仲裁出唯一权威值，``winners``
      为最高优先级一档的全部并列读数（照度相同，合并为一个点）；
    - ``legacy=False`` 且 ``status="conflict"``：最高优先级并列且照度不同，
      ``winners`` 为并列读数，``lux_span`` 为其照度跨度，该时刻不进入剂量
      与诊断时间线。
    """

    ts: float
    candidates: list[TimelineCandidate]
    legacy: bool
    active_version: int | None = None
    status: str | None = None          # "point" | "conflict"（legacy 时为 None）
    lux: float | None = None
    winners: list[TimelineCandidate] = field(default_factory=list)
    priority: int | None = None        # 权威档优先级；未列入来源胜出时为 None
    lux_span: float | None = None

    def points(self) -> list[TimelineCandidate]:
        """剂量时间线上的点：legacy 为全部原始读数；仲裁点为并列权威读数（同值）；冲突为空。"""
        if self.legacy:
            return list(self.candidates)
        if self.status == "point":
            return list(self.winners)
        return []

    def is_break(self) -> bool:
        """仲裁冲突时刻：剂量积分不可跨过它。"""
        return not self.legacy and self.status == "conflict"


# ---------- 配置读取 ----------

def list_versions(conn: sqlite3.Connection, canonical_id: str) -> list[PriorityVersion]:
    """按生效时刻升序（相同则按版本号）取出来源优先级版本与条目。"""
    rows = conn.execute(
        "SELECT id, effective_at, created_at FROM source_priority_versions "
        "WHERE exhibit_id = ? ORDER BY effective_at ASC, id ASC",
        (canonical_id,),
    ).fetchall()
    versions: list[PriorityVersion] = []
    for r in rows:
        entry_rows = conn.execute(
            "SELECT source_id, priority FROM source_priority_entries "
            "WHERE version_id = ? ORDER BY priority DESC, source_id ASC",
            (r["id"],),
        ).fetchall()
        versions.append(PriorityVersion(
            version=r["id"],
            effective_at=r["effective_at"],
            created_at=r["created_at"],
            entries=[PriorityEntry(source_id=e["source_id"], priority=e["priority"])
                     for e in entry_rows],
        ))
    return versions


def current_version_number(conn: sqlite3.Connection, canonical_id: str) -> int:
    row = conn.execute(
        "SELECT MAX(id) AS m FROM source_priority_versions WHERE exhibit_id = ?",
        (canonical_id,),
    ).fetchone()
    return row["m"] or 0 if row is not None else 0


def active_version_at(versions: list[PriorityVersion], ts: float) -> PriorityVersion | None:
    """采样时刻 ts 适用的优先级版本：effective_at <= ts 的最新版本；无则 None。"""
    chosen: PriorityVersion | None = None
    for v in versions:  # 已按 effective_at 升序
        if v.effective_at <= ts:
            chosen = v
        else:
            break
    return chosen


# ---------- 配置更新（expected_version 乐观并发，409） ----------

def put_priority_config(
    conn: sqlite3.Connection,
    requested_id: str,
    payload: SourcePriorityPut,
) -> SourcePriorityConfigOut:
    """写入一个新生效版本；必须携带 expected_version，过期返回 409。

    版本只增不改：不修改任何历史版本、读数或收据；优先级调整仅从新版本的
    生效时刻起影响查询重算。
    """
    effective_s = ensure_aware(payload.effective_at).timestamp()

    conn.execute("BEGIN IMMEDIATE")
    try:
        exhibit = get_canonical_or_404(conn, requested_id)
        canonical_id = exhibit["id"]

        latest = conn.execute(
            "SELECT id FROM source_priority_versions WHERE exhibit_id = ? "
            "ORDER BY id DESC LIMIT 1",
            (canonical_id,),
        ).fetchone()
        current = latest["id"] if latest is not None else 0
        if payload.expected_version != current:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"来源优先级配置版本已过期：expected_version="
                    f"{payload.expected_version}，当前最新版本为 {current}；"
                    "请重新读取配置后基于最新版本号提交"
                ),
            )

        dup_effective = conn.execute(
            "SELECT 1 FROM source_priority_versions WHERE exhibit_id = ? AND effective_at = ?",
            (canonical_id, effective_s),
        ).fetchone()
        if dup_effective is not None:
            raise HTTPException(
                status_code=422,
                detail="同一生效时刻已存在优先级版本：版本只增不改，请改用新的生效时刻",
            )

        cur = conn.execute(
            "INSERT INTO source_priority_versions (exhibit_id, effective_at) VALUES (?, ?)",
            (canonical_id, effective_s),
        )
        version_id = cur.lastrowid
        for e in payload.priorities:
            conn.execute(
                "INSERT INTO source_priority_entries (version_id, exhibit_id, source_id, priority) "
                "VALUES (?, ?, ?, ?)",
                (version_id, canonical_id, e.source_id, e.priority),
            )
        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        # 来源在同一版本内重复等数据层兜底（模型层通常已拦截）
        raise HTTPException(status_code=422, detail=f"优先级配置不合法: {exc}")
    except Exception:
        conn.rollback()
        raise

    return load_priority_config(conn, requested_id)


def load_priority_config(conn: sqlite3.Connection, requested_id: str) -> SourcePriorityConfigOut:
    exhibit = get_canonical_or_404(conn, requested_id)
    canonical_id = exhibit["id"]
    versions = list_versions(conn, canonical_id)
    return SourcePriorityConfigOut(
        requested_id=requested_id,
        canonical_id=canonical_id,
        current_version=versions[-1].version if versions else 0,
        versions=[
            SourcePriorityVersionOut(
                version=v.version,
                effective_at=_to_iso(v.effective_at),
                created_at=v.created_at,
                priorities=[{"source_id": e.source_id, "priority": e.priority} for e in v.entries],
            )
            for v in versions
        ],
    )


# ---------- 仲裁 ----------

def _candidate_sort_key(c: TimelineCandidate) -> tuple[str, str, str]:
    # 同一时刻内先按读数 ID 排序（与旧诊断口径一致，结果与上报顺序无关），
    # 读数 ID 相同时再以来源 ID 兜底（实际同展品读数 ID 唯一，不会走到）；
    # 未标注来源以空串占位，排序确定即可（权威/冲突集合不依赖该次序）。
    return (c.reading_id, c.source_id or "", c.reading_id)


def arbitrate(
    rows: list[tuple[str, float, float, str | None]],
    versions: list[PriorityVersion],
) -> list[TimelineGroup]:
    """把原始读数行 (reading_id, ts, lux, source_id) 按时刻分组并仲裁。

    返回按采样时刻升序的时刻组；同一时刻的候选按读数 ID 稳定排序，与读数
    到达顺序无关。调用方须先排除已作废读数。
    """
    ordered = sorted(rows, key=lambda r: (r[1], r[0], r[3] or ""))
    groups: list[TimelineGroup] = []
    members: list[TimelineCandidate] = []
    cur_ts: float | None = None

    def flush() -> None:
        if cur_ts is None:
            return
        version = active_version_at(versions, cur_ts)
        candidates = sorted(members, key=_candidate_sort_key)
        if version is None:
            # 无生效版本：旧行为——原始读数全部原样进入时间线
            groups.append(TimelineGroup(
                ts=cur_ts, candidates=candidates, legacy=True, active_version=None,
            ))
            return

        tiers = [(c, version.priority_of(c.source_id)) for c in candidates]
        # 最高优先级档：已列来源的任何优先级都高于未列入档（None）
        listed = [t for t in tiers if t[1] is not None]
        top_priority: int | None
        pool: list[TimelineCandidate]
        if listed:
            top_priority = max(t[1] for t in listed)
            pool = [c for c, p in tiers if p == top_priority]
        else:
            top_priority = None
            pool = list(candidates)

        lux_values = {c.lux for c in pool}
        if len(lux_values) > 1:
            groups.append(TimelineGroup(
                ts=cur_ts, candidates=candidates, legacy=False,
                active_version=version.version, status="conflict",
                winners=pool, priority=top_priority,
                lux_span=max(lux_values) - min(lux_values),
            ))
        else:
            groups.append(TimelineGroup(
                ts=cur_ts, candidates=candidates, legacy=False,
                active_version=version.version, status="point",
                lux=next(iter(lux_values)), winners=pool, priority=top_priority,
            ))

    for rid, ts, lux, source_id in ordered:
        if cur_ts is not None and ts != cur_ts:
            flush()
            members = []
        cur_ts = ts
        members.append(TimelineCandidate(reading_id=rid, ts=ts, lux=lux, source_id=source_id))
    flush()
    return groups


def authoritative_points(
    groups: list[TimelineGroup],
) -> tuple[list[tuple[float, float]], set[float]]:
    """剂量核算视角：(ts, lux) 权威点序列 + 仲裁冲突时刻集合。

    legacy 时刻的全部原始读数各自成点（同时刻间隔为 0，与旧版一致）；
    仲裁点同值并列读数合并为一个点；冲突时刻不出点且两侧不可跨接积分。
    """
    points: list[tuple[float, float]] = []
    breaks: set[float] = set()
    for g in groups:
        if g.is_break():
            breaks.add(g.ts)
            continue
        for c in g.points():
            points.append((c.ts, c.lux))
    points.sort(key=lambda p: (p[0], p[1]))
    return points, breaks


# ---------- 权威时间线查询 ----------

def build_timeline_out(
    conn: sqlite3.Connection,
    requested_id: str,
    start_s: float | None,
    end_s: float | None,
    limit: int,
) -> AuthoritativeTimelineOut:
    exhibit = get_canonical_or_404(conn, requested_id)
    canonical_id = exhibit["id"]
    versions = list_versions(conn, canonical_id)

    sql = "SELECT reading_id, ts, lux, source_id FROM readings WHERE exhibit_id = ? AND voided_at IS NULL"
    params: list = [canonical_id]
    if start_s is not None:
        sql += " AND ts >= ?"
        params.append(start_s)
    if end_s is not None:
        sql += " AND ts <= ?"
        params.append(end_s)
    sql += " ORDER BY ts LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()

    # 仲裁需要时刻内全部分组；过滤后边界时刻不会被截断（窗口为含端点），
    # 因为同一时刻的读数在同一查询条件下同时入选。
    groups = arbitrate(
        [(r["reading_id"], r["ts"], r["lux"], r["source_id"]) for r in rows],
        versions,
    )

    points_out: list[AuthoritativeTimelinePointOut] = []
    for g in groups:
        candidates = [
            TimelineCandidateOut(
                id=c.reading_id, ts=_to_iso(c.ts), lux=c.lux, source_id=c.source_id,
            )
            for c in g.candidates
        ]
        if g.legacy:
            lux_values = sorted({c.lux for c in g.candidates})
            unique = len(lux_values) == 1
            points_out.append(AuthoritativeTimelinePointOut(
                ts=_to_iso(g.ts),
                status="legacy",
                version=None,
                lux=lux_values[0] if unique else None,
                priority=None,
                source_ids=sorted({c.source_id for c in g.candidates if c.source_id is not None}),
                reading_ids=[c.reading_id for c in g.candidates],
                lux_span=None if unique else lux_values[-1] - lux_values[0],
                candidates=candidates,
            ))
        elif g.status == "conflict":
            points_out.append(AuthoritativeTimelinePointOut(
                ts=_to_iso(g.ts),
                status="conflict",
                version=g.active_version,
                lux=None,
                priority=g.priority,
                source_ids=sorted({c.source_id for c in g.winners if c.source_id is not None}),
                reading_ids=[c.reading_id for c in g.winners],
                lux_span=g.lux_span,
                candidates=candidates,
            ))
        else:
            points_out.append(AuthoritativeTimelinePointOut(
                ts=_to_iso(g.ts),
                status="authoritative",
                version=g.active_version,
                lux=g.lux,
                priority=g.priority,
                source_ids=sorted({c.source_id for c in g.winners if c.source_id is not None}),
                reading_ids=[c.reading_id for c in g.winners],
                lux_span=None,
                candidates=candidates,
            ))

    return AuthoritativeTimelineOut(
        requested_id=requested_id,
        canonical_id=canonical_id,
        start=_to_iso(start_s) if start_s is not None else None,
        end=_to_iso(end_s) if end_s is not None else None,
        current_version=versions[-1].version if versions else 0,
        points=points_out,
    )


def _to_iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")
