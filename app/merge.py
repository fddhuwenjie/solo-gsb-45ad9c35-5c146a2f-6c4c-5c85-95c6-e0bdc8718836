"""重复展品档案归并：预演、版本令牌、单事务提交、永久别名。

设计要点：
- 来源 ID 归并后写入 ``exhibit_aliases`` 永久指向目标规范 ID；
  来源名下的旧别名在提交事务内改指向目标，表中始终只有一跳，
  不可能形成别名环。
- 预演令牌固化两边元数据与读数集合的 SHA-256 版本；提交在
  ``BEGIN IMMEDIATE`` 事务内重算哈希，预演后任一侧被写入/修改/删除
  都会导致哈希不一致 → 409，事务回滚，不产生部分变更。
- 同读数 ID 且时间、照度一致的碰撞自动去重；内容不同的碰撞必须逐条
  裁决（keep_target / keep_source / rename_source），未解决不得提交。
- 提交成功后把结果固化在令牌行上，重复使用同一令牌回放原结果。
- 归并不触碰照度上报存证收据链：来源与目标的既有链原样保留、提交前
  分别校验（断链或内容不符即 409 拒绝归并），归并后的新上报继续接入
  目标原链，旧哈希绝不重写。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone

from fastapi import HTTPException

from .receipts import load_chain, verify_chain_rows
from .schemas import (
    MergeCollision,
    MergeCommitOut,
    MergeMetaDiff,
    MergePreviewOut,
    MergePriorityCollision,
    MergePriorityResolution,
    MergePriorityResolutionResult,
    MergeResolution,
    MergeResolutionResult,
)

# ---------- 工具 ----------

def to_iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_aware_ts(dt: datetime) -> float:
    """裁决请求中的生效时刻 → Unix 秒；缺省时区按 UTC。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _meta_hash(row: sqlite3.Row) -> str:
    payload = {
        "id": row["id"],
        "name": row["name"],
        "dose_limit_luxh": float.hex(row["dose_limit_luxh"]),
        "max_interval_seconds": float.hex(row["max_interval_seconds"]),
        "created_at": row["created_at"],
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def _readings_hash(rows: list[sqlite3.Row]) -> str:
    # 作废标记与来源 ID 同样是读数集合版本的一部分：预演后发生批次更正（作废）
    # 或任何来源标注变化（归并内容碰撞）都使令牌失效。
    cols = rows[0].keys() if rows else ()
    payload = [
        (
            r["reading_id"], float.hex(r["ts"]), float.hex(r["lux"]),
            r["voided_at"] if "voided_at" in cols else None,
            r["voided_by"] if "voided_by" in cols else None,
            r["source_id"] if "source_id" in cols else None,
        )
        for r in rows
    ]
    payload.sort()
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def _priority_rows(conn: sqlite3.Connection, exhibit_id: str) -> list[sqlite3.Row]:
    """取来源优先级配置的完整快照（版本时刻 + 来源 + 优先级），按时刻/来源排序。"""
    return conn.execute(
        """SELECT v.effective_at AS effective_at, e.source_id AS source_id, e.priority AS priority
           FROM source_priority_versions v
           JOIN source_priority_entries e ON e.version_id = v.id
           WHERE v.exhibit_id = ?
           ORDER BY v.effective_at ASC, e.source_id ASC""",
        (exhibit_id,),
    ).fetchall()


def _priority_hash(rows: list[sqlite3.Row]) -> str:
    payload = [(float.hex(r["effective_at"]), r["source_id"], r["priority"]) for r in rows]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


# 无来源优先级配置时的快照哈希（旧预演令牌迁移列的默认值）
_EMPTY_PRIORITY_HASH = hashlib.sha256(b"[]").hexdigest()


def _priority_steps(
    rows: list[sqlite3.Row],
) -> list[tuple[float, dict[str, int]]]:
    """把 (effective_at, source_id, priority) 三元组展开为逐生效时刻的步骤表。

    每个步骤为 (effective_at, {source_id: priority})：自该时刻起生效的完整
    来源→优先级映射（沿用上一时刻未变动的条目，条目从不在新版本中重述）。
    """
    boundaries: dict[float, dict[str, int]] = {}
    for r in rows:
        boundaries.setdefault(r["effective_at"], {})[r["source_id"]] = r["priority"]
    steps: list[tuple[float, dict[str, int]]] = []
    current: dict[str, int] = {}
    for eff in sorted(boundaries):
        current = {**current, **boundaries[eff]}
        steps.append((eff, current))
    return steps


def _value_at(steps: list[tuple[float, dict[str, int]]], eff: float, source_id: str) -> int | None:
    """在生效时刻 eff（含）该来源的优先级；条目不存在（或该侧无配置）→ None。"""
    value: int | None = None
    for boundary, mapping in steps:
        if boundary <= eff:
            value = mapping.get(source_id)
        else:
            break
    return value


def _priority_collisions(
    src_rows: list[sqlite3.Row],
    tgt_rows: list[sqlite3.Row],
) -> list[MergePriorityCollision]:
    """列出两边来源优先级配置在归并时间线上的**分歧冲突**。

    合并是**并集**：某来源只在一侧配置（另一侧该时刻无条目）时自动并入，
    不需要裁决；只有同一时刻两侧**都定义了**该来源但优先级不同（或一侧条目
    结束、另一侧仍保留）时才是必须显式裁决的冲突，冲突不得静默覆盖。
    """
    src_steps = _priority_steps(src_rows)
    tgt_steps = _priority_steps(tgt_rows)
    boundaries = sorted({
        *(r["effective_at"] for r in src_rows),
        *(r["effective_at"] for r in tgt_rows),
    })
    sources = sorted({
        *(r["source_id"] for r in src_rows),
        *(r["source_id"] for r in tgt_rows),
    })
    collisions: list[MergePriorityCollision] = []
    for eff in boundaries:
        for source_id in sources:
            sv = _value_at(src_steps, eff, source_id)
            tv = _value_at(tgt_steps, eff, source_id)
            # 两侧均“已定义”（条目可能已结束为 None，但另一侧仍有值）且不同 → 冲突；
            # 仅一侧定义 → 自动并入，不出现在碰撞清单。
            src_defined = any(
                source_id in m for boundary, m in src_steps if boundary <= eff
            )
            tgt_defined = any(
                source_id in m for boundary, m in tgt_steps if boundary <= eff
            )
            if sv != tv and src_defined and tgt_defined:
                collisions.append(MergePriorityCollision(
                    source_id=source_id,
                    effective_at=to_iso(eff),
                    source_priority=sv,
                    target_priority=tv,
                ))
    return collisions


def _merge_priority(
    src_rows: list[sqlite3.Row],
    tgt_rows: list[sqlite3.Row],
    decisions: dict[tuple[float, str], str],
) -> list[tuple[float, list[tuple[str, int | None]]]]:
    """按逐条裁决把两边优先级配置并成一份逐时刻配置。

    返回 [(effective_at, [(source_id, priority)])]，时刻与来源均升序；
    priority 为 None 表示该侧无条目（仅另一侧配置存在且裁决给该侧时也不会
    为 None——None 只可能在裁决保留的一侧该条目已结束时出现，此时跳过写入）。
    """
    src_steps = _priority_steps(src_rows)
    tgt_steps = _priority_steps(tgt_rows)
    boundaries = sorted({
        *(r["effective_at"] for r in src_rows),
        *(r["effective_at"] for r in tgt_rows),
    })
    sources = sorted({
        *(r["source_id"] for r in src_rows),
        *(r["source_id"] for r in tgt_rows),
    })
    out: list[tuple[float, list[tuple[str, int | None]]]] = []
    for eff in boundaries:
        entries: list[tuple[str, int | None]] = []
        for source_id in sources:
            sv = _value_at(src_steps, eff, source_id)
            tv = _value_at(tgt_steps, eff, source_id)
            src_defined = any(
                source_id in m for boundary, m in src_steps if boundary <= eff
            )
            tgt_defined = any(
                source_id in m for boundary, m in tgt_steps if boundary <= eff
            )
            if sv == tv:
                value = sv
            elif src_defined and tgt_defined:
                # 两侧都定义且不一致：必须有显式裁决
                value = sv if decisions[(eff, source_id)] == "keep_source" else tv
            else:
                # 仅一侧定义：自动并入该侧取值（无分歧，无需裁决）
                value = sv if src_defined else tv
            entries.append((source_id, value))
        out.append((eff, entries))
    return out


# ---------- 别名解析 ----------

def resolve_canonical(conn: sqlite3.Connection, exhibit_id: str) -> str | None:
    """把请求 ID（可能是别名）解析为规范档案 ID；不存在返回 None。

    正常情况下别名表只有一跳；这里仍带跳数上限沿链解析，
    任何企图重指回既有别名（可能成环）的操作在提交时即被拒绝。
    """
    cur = exhibit_id
    seen: set[str] = set()
    while cur not in seen:
        seen.add(cur)
        row = conn.execute(
            "SELECT canonical_id FROM exhibit_aliases WHERE alias_id = ?",
            (cur,),
        ).fetchone()
        if row is None:
            break
        cur = row["canonical_id"]
    exists = conn.execute(
        "SELECT 1 FROM exhibits WHERE id = ?", (cur,)
    ).fetchone()
    return cur if exists is not None else None


def aliases_of(conn: sqlite3.Connection, canonical_id: str) -> list[str]:
    rows = conn.execute(
        "SELECT alias_id FROM exhibit_aliases WHERE canonical_id = ? ORDER BY alias_id",
        (canonical_id,),
    ).fetchall()
    return [r["alias_id"] for r in rows]


def _get_canonical_row(conn: sqlite3.Connection, exhibit_id: str) -> sqlite3.Row:
    canonical = resolve_canonical(conn, exhibit_id)
    if canonical is None:
        raise HTTPException(status_code=404, detail=f"展品不存在: {exhibit_id}")
    return conn.execute("SELECT * FROM exhibits WHERE id = ?", (canonical,)).fetchone()


def get_canonical_or_404(conn: sqlite3.Connection, exhibit_id: str) -> sqlite3.Row:
    return _get_canonical_row(conn, exhibit_id)


# ---------- 预演 ----------

def _meta_diff_field(field: str, source: object, target: object) -> MergeMetaDiff | None:
    if source != target:
        return MergeMetaDiff(field=field, source=source, target=target)
    return None


def create_preview(conn: sqlite3.Connection, requested_source: str,
                   requested_target: str) -> MergePreviewOut:
    if requested_source == requested_target:
        raise HTTPException(status_code=422, detail="source_id 与 target_id 不能相同")

    src = get_canonical_or_404(conn, requested_source)
    tgt = get_canonical_or_404(conn, requested_target)
    source_id, target_id = src["id"], tgt["id"]
    if source_id == target_id:
        # 两个 ID 是同一档案（其中一个可能是别名）：没有可归并的两个档案
        raise HTTPException(
            status_code=422,
            detail=f"{requested_source} 与 {requested_target} 已指向同一档案 {source_id}，无需归并",
        )

    src_rows = conn.execute(
        "SELECT * FROM readings WHERE exhibit_id = ? ORDER BY reading_id", (source_id,)
    ).fetchall()
    tgt_rows = conn.execute(
        "SELECT * FROM readings WHERE exhibit_id = ? ORDER BY reading_id", (target_id,)
    ).fetchall()

    src_map = {r["reading_id"]: r for r in src_rows}
    tgt_map = {r["reading_id"]: r for r in tgt_rows}

    collisions: list[MergeCollision] = []
    auto_dup = 0
    conflicts = 0
    for rid in sorted(src_map.keys() & tgt_map.keys()):
        s, t = src_map[rid], tgt_map[rid]
        same = s["ts"] == t["ts"] and s["lux"] == t["lux"] and \
            (s["source_id"] if "source_id" in s.keys() else None) == \
            (t["source_id"] if "source_id" in t.keys() else None)
        if same:
            kind = "duplicate"
            auto_dup += 1
        else:
            kind = "conflict"
            conflicts += 1
        collisions.append(MergeCollision(
            reading_id=rid,
            kind=kind,
            source_ts=to_iso(s["ts"]),
            source_lux=s["lux"],
            target_ts=to_iso(t["ts"]),
            target_lux=t["lux"],
            source_source_id=s["source_id"] if "source_id" in s.keys() else None,
            target_source_id=t["source_id"] if "source_id" in t.keys() else None,
        ))

    # 来源优先级配置归并：列出同一生效时刻对同一来源的优先级分歧，
    # 这些分歧必须在提交时逐条裁决，冲突不得静默覆盖。
    src_priority_rows = _priority_rows(conn, source_id)
    tgt_priority_rows = _priority_rows(conn, target_id)
    priority_collisions = _priority_collisions(src_priority_rows, tgt_priority_rows)

    metadata: dict[str, MergeMetaDiff] = {}
    for field, sv, tv in (
        ("name", src["name"], tgt["name"]),
        ("dose_limit_luxh", src["dose_limit_luxh"], tgt["dose_limit_luxh"]),
        ("max_interval_seconds", src["max_interval_seconds"], tgt["max_interval_seconds"]),
    ):
        diff = _meta_diff_field(field, sv, tv)
        if diff is not None:
            metadata[field] = diff

    source_only = len(src_map) - len(collisions)

    token = uuid.uuid4().hex
    conn.execute(
        """INSERT INTO merge_previews
           (token, source_id, target_id, source_meta_hash, target_meta_hash,
            source_read_hash, target_read_hash, source_priority_hash,
            target_priority_hash, conflict_count)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            token, source_id, target_id,
            _meta_hash(src), _meta_hash(tgt),
            _readings_hash(list(src_rows)), _readings_hash(list(tgt_rows)),
            _priority_hash(src_priority_rows) or _EMPTY_PRIORITY_HASH,
            _priority_hash(tgt_priority_rows) or _EMPTY_PRIORITY_HASH,
            conflicts,
        ),
    )
    conn.commit()

    return MergePreviewOut(
        token=token,
        requested_source_id=requested_source,
        requested_target_id=requested_target,
        source_id=source_id,
        target_id=target_id,
        metadata=metadata,
        source_reading_count=len(src_rows),
        target_reading_count=len(tgt_rows),
        collisions=collisions,
        priority_collisions=priority_collisions,
        auto_duplicate_count=auto_dup,
        conflict_count=conflicts,
        source_only_count=source_only,
        ready_to_commit=conflicts == 0 and not priority_collisions,
    )


# ---------- 提交 ----------

class _PreviewChanged(Exception):
    """预演后档案被写入/修改/删除，提交必须 409。"""


class _ChainBroken(Exception):
    """来源或目标的存证收据链校验失败（断链/内容不符），归并必须 409 拒绝。"""


class _CommitValidation(Exception):
    """裁决不合法或不完整，422。"""


def _validate_resolutions(
    resolutions: list[MergeResolution],
    conflict_ids: set[str],
    target_existing_ids: set[str],
    source_only_ids: set[str],
) -> None:
    # 改名后的新 ID 不能与目标现有读数、即将搬迁的来源独有读数或其他新 ID 撞车
    reserved = target_existing_ids | source_only_ids
    seen: set[str] = set()
    new_ids: set[str] = set()
    for r in resolutions:
        if r.reading_id not in conflict_ids:
            raise _CommitValidation(
                f"读数 {r.reading_id} 不是待裁决的内容碰撞（冲突 ID："
                f"{sorted(conflict_ids) or '无'}）"
            )
        if r.reading_id in seen:
            raise _CommitValidation(f"读数 {r.reading_id} 的裁决重复出现")
        seen.add(r.reading_id)
        if r.action == "rename_source":
            new_id = r.new_id
            assert new_id is not None  # 由 pydantic 模型保证
            if new_id in reserved:
                raise _CommitValidation(
                    f"读数 {r.reading_id} 指定的新 ID {new_id} 与归并后已有的读数 ID 冲突"
                )
            if new_id in new_ids:
                raise _CommitValidation(f"新读数 ID {new_id} 被多次指定")
            new_ids.add(new_id)
    missing = sorted(conflict_ids - seen)
    if missing:
        raise _CommitValidation(f"仍有 {len(missing)} 条碰撞未裁决，未解决不得提交: {missing}")


def commit_merge(
    conn: sqlite3.Connection,
    token: str,
    resolutions: list[MergeResolution],
    priority_resolutions: list[MergePriorityResolution] | None = None,
) -> MergeCommitOut:
    """在单个 SQLite 事务中执行归并；同令牌重复提交回放首次结果。

    读数碰撞由 ``resolutions`` 逐条裁决；来源优先级配置分歧由
    ``priority_resolutions`` 逐条裁决（keep_source / keep_target），
    归并后在目标档案重写合并配置，冲突不得静默覆盖。
    """
    priority_resolutions = priority_resolutions or []
    preview = conn.execute(
        "SELECT * FROM merge_previews WHERE token = ?", (token,)
    ).fetchone()
    if preview is None:
        raise HTTPException(status_code=404, detail=f"预演令牌不存在或已失效: {token}")

    # 已提交过：回放首次结果（令牌同时保证版本检查不会再跑）
    if preview["committed_at"] is not None:
        assert preview["result_json"] is not None
        data = json.loads(preview["result_json"])
        return MergeCommitOut(**data, already_committed=True)

    source_id, target_id = preview["source_id"], preview["target_id"]

    # 立即拿写锁，确保“校验 + 搬迁 + 删除来源”原子完成；
    # 预演哈希也在锁内重算，杜绝预演后的并发写入。
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.Error as e:  # 极端情况下前序语句残留事务
        raise HTTPException(status_code=500, detail=f"无法开启归并事务: {e}")

    try:
        # 锁内重读：可能另一个请求已用同令牌完成提交
        preview = conn.execute(
            "SELECT * FROM merge_previews WHERE token = ?", (token,)
        ).fetchone()
        if preview is None:
            raise HTTPException(status_code=404, detail=f"预演令牌不存在或已失效: {token}")
        if preview["committed_at"] is not None:
            assert preview["result_json"] is not None
            conn.commit()  # 此时尚未写入，COMMIT 仅释放 IMMEDIATE 锁
            return MergeCommitOut(**json.loads(preview["result_json"]), already_committed=True)

        src = conn.execute("SELECT * FROM exhibits WHERE id = ?", (source_id,)).fetchone()
        tgt = conn.execute("SELECT * FROM exhibits WHERE id = ?", (target_id,)).fetchone()
        if src is None or tgt is None:
            raise _PreviewChanged("预演后来源或目标档案已被删除")

        src_rows = conn.execute(
            "SELECT * FROM readings WHERE exhibit_id = ?", (source_id,)
        ).fetchall()
        tgt_rows = conn.execute(
            "SELECT * FROM readings WHERE exhibit_id = ?", (target_id,)
        ).fetchall()
        if (
            _meta_hash(src) != preview["source_meta_hash"]
            or _meta_hash(tgt) != preview["target_meta_hash"]
            or _readings_hash(list(src_rows)) != preview["source_read_hash"]
            or _readings_hash(list(tgt_rows)) != preview["target_read_hash"]
            or _priority_hash(_priority_rows(conn, source_id))
                != (preview["source_priority_hash"] or _EMPTY_PRIORITY_HASH)
            or _priority_hash(_priority_rows(conn, target_id))
                != (preview["target_priority_hash"] or _EMPTY_PRIORITY_HASH)
        ):
            raise _PreviewChanged("预演后来源或目标档案发生写入或修改（含来源优先级配置变更），预演结果已过期")

        # 归并前分别校验来源与目标的既有存证收据链：任一断链或内容不符即
        # 拒绝归并，事务回滚、不产生部分变更。两条链在归并后原样保留，
        # 旧哈希绝不重写，归并后的新上报继续接入目标原链。
        for cid in (source_id, target_id):
            verdict = verify_chain_rows(load_chain(conn, cid))
            if not verdict.ok:
                failure = verdict.first_failure or {}
                raise _ChainBroken(
                    f"档案 {cid} 的存证收据链校验失败：第 {failure.get('seq')} 张"
                    f"（{failure.get('reason')}），归并被拒绝"
                )

        src_map = {r["reading_id"]: r for r in src_rows}
        tgt_map = {r["reading_id"]: r for r in tgt_rows}
        shared = src_map.keys() & tgt_map.keys()

        def _same_reading(s: sqlite3.Row, t: sqlite3.Row) -> bool:
            return (
                s["ts"] == t["ts"] and s["lux"] == t["lux"]
                and (s["source_id"] if "source_id" in s.keys() else None)
                == (t["source_id"] if "source_id" in t.keys() else None)
            )

        conflict_ids = {rid for rid in shared if not _same_reading(src_map[rid], tgt_map[rid])}
        dup_ids = shared - conflict_ids
        source_only_ids = set(src_map) - shared

        _validate_resolutions(
            resolutions,
            conflict_ids=conflict_ids,
            # rename 的新 ID 不得与目标侧任何现存读数或即将搬迁的来源独有读数冲突
            target_existing_ids=set(tgt_map),
            source_only_ids=source_only_ids,
        )
        res_by_id = {r.reading_id: r for r in resolutions}

        # 来源优先级配置分歧必须逐条裁决（keep_source/keep_target），
        # 未全部解决不得提交，冲突不得静默覆盖。
        src_priority_rows = _priority_rows(conn, source_id)
        tgt_priority_rows = _priority_rows(conn, target_id)
        pending_priority: set[tuple[float, str]] = set()
        for c in _priority_collisions(src_priority_rows, tgt_priority_rows):
            eff_epoch = datetime.fromisoformat(
                c.effective_at.replace("Z", "+00:00")
            ).timestamp()
            pending_priority.add((eff_epoch, c.source_id))
        seen_priority: set[tuple[float, str]] = set()
        decisions: dict[tuple[float, str], str] = {}
        for pr in priority_resolutions:
            key = (ensure_aware_ts(pr.effective_at), pr.source_id)
            if key in seen_priority:
                raise _CommitValidation(
                    f"来源 {pr.source_id} 在 {pr.effective_at} 的优先级裁决重复出现"
                )
            if key not in pending_priority:
                raise _CommitValidation(
                    f"来源 {pr.source_id} 在 {pr.effective_at} 不存在优先级分歧，"
                    "无需裁决（只能裁决预演列出的分歧）"
                )
            seen_priority.add(key)
            decisions[key] = pr.action
        missing_priority = sorted(pending_priority - seen_priority)
        if missing_priority:
            raise _CommitValidation(
                f"仍有 {len(missing_priority)} 条来源优先级配置分歧未裁决，"
                f"未全部解决不得提交（冲突不得静默覆盖）: {missing_priority[:10]}"
            )

        # 1) 逐条搬迁来源读数
        moved = 0
        applied: list[MergeResolutionResult] = []
        for rid in sorted(src_map):
            s = src_map[rid]
            if rid in dup_ids:
                # 完全一致：保留目标，丢弃来源副本（自动去重）
                continue
            if rid in conflict_ids:
                decision = res_by_id[rid]
                if decision.action == "keep_target":
                    applied.append(MergeResolutionResult(reading_id=rid, action="keep_target"))
                    continue
                if decision.action == "keep_source":
                    conn.execute(
                        "UPDATE readings SET ts = ?, lux = ?, source_id = ? "
                        "WHERE exhibit_id = ? AND reading_id = ?",
                        (s["ts"], s["lux"], s["source_id"], target_id, rid),
                    )
                    applied.append(MergeResolutionResult(reading_id=rid, action="keep_source"))
                    continue
                # rename_source：以新 ID 插入来源内容
                new_id = decision.new_id
                assert new_id is not None
                conn.execute(
                    "INSERT INTO readings (exhibit_id, reading_id, ts, lux, source_id, "
                    "received_at, voided_at, voided_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (target_id, new_id, s["ts"], s["lux"], s["source_id"], s["received_at"],
                     s["voided_at"], s["voided_by"]),
                )
                applied.append(
                    MergeResolutionResult(reading_id=rid, action="rename_source", new_id=new_id)
                )
                continue
            # 仅来源存在：整体搬迁，保留读数 ID、原始接收时刻、来源与作废标记
            conn.execute(
                "INSERT INTO readings (exhibit_id, reading_id, ts, lux, source_id, "
                "received_at, voided_at, voided_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (target_id, rid, s["ts"], s["lux"], s["source_id"], s["received_at"],
                 s["voided_at"], s["voided_by"]),
            )
            moved += 1

        # 2) 来源名下的旧别名改指向目标（保持一跳）；
        #    拒绝把任何已有别名链重指回别名（那只会从既有别名出发，不可能成环，
        #    但 canonical 列必须指向真实档案，这里统一约束为目标 ID）。
        conn.execute(
            "UPDATE exhibit_aliases SET canonical_id = ? WHERE canonical_id = ?",
            (target_id, source_id),
        )
        # 3) 来源 ID 成为目标的永久别名。alias_id 若已存在，
        #    只可能指向第三方档案（主键冲突）——这种情况理论上不可能，
        #    因为 source 本身是规范 ID；用 INSERT OR FAIL 快速失败。
        conn.execute(
            "INSERT INTO exhibit_aliases (alias_id, canonical_id) VALUES (?, ?)",
            (source_id, target_id),
        )
        # 4) 归并来源优先级配置：按逐条裁决合并，在目标档案重写配置。
        #    目标原有版本在同一事务内删除并按合并结果重建——历史读数与收据
        #    不变，仲裁本就在查询时按生效时刻重放；冲突分歧已全部显式裁决，
        #    不存在静默覆盖。归并时刻之前只有一侧存在的配置同样保留。
        #    合并后某生效时刻若一个条目都不剩（例如裁决采用在该时刻尚未生效
        #    的一侧配置），则不写空版本：该时刻回到“无生效版本”的 legacy，
        #    与“所有来源都未列入”的仲裁结果一致，也不产生违背“版本含完整
        #    清单”约定的空行。
        merged_steps = _merge_priority(src_priority_rows, tgt_priority_rows, decisions)
        priority_applied: list[MergePriorityResolutionResult] = []
        if merged_steps:
            conn.execute(
                "DELETE FROM source_priority_entries WHERE exhibit_id = ?", (target_id,)
            )
            conn.execute(
                "DELETE FROM source_priority_versions WHERE exhibit_id = ?", (target_id,)
            )
        for eff, entries in merged_steps:
            write_entries = [(sid, pri) for sid, pri in entries if pri is not None]
            if not write_entries:
                continue
            cur = conn.execute(
                "INSERT INTO source_priority_versions (exhibit_id, effective_at) VALUES (?, ?)",
                (target_id, eff),
            )
            version_id = cur.lastrowid
            for source_id_value, priority_value in write_entries:
                conn.execute(
                    "INSERT INTO source_priority_entries "
                    "(version_id, exhibit_id, source_id, priority) VALUES (?, ?, ?, ?)",
                    (version_id, target_id, source_id_value, priority_value),
                )
        # 记录裁决明细（仅预演列出的分歧；自动合并的一致/单边条目不列出）
        for eff_epoch, source_id_value in sorted(decisions):
            action = decisions[(eff_epoch, source_id_value)]
            src_steps = _priority_steps(src_priority_rows)
            tgt_steps = _priority_steps(tgt_priority_rows)
            sv = _value_at(src_steps, eff_epoch, source_id_value)
            tv = _value_at(tgt_steps, eff_epoch, source_id_value)
            priority_applied.append(MergePriorityResolutionResult(
                source_id=source_id_value,
                effective_at=to_iso(eff_epoch),
                action=action,
                priority=sv if action == "keep_source" else tv,
            ))

        # 5) 删除来源档案读数与来源档案本身（须在别名落库、配置合并之后；
        #    来源优先级配置随档案删除级联清除）
        conn.execute("DELETE FROM readings WHERE exhibit_id = ?", (source_id,))
        conn.execute("DELETE FROM exhibits WHERE id = ?", (source_id,))

        final_count = conn.execute(
            "SELECT COUNT(*) AS n FROM readings WHERE exhibit_id = ?", (target_id,)
        ).fetchone()["n"]
        aliases = aliases_of(conn, target_id)
        # 双方既有收据链原样保留（归并不触碰收据表），列出链标识供分别核查
        receipt_chains = [
            cid for cid in (source_id, target_id)
            if conn.execute(
                "SELECT 1 FROM ingest_receipts WHERE chain_id = ? LIMIT 1", (cid,)
            ).fetchone() is not None
        ]

        result = MergeCommitOut(
            token=token,
            source_id=source_id,
            target_id=target_id,
            name=tgt["name"],
            dose_limit_luxh=tgt["dose_limit_luxh"],
            max_interval_seconds=tgt["max_interval_seconds"],
            moved_count=moved,
            auto_deduplicated_count=len(dup_ids),
            resolutions_applied=applied,
            priority_resolutions_applied=priority_applied,
            final_reading_count=final_count,
            aliases=aliases,
            receipt_chains=receipt_chains,
            already_committed=False,
        )
        # 5) 固化结果供同令牌重放；先写结果再置 committed_at
        conn.execute(
            "UPDATE merge_previews SET result_json = ?, committed_at = "
            "strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE token = ?",
            (json.dumps(result.model_dump(exclude={"already_committed"}),
                        ensure_ascii=False), token),
        )
        conn.commit()
        return result

    except _PreviewChanged as e:
        conn.rollback()
        raise HTTPException(status_code=409, detail=f"归并预演已失效：{e}")
    except _ChainBroken as e:
        conn.rollback()
        raise HTTPException(status_code=409, detail=str(e))
    except _CommitValidation as e:
        conn.rollback()
        raise HTTPException(status_code=422, detail=str(e))
    except sqlite3.IntegrityError as e:
        conn.rollback()
        # 搬迁/改名与目标读数主键撞车等：数据层兜底，不产生部分变更
        raise HTTPException(status_code=422, detail=f"归并被拒绝（读数 ID 冲突或别名冲突）: {e}")
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
