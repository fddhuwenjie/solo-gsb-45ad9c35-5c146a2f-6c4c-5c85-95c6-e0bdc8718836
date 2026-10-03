"""照度采集质量诊断：在规范档案的**完整时间线**上识别采集质量事件。

事件类型：
- ``timestamp_conflict``：**旧行为（legacy，该时刻无生效来源优先级版本）**
  下，同一时刻存在**照度不同**的原始读数时产生——该时刻有歧义，不参与任何
  连续链并同时截断其前后侧的 stuck 链；同一时刻照度完全相同的多条读数先
  合并为时间线上的一个点（读数 ID 全部保留）；
  多来源仲裁冲突时刻（最高优先级并列且照度不同，见 ``arbitration``）**保持
  冲突、不进入诊断时间线**：链在其两侧中断，但不产生事件（它已在权威时间线
  查询中以 ``status=conflict`` 显式给出，不是采集质量异常）。
- ``gap``：两个相邻无歧义读数的间隔**超过**展品 ``max_interval_seconds``
  （恰好等于不算 gap），缺测段两侧各自成链；
- ``jump``：间隔合规（不超过 max_interval_seconds）但照度变化速率
  ``|Δlux| / Δt`` **超过**调用方给定上限（恰好等于不算 jump）；
- ``stuck``：由相邻无歧义读数组成的**最大**连续链，链上每一对相邻读数的
  照度差都不超过卡死容差，且首末读数累计时长达到最短持续时长。
  gap、jump、照度差超过容差的相邻对、legacy 同时刻异值与仲裁冲突时刻都会
  截断链。

事件先基于完整时间线一次性形成（与查询窗口无关），再由调用方筛选与窗口
**相交**的事件；筛选只做包含/排除，绝不裁剪事件的原始边界与相关读数 ID，
因此缩窄窗口不会改变任何事件本身。最终按 (起点, 类型) 稳定排序。
"""
from __future__ import annotations

from dataclasses import dataclass

from .arbitration import TimelineGroup, arbitrate

# 起点相同时的类型先后（实际数据中不同类型事件一般不会共用起点，
# 这里仅给出确定的总排序，保证输出稳定）
TYPE_ORDER: dict[str, int] = {
    "gap": 0,
    "jump": 1,
    "stuck": 2,
    "timestamp_conflict": 3,
}


@dataclass
class DiagnosisEvent:
    """一个采集质量事件；时间均为 Unix 秒（UTC）。"""

    type: str  # gap | jump | stuck | timestamp_conflict
    start: float
    end: float
    reading_ids: list[str]
    interval_seconds: float | None = None       # gap / jump：相邻读数间隔
    rate_lux_per_second: float | None = None    # jump：|Δlux| / Δt（lux/秒）
    duration_seconds: float | None = None       # stuck：首末读数累计时长
    lux_span: float | None = None               # stuck：链上照度跨度；conflict：同时刻不同值跨度


@dataclass
class _Point:
    """时间线上一个无歧义时刻：同时刻同值读数合并而成。"""

    ts: float
    lux: float
    ids: list[str]


def build_events_from_groups(
    groups: list[TimelineGroup],
    *,
    max_interval_s: float,
    max_rate_lux_per_s: float,
    stuck_tol_lux: float,
    min_stuck_s: float,
) -> list[DiagnosisEvent]:
    """在已仲裁的时刻分组上构建质量事件（尚未按窗口筛选）。

    - legacy 时刻（无生效版本）：与旧版完全一致——同值合并为一个点，异值记
      ``timestamp_conflict``；原始多条读数在剂量侧各自成点（同时刻零间隔），
      诊断侧按同值合并/异值冲突处理；
    - 仲裁权威点：并列读数同值，天然是合并后的一个点；
    - 仲裁冲突时刻：不落点、从两侧中断连续链（不产生诊断事件）。
    """
    events: list[DiagnosisEvent] = []
    run: list[_Point] = []

    def flush_stuck() -> None:
        """结束当前平稳链：时长达标则合并为一个最大 stuck 事件。"""
        if len(run) >= 2:
            duration = run[-1].ts - run[0].ts
            if duration >= min_stuck_s:
                luxs = [p.lux for p in run]
                ids: list[str] = []
                for p in run:
                    ids.extend(p.ids)
                events.append(DiagnosisEvent(
                    type="stuck",
                    start=run[0].ts,
                    end=run[-1].ts,
                    reading_ids=ids,
                    duration_seconds=duration,
                    lux_span=max(luxs) - min(luxs),
                ))
        run.clear()

    def start_after_break(point: _Point) -> None:
        flush_stuck()
        run.append(point)

    prev: _Point | None = None
    for g in groups:
        if g.is_break():
            # 多来源仲裁冲突：保持冲突、不进诊断时间线；截断两侧链，不产生事件
            flush_stuck()
            prev = None
            continue

        if g.legacy:
            # 旧行为：原始读数按照度去重，同值合并、异值记 timestamp_conflict
            members = sorted(g.candidates, key=lambda c: c.reading_id)
            lux_values = {c.lux for c in members}
            ids = [c.reading_id for c in members]
            if len(lux_values) > 1:
                flush_stuck()
                events.append(DiagnosisEvent(
                    type="timestamp_conflict",
                    start=g.ts,
                    end=g.ts,
                    reading_ids=ids,
                    lux_span=max(lux_values) - min(lux_values),
                ))
                prev = None
                continue
            cur = _Point(ts=g.ts, lux=members[0].lux, ids=ids)
        else:
            # 仲裁权威点：winners 照度相同，合并为一个点（ID 按读数 ID 排序）
            assert g.status == "point"
            cur = _Point(
                ts=g.ts, lux=g.lux,  # type: ignore[arg-type]
                ids=sorted(c.reading_id for c in g.winners),
            )

        if prev is not None:
            dt = cur.ts - prev.ts  # 分组后必为非负；同时刻零间隔不产生 gap/jump
            delta = abs(cur.lux - prev.lux)
            if dt > max_interval_s:
                # 间隔超限：gap，gap 两侧各自成链（新链从 cur 起）
                flush_stuck()
                events.append(DiagnosisEvent(
                    type="gap",
                    start=prev.ts,
                    end=cur.ts,
                    reading_ids=prev.ids + cur.ids,
                    interval_seconds=dt,
                ))
                run = [cur]
            elif dt > 0 and delta / dt > max_rate_lux_per_s:
                # 间隔合规但速率超限：jump，同样截断
                flush_stuck()
                events.append(DiagnosisEvent(
                    type="jump",
                    start=prev.ts,
                    end=cur.ts,
                    reading_ids=prev.ids + cur.ids,
                    interval_seconds=dt,
                    rate_lux_per_second=delta / dt,
                ))
                run = [cur]
            elif delta > stuck_tol_lux:
                # 既非 gap 也非 jump，但照度差超过卡死容差：不能并入 stuck 链
                flush_stuck()
                run = [cur]
            else:
                # 相邻照度差在容差内（dt==0 的同时刻合并点 delta==0，同样并入）：
                # 延续当前最大平稳链
                run.append(cur)
        else:
            run = [cur]
        prev = cur

    flush_stuck()
    return events


def build_events(
    readings: list[tuple[str, float, float]],  # (reading_id, ts, lux)
    *,
    max_interval_s: float,
    max_rate_lux_per_s: float,
    stuck_tol_lux: float,
    min_stuck_s: float,
) -> list[DiagnosisEvent]:
    """旧入口：在无来源优先级配置的原始读数上构建事件。

    全部读数视为未标注来源、无生效版本（legacy），与历史行为完全一致：
    按 (时刻, 读数 ID) 排序，同时刻同值合并、异值记 timestamp_conflict。
    """
    groups = arbitrate(
        [(rid, ts, lux, None) for rid, ts, lux in readings],
        versions=[],
    )
    return build_events_from_groups(
        groups,
        max_interval_s=max_interval_s,
        max_rate_lux_per_s=max_rate_lux_per_s,
        stuck_tol_lux=stuck_tol_lux,
        min_stuck_s=min_stuck_s,
    )


def intersect_window(
    events: list[DiagnosisEvent], start_s: float, end_s: float
) -> list[DiagnosisEvent]:
    """筛选与闭窗口 [start_s, end_s] 相交的事件；边界相接也算相交。

    只筛选，不改变事件的 start / end / reading_ids 等任何内容。
    """
    return [e for e in events if e.start <= end_s and e.end >= start_s]


def sort_events(events: list[DiagnosisEvent]) -> list[DiagnosisEvent]:
    """按 (起点, 类型) 稳定排序。"""
    return sorted(events, key=lambda e: (e.start, TYPE_ORDER[e.type]))
