"""Pydantic 请求/响应模型。"""
from __future__ import annotations

import re
from datetime import date as dt_date
from datetime import datetime, timezone
from typing import Annotated, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_serializer, model_validator


def ensure_aware(dt: datetime) -> datetime:
    """不带时区的时间一律按 UTC 解释。"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


class ExhibitCreate(BaseModel):
    id: str | None = Field(default=None, min_length=1, max_length=200,
                           description="展品 ID，不填则自动生成")
    name: str = Field(min_length=1, max_length=200, description="展品名称")
    dose_limit_luxh: float = Field(gt=0, description="累计光照剂量上限（lux·h）")
    max_interval_seconds: float = Field(gt=0, description="最大采样间隔（秒）")


class ExhibitOut(BaseModel):
    requested_id: str = Field(description="请求中使用的 ID（可能是别名）")
    canonical_id: str = Field(description="规范档案 ID；归并后所有别名均指向它")
    name: str
    dose_limit_luxh: float
    max_interval_seconds: float
    created_at: str


class ReadingIn(BaseModel):
    id: str = Field(min_length=1, max_length=200, description="读数 ID，幂等键")
    ts: datetime = Field(description="采样时刻，ISO 8601；缺省时区按 UTC 处理")
    lux: float = Field(ge=0, description="照度（lux）")
    source_id: str | None = Field(
        default=None, min_length=1, max_length=200,
        description="读数来源（采集器/传感器 ID）；不填为未标注来源读数，"
                    "与旧请求行为一致。同一展品同一采样时刻的不同来源读数全部留存",
    )

    @field_validator("ts")
    @classmethod
    def _aware(cls, v: datetime) -> datetime:
        return ensure_aware(v)


class ReadingBatch(BaseModel):
    readings: list[ReadingIn] = Field(min_length=1, max_length=10000)


class ReadingResult(BaseModel):
    id: str
    status: str  # inserted | duplicate | conflict


# ---------- 照度上报存证（收据链） ----------

class ReceiptReadingOut(BaseModel):
    """收据中记录的请求读数（规范化：ts 统一为 UTC、'Z' 结尾的 ISO 8601）。

    旧版收据没有 ``source_id`` 键；仅当上报携带来源时才记录（缺键在重算时
    等价于未标注来源），保证既有收据的内容哈希逐字节可复现。序列化时同样
    省略 null 来源键，使从响应内容重算的 content_hash 与落库哈希一致。
    """

    id: str
    ts: str
    lux: float
    source_id: str | None = None

    @model_serializer(mode="plain")
    def _serialize(self) -> dict:
        data = {"id": self.id, "ts": self.ts, "lux": self.lux}
        if self.source_id is not None:
            data["source_id"] = self.source_id
        return data


class ReceiptRequestOut(BaseModel):
    readings: list[ReceiptReadingOut] = Field(description="本次上报请求中的读数（按提交顺序）")


class ReceiptOutcomeOut(BaseModel):
    inserted: int
    duplicates: int
    conflicts: int
    results: list[ReadingResult] = Field(description="每条读数的处理结果（与请求顺序一致）")


class ReceiptContentOut(BaseModel):
    """收据内容：计算 content_hash 的规范序列化对象。"""

    chain_id: str = Field(description="链标识：写入时的规范档案 ID")
    seq: int = Field(description="链内序号，从 1 连续递增")
    receipt_id: str
    requested_id: str = Field(description="上报请求中使用的展品 ID（可能是别名）")
    created_at: str
    request: ReceiptRequestOut
    outcome: ReceiptOutcomeOut


class ReceiptOut(BaseModel):
    chain_id: str = Field(description="链标识：写入时的规范档案 ID")
    seq: int = Field(description="链内序号，从 1 连续递增")
    receipt_id: str
    prev_hash: str = Field(description="上一张收据的链哈希；首张为 64 个 0（创世）")
    content_hash: str = Field(description="SHA-256(规范序列化内容)")
    chain_hash: str = Field(description="SHA-256(prev_hash + content_hash)")
    created_at: str
    content: ReceiptContentOut
    voids: list[ReceiptVoidOut] = Field(
        default_factory=list,
        description="该收据已完成的批次更正（作废标记）；更正不改写收据内容、序号与链哈希，"
                    "故标记放在 content 之外。一张收据至多有一条成功更正",
    )


class ReceiptListOut(BaseModel):
    requested_id: str = Field(description="请求中使用的展品 ID（可能是别名）")
    canonical_id: str = Field(description="规范档案 ID")
    chain_id: str = Field(description="收据链标识，即规范档案 ID")
    receipts: list[ReceiptOut]


class ChainReceiptsOut(BaseModel):
    chain_id: str
    receipts: list[ReceiptOut]


class ChainFailureOut(BaseModel):
    seq: int = Field(description="首个校验失败的收据序号")
    reason: str = Field(
        description="seq_gap（序号不连续）/ prev_hash_mismatch（断链）/ "
                    "content_hash_mismatch（内容不符）/ chain_hash_mismatch（链哈希不符）",
    )
    detail: str


class ChainVerifyOut(BaseModel):
    chain_id: str = Field(description="链标识（写入时的规范档案 ID）")
    ok: bool = Field(description="整链是否通过校验")
    checked: int = Field(description="已检查且通过的收据数量；失败时即首个失败位置之前的数量")
    total: int = Field(description="链上收据总数")
    first_failure: ChainFailureOut | None = Field(
        description="首个断链或内容不符的位置；通过时为 null",
    )


class IngestResponse(BaseModel):
    requested_id: str = Field(description="请求中使用的展品 ID（可能是别名）")
    canonical_id: str = Field(description="读数实际写入的规范档案 ID")
    inserted: int
    duplicates: int
    conflicts: int
    results: list[ReadingResult]
    receipt: ReceiptOut = Field(description="本次上报的存证收据（与读数同一事务生成）")


# ---------- 照度上报批次更正（作废） ----------

class ReadingCorrectionRequest(BaseModel):
    receipt_id: str = Field(min_length=1, max_length=200,
                            description="被更正批次的存证收据 receipt_id（上报响应中返回）")
    request_key: str = Field(min_length=1, max_length=200,
                             description="更正幂等键：同键重放首次结果，换目标（收据/档案/原因）→ 409")
    reason: str = Field(min_length=1, max_length=2000, description="作废原因")


class VoidedReadingOut(BaseModel):
    """一条被更正作废的读数（内容取自被更正收据记录的插入结果）。"""

    id: str = Field(description="读数 ID")
    ts: str = Field(description="采样时刻（UTC，ISO 8601，Z 结尾）")
    lux: float = Field(description="作废时读数记录的照度（lux）")
    source_id: str | None = Field(
        default=None, description="读数上报来源；旧版收据/未标注来源为 null",
    )


class ReceiptVoidOut(BaseModel):
    """收据上的作废标记：查询收据时附在 content 之外（content 与哈希永不改变）。"""

    request_key: str = Field(description="完成本次作废的更正 request_key")
    reason: str = Field(description="作废原因")
    voided_count: int = Field(description="该收据中被作废的读数条数")
    voided_readings: list[VoidedReadingOut] = Field(description="被作废的读数明细（按首次更正请求顺序去重）")
    voided_at: str = Field(description="作废时刻（UTC，ISO 8601，Z 结尾）")


class ReadingCorrectionOut(BaseModel):
    requested_id: str = Field(description="请求中使用的展品 ID（可能是别名）")
    canonical_id: str = Field(description="解析后的规范档案 ID（即被更正收据的链标识）")
    receipt_id: str = Field(description="被更正的存证收据 receipt_id")
    chain_id: str = Field(description="被更正收据的链标识（写入时的规范档案 ID）")
    seq: int = Field(description="被更正在其链内的序号")
    request_key: str = Field(description="本次更正的幂等键")
    reason: str
    voided_count: int = Field(description="本次实际作废的读数条数")
    voided_readings: list[VoidedReadingOut] = Field(
        description="本次作废的读数：仅该收据实际插入、作废时仍归属该档案且内容未变的读数",
    )
    voided_at: str = Field(description="作废时刻（UTC，ISO 8601，Z 结尾）")
    already_committed: bool = Field(
        default=False,
        description="false 表示本次首次落库；true 表示同一 request_key 重放，返回首次结果",
    )


class ReadingOut(BaseModel):
    id: str
    ts: str
    lux: float
    source_id: str | None = Field(description="读数来源；未标注来源为 null（旧版读数）")
    received_at: str


class ReadingListOut(BaseModel):
    requested_id: str
    canonical_id: str
    readings: list[ReadingOut]


# ---------- 来源优先级版本（多来源仲裁） ----------

class PriorityEntryIn(BaseModel):
    """一个来源的优先级：priority 越大越权威；同优先级为并列档。"""

    source_id: str = Field(min_length=1, max_length=200, description="读数来源 ID（与上报读数的 source_id 对应）")
    priority: int = Field(description="优先级（整数，越大越权威）；同值为并列，并列且照度不同则时刻冲突")


class SourcePriorityPut(BaseModel):
    """新增一个来源优先级版本（版本只增不改，按生效时刻生效）。"""

    expected_version: int = Field(
        ge=0,
        description="客户端所见的当前最新版本号；从未配置时为 0。与服务端不一致 → 409",
    )
    effective_at: datetime = Field(
        description="新版本的生效时刻（含端点，ISO 8601；缺省时区按 UTC）："
                    "仅对该时刻及以后采样的读数生效，历史仲裁结果不改写",
    )
    priorities: list[PriorityEntryIn] = Field(
        min_length=1, max_length=1000, description="完整的来源优先级清单（整体写入新版本，不增量合并）",
    )

    @field_validator("effective_at")
    @classmethod
    def _aware(cls, v: datetime) -> datetime:
        return ensure_aware(v)

    @model_validator(mode="after")
    def _unique_sources(self) -> SourcePriorityPut:
        ids = [p.source_id for p in self.priorities]
        if len(set(ids)) != len(ids):
            raise ValueError("来源优先级清单内 source_id 重复：同一版本中每个来源只能出现一次")
        return self


class PriorityEntryOut(BaseModel):
    source_id: str
    priority: int


class SourcePriorityVersionOut(BaseModel):
    version: int = Field(description="版本号（全局连续递增，按创建顺序分配）")
    effective_at: str = Field(description="生效时刻（UTC，ISO 8601，Z 结尾，含端点）")
    created_at: str = Field(description="版本写入时刻（UTC，ISO 8601）")
    priorities: list[PriorityEntryOut] = Field(description="该版本的完整来源优先级清单（priority 降序）")


class SourcePriorityConfigOut(BaseModel):
    requested_id: str = Field(description="请求中使用的展品 ID（可能是别名）")
    canonical_id: str = Field(description="规范档案 ID")
    current_version: int = Field(description="当前最新版本号；从未配置时为 0")
    versions: list[SourcePriorityVersionOut] = Field(
        description="全部历史版本（按生效时刻升序）；版本只增不改",
    )


class TimelineCandidateOut(BaseModel):
    """权威时间线上某采样时刻留存的一条原始读数。"""

    id: str = Field(description="读数 ID")
    ts: str = Field(description="采样时刻（UTC，ISO 8601，Z 结尾）")
    lux: float
    source_id: str | None = Field(description="读数来源；未标注来源为 null")


class AuthoritativeTimelinePointOut(BaseModel):
    """一个采样时刻的仲裁结果。"""

    ts: str = Field(description="采样时刻（UTC，ISO 8601，Z 结尾）")
    status: Literal["authoritative", "conflict", "legacy"] = Field(
        description=(
            "authoritative：按生效版本仲裁出唯一权威照度；"
            "conflict：最高优先级并列来源照度不一致，该时刻保持冲突、不进入剂量/诊断时间线；"
            "legacy：该时刻无生效优先级版本，保持旧行为（原始读数各自成点）"
        ),
    )
    version: int | None = Field(
        description="该时刻实际生效的优先级版本号；legacy（无生效版本）时为 null",
    )
    lux: float | None = Field(
        description="权威照度：authoritative 为仲裁值；conflict 为 null；"
                    "legacy 且原始照度不一致时为 null，唯一照度时为该照度",
    )
    priority: int | None = Field(
        description="权威来源所在优先级档（未列入来源胜出时为 null，已列来源恒为整数）",
    )
    source_ids: list[str] = Field(
        description="权威/并列冲突的来源 ID（未标注来源读数胜出时为空列表；candidates 含全部留存来源）",
    )
    reading_ids: list[str] = Field(
        description="权威/并列冲突的读数 ID（同值合并时为全部并列读数 ID）",
    )
    lux_span: float | None = Field(
        description="conflict：最高优先级并列读数的照度跨度；legacy 异值时为原始跨度；否则 null",
    )
    candidates: list[TimelineCandidateOut] = Field(
        description="该采样时刻留存的全部原始读数（按来源、读数 ID 排序），用于冲突核查",
    )


class AuthoritativeTimelineOut(BaseModel):
    requested_id: str = Field(description="请求中使用的展品 ID（可能是别名）")
    canonical_id: str = Field(description="规范档案 ID")
    start: str | None = Field(description="查询窗口起点（含）；未指定为 null")
    end: str | None = Field(description="查询窗口终点（含）；未指定为 null")
    current_version: int = Field(description="配置当前最新版本号；从未配置时为 0")
    points: list[AuthoritativeTimelinePointOut] = Field(
        description="按采样时刻升序的逐时刻仲裁结果；冲突时刻也会返回（status=conflict）",
    )


# ---------- 照度采集质量诊断 ----------

class DiagnoseRequest(BaseModel):
    start: datetime = Field(description="诊断窗口起点（含），ISO 8601；缺省时区按 UTC 处理")
    end: datetime = Field(description="诊断窗口终点（含），ISO 8601；缺省时区按 UTC 处理")
    max_rate_lux_per_second: float = Field(
        gt=0,
        description="照度最大允许变化速率（lux/秒）；相邻读数间隔合规但 |Δlux|/Δt 超限记为 jump",
    )
    stuck_tolerance_lux: float = Field(
        ge=0,
        description="卡死容差（lux）：相邻读数照度差不超过该值才可能并入同一段卡死链",
    )
    min_stuck_seconds: float = Field(
        gt=0,
        description="卡死事件最短持续时长（秒）：平稳链首末读数累计时长达到该值才记为 stuck",
    )

    @field_validator("start", "end")
    @classmethod
    def _aware(cls, v: datetime) -> datetime:
        return ensure_aware(v)


class DiagnosisEventOut(BaseModel):
    type: Literal["gap", "jump", "stuck", "timestamp_conflict"]
    start: str = Field(description="事件起点（ISO 8601）；timestamp_conflict 起点与终点相同")
    end: str = Field(description="事件终点（ISO 8601），始终为事件在完整时间线上的原始边界")
    reading_ids: list[str] = Field(description="相关读数 ID：事件边界读数（stuck 为整段全部读数）")
    interval_seconds: float | None = Field(
        default=None,
        description="gap / jump：两条相邻读数的间隔（秒）",
    )
    rate_lux_per_second: float | None = Field(
        default=None,
        description="jump：照度变化速率 |Δlux|/Δt（lux/秒）",
    )
    duration_seconds: float | None = Field(
        default=None,
        description="stuck：首末读数累计持续时长（秒）",
    )
    lux_span: float | None = Field(
        default=None,
        description="stuck：整段照度最大值减最小值；timestamp_conflict：同时刻不同读数的照度跨度",
    )


class DiagnoseResponse(BaseModel):
    requested_id: str = Field(description="请求中使用的展品 ID（可能是别名）")
    canonical_id: str = Field(description="诊断所基于的规范档案 ID")
    start: str
    end: str
    max_interval_seconds: float = Field(description="展品配置的最大采样间隔（秒）：相邻读数间隔超过它记为 gap")
    max_rate_lux_per_second: float
    stuck_tolerance_lux: float
    min_stuck_seconds: float
    events: list[DiagnosisEventOut] = Field(
        description="与查询窗口相交的质量事件，按 (起点, 类型) 稳定排序；"
                    "事件边界始终基于规范档案的完整时间线，不随窗口缩窄而改变",
    )


class DoseResponse(BaseModel):
    requested_id: str = Field(description="请求中使用的展品 ID（可能是别名）")
    canonical_id: str = Field(description="剂量核算所基于的规范档案 ID")
    start: str
    end: str
    dose_limit_luxh: float
    known_dose_luxh: float = Field(description="所查窗口内的已知剂量（lux·h），缺测段未计入")
    remaining_luxh: float = Field(description="截至窗口末的全历史剩余额度（lux·h），越线时为负")
    exceeded: bool = Field(description="截至窗口末，全历史累计剂量是否已越线")
    first_exceeded_at: str | None = Field(description="全历史首次越线时刻（ISO 8601），不随查询窗口变化；截至窗口末未越线为 null")
    unknown_seconds: float = Field(description="窗口内未被有效积分段覆盖的时长（秒），不插值")
    covered_seconds: float = Field(description="窗口内被有效积分段覆盖的时长（秒）")
    reading_count: int = Field(
        description="窗口内（含端点）进入权威时间线的计数：仲裁权威点每时刻计 1（同值并列"
                    "读数合并）、仲裁冲突时刻不计（保持冲突、不入时间线）；无生效优先级版本"
                    "的 legacy 时刻按旧口径计原始读数条数",
    )
    closed_seconds: float | None = Field(
        default=None,
        description="仅 calendar=display 时返回：窗口内处于闭馆时段的时长（秒），"
                    "与 covered_seconds、unknown_seconds 之和等于窗口时长",
    )


# ---------- 重复档案归并 ----------

class MergePreviewRequest(BaseModel):
    source_id: str = Field(min_length=1, max_length=200, description="被归并的来源档案 ID")
    target_id: str = Field(min_length=1, max_length=200, description="保留的目标档案 ID（名称与剂量配置以它为准）")


class MergeMetaDiff(BaseModel):
    field: str
    source: object = None
    target: object = None


class MergeCollision(BaseModel):
    reading_id: str
    source_ts: str
    source_lux: float
    target_ts: str
    target_lux: float
    source_source_id: str | None = Field(
        default=None,
        description="来源侧读数上报来源 source_id（未标注为 null）",
    )
    target_source_id: str | None = Field(
        default=None,
        description="目标侧读数上报来源 source_id（未标注为 null）",
    )
    kind: Literal["duplicate", "conflict"] = Field(
        description="duplicate：时间、照度（及上报来源）一致，提交时自动去重；"
                    "conflict：内容不同（ts/lux/source_id 任一不同），必须逐条裁决",
    )


class MergePriorityCollision(BaseModel):
    """两边来源优先级配置在某一生效时刻对同一来源给出不同优先级。"""

    source_id: str = Field(description="发生分歧的读数来源 ID")
    effective_at: str = Field(description="分歧生效时刻（UTC，ISO 8601，Z 结尾，含端点）")
    source_priority: int | None = Field(
        description="来源档案该时刻起对此来源的优先级；该来源仅目标配置存在或来源侧已结束条目时为 null",
    )
    target_priority: int | None = Field(
        description="目标档案该时刻起对此来源的优先级；该来源仅来源配置存在时为 null",
    )


class MergePreviewOut(BaseModel):
    token: str = Field(description="版本令牌：绑定预演时刻两边元数据与读数集合，提交时校验")
    requested_source_id: str
    requested_target_id: str
    source_id: str = Field(description="解析后的规范来源 ID")
    target_id: str = Field(description="解析后的规范目标 ID")
    metadata: dict[str, MergeMetaDiff] = Field(description="名称与剂量配置差异，逐字段对比")
    source_reading_count: int
    target_reading_count: int
    collisions: list[MergeCollision] = Field(description="两边同读数 ID 的碰撞明细")
    priority_collisions: list[MergePriorityCollision] = Field(
        default_factory=list,
        description="两边来源优先级配置合并时，同一生效时刻对同一来源给出不同优先级的分歧清单；"
                    "必须逐条裁决（keep_target/keep_source），冲突不得静默覆盖",
    )
    auto_duplicate_count: int = Field(description="时间、照度（及来源）一致、提交时自动去重的条数")
    conflict_count: int = Field(description="内容不同、必须逐条裁决的读数碰撞条数")
    source_only_count: int = Field(description="仅来源存在、将随档案搬迁的读数条数")
    ready_to_commit: bool = Field(description="是否不存在待裁决冲突（读数碰撞与优先级分歧均解决才为 true）")


class MergeResolution(BaseModel):
    reading_id: str = Field(min_length=1, max_length=200)
    action: Literal["keep_target", "keep_source", "rename_source"] = Field(
        description="keep_target：保留目标读数；keep_source：用来源内容覆盖目标；"
                    "rename_source：为来源读数指定新 ID 后保留",
    )
    new_id: str | None = Field(default=None, min_length=1, max_length=200,
                               description="action=rename_source 时必填，来源读数搬迁到目标档案后使用的新 ID")

    @model_validator(mode="after")
    def _require_new_id(self) -> MergeResolution:
        if self.action == "rename_source" and not self.new_id:
            raise ValueError("action=rename_source 时必须提供 new_id")
        return self


class MergeCommitRequest(BaseModel):
    token: str = Field(min_length=1, max_length=200)
    resolutions: list[MergeResolution] = Field(default_factory=list, description="读数碰撞逐条裁决")
    priority_resolutions: list[MergePriorityResolution] = Field(
        default_factory=list,
        description="来源优先级配置分歧逐条裁决：keep_source=从生效时刻起采用来源侧配置，"
                    "keep_target=保留目标侧配置；冲突不得静默覆盖",
    )


class MergePriorityResolution(BaseModel):
    source_id: str = Field(min_length=1, max_length=200)
    effective_at: datetime = Field(description="分歧生效时刻（ISO 8601；缺省时区按 UTC）")
    action: Literal["keep_target", "keep_source"] = Field(
        description="keep_target：该生效时刻起该来源沿用目标优先级；keep_source：采用来源侧优先级",
    )

    @field_validator("effective_at")
    @classmethod
    def _aware(cls, v: datetime) -> datetime:
        return ensure_aware(v)


class MergeResolutionResult(BaseModel):
    reading_id: str
    action: str
    new_id: str | None = None


class MergePriorityResolutionResult(BaseModel):
    source_id: str
    effective_at: str
    action: str
    priority: int | None = Field(
        description="裁决后该生效时刻起该来源在目标档案配置中的优先级；为 null 表示该侧无条目（条目结束）",
    )


class MergeCommitOut(BaseModel):
    token: str
    source_id: str = Field(description="被归并的来源规范 ID（归并后成为 target_id 的永久别名）")
    target_id: str = Field(description="归并后保留的规范档案 ID")
    name: str = Field(description="目标档案名称（保持不变）")
    dose_limit_luxh: float = Field(description="目标档案剂量上限（保持不变）")
    max_interval_seconds: float = Field(description="目标档案最大采样间隔（保持不变）")
    moved_count: int = Field(description="仅来源存在、直接搬迁的读数条数")
    auto_deduplicated_count: int = Field(description="同 ID 同内容自动去重（丢弃来源副本）的条数")
    resolutions_applied: list[MergeResolutionResult]
    priority_resolutions_applied: list[MergePriorityResolutionResult] = Field(
        default_factory=list,
        description="来源优先级配置分歧的裁决结果（按生效时刻、来源排序）",
    )
    final_reading_count: int = Field(description="归并后目标档案读数总数")
    aliases: list[str] = Field(description="目标档案当前的全部别名（含本次来源 ID）")
    receipt_chains: list[str] = Field(
        default_factory=list,
        description="归并后原样保留的存证收据链标识（来源与目标的既有链），"
                    "可用 GET /receipts/chains/{chain_id}/verify 分别校验",
    )
    already_committed: bool = Field(description="true 表示本次为同令牌重放，返回的是首次提交结果")


# ---------- 展陈开放日历 ----------

_TIME_RE = re.compile(r"^(\d{2}):(\d{2})(?::(\d{2}))?$")


def parse_local_time(value: str) -> int:
    """把 "HH:MM" 或 "HH:MM:SS" 解析为本地当日秒偏移 [0, 86400)。"""
    m = _TIME_RE.match(value)
    if m is None:
        raise ValueError(f"时间格式须为 HH:MM 或 HH:MM:SS: {value!r}")
    hh, mm, ss = int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)
    if hh > 23 or mm > 59 or ss > 59:
        raise ValueError(f"非法本地时刻: {value!r}")
    return hh * 3600 + mm * 60 + ss


def format_local_time(sec: int) -> str:
    """秒偏移格式化为 HH:MM（整分钟）或 HH:MM:SS。"""
    hh, rem = divmod(sec, 3600)
    mm, ss = divmod(rem, 60)
    return f"{hh:02d}:{mm:02d}" if ss == 0 else f"{hh:02d}:{mm:02d}:{ss:02d}"


class CalendarWindow(BaseModel):
    """一个开放时段；end 不晚于 start 表示跨午夜到次日同一本地时刻。"""

    start: str = Field(description="开始本地时刻，HH:MM 或 HH:MM:SS", examples=["09:00"])
    end: str = Field(description="结束本地时刻，HH:MM 或 HH:MM:SS；不晚于 start 表示跨午夜",
                     examples=["17:00"])

    @field_validator("start", "end")
    @classmethod
    def _valid_time(cls, v: str) -> str:
        parse_local_time(v)
        return v


class CalendarWeeklyWindow(CalendarWindow):
    weekday: int = Field(ge=1, le=7, description="本地星期几，ISO 8601：1=周一 … 7=周日")


class CalendarExceptionDay(BaseModel):
    """例外日：整体替换当日每周规则；windows 为空表示当日闭馆。"""

    date: dt_date = Field(description="本地日期，YYYY-MM-DD")
    windows: list[CalendarWindow] = Field(default_factory=list,
                                          description="当日开放时段；空列表表示当日闭馆")


class CalendarConfigIn(BaseModel):
    """整体设置展陈开放日历（PUT 语义：全量替换）。"""

    timezone: str = Field(min_length=1, max_length=100,
                          description="IANA 时区名，如 Asia/Shanghai、America/New_York")
    weekly: list[CalendarWeeklyWindow] = Field(default_factory=list,
                                               description="每周开放时段，可多个；重叠区间在展开时合并")
    exceptions: list[CalendarExceptionDay] = Field(
        default_factory=list,
        description="按本地日期替换当周规则的例外；同一日期只能出现一次",
    )

    @field_validator("timezone")
    @classmethod
    def _valid_timezone(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except Exception:
            raise ValueError(f"未知 IANA 时区: {v!r}")
        return v

    @model_validator(mode="after")
    def _unique_exception_dates(self) -> CalendarConfigIn:
        dates = [e.date for e in self.exceptions]
        if len(set(dates)) != len(dates):
            raise ValueError("例外日期重复：同一本地日期只能配置一次")
        return self


class CalendarWindowOut(BaseModel):
    start: str
    end: str


class CalendarWeeklyWindowOut(CalendarWindowOut):
    weekday: int


class CalendarExceptionDayOut(BaseModel):
    date: str
    windows: list[CalendarWindowOut]


class CalendarOut(BaseModel):
    requested_id: str = Field(description="请求中使用的展品 ID（可能是别名）")
    canonical_id: str = Field(description="日历所属的规范档案 ID")
    timezone: str
    weekly: list[CalendarWeeklyWindowOut]
    exceptions: list[CalendarExceptionDayOut]
    updated_at: str


# ---------- SQLite 在线快照 ----------

class SnapshotOut(BaseModel):
    """一份已发布的在线快照清单。"""

    seq: int = Field(description="快照编号，从 1 连续递增（仅对已发布快照）")
    created_at: str = Field(description="快照创建时间（UTC，ISO 8601，Z 结尾）")
    sha256: str = Field(description="快照数据库文件的 SHA-256（十六进制）")
    pages: int = Field(description="快照数据库页数（PRAGMA page_count）")
    bytes: int = Field(description="快照数据库文件字节数")
    download_url: str = Field(description="快照文件下载地址")


class SnapshotListOut(BaseModel):
    snapshots: list[SnapshotOut] = Field(
        description="按编号升序的已发布快照；失败或未完成的文件不会出现",
    )


class SnapshotCheckOut(BaseModel):
    """单项校验结果。"""

    name: Literal["sha256", "bytes", "page_count", "integrity_check", "foreign_key_check"]
    ok: bool = Field(description="该项是否通过")
    detail: dict[str, object] = Field(
        default_factory=dict,
        description="该项的明细：哈希/页数/字节数给出 expected 与 actual；"
                    "integrity_check 给出 result；foreign_key_check 给出 violations",
    )


class SnapshotVerifyOut(BaseModel):
    seq: int
    ok: bool = Field(description="所有校验项是否全部通过")
    snapshot: SnapshotOut
    checks: list[SnapshotCheckOut] = Field(
        description="逐项校验结果：sha256 / bytes / page_count / "
                    "integrity_check / foreign_key_check",
    )


# ---------- RFID 馆藏盘点对账 ----------

class InventoryExpectedAreaIn(BaseModel):
    """展品在本轮盘点中应当出现的区域。"""

    exhibit_id: str = Field(min_length=1, max_length=200,
                            description="展品 ID 或归并别名；提交时解析为规范档案 ID")
    area: str = Field(min_length=1, max_length=100, description="预期区域标识（如 A 区展柜）")


class InventoryReaderEventIn(BaseModel):
    """读头事件：某展品（RFID 标签）在某区域被读到一次及信号强度 RSSI。"""

    exhibit_id: str = Field(min_length=1, max_length=200,
                            description="被读到的展品 ID 或归并别名；提交时解析为规范档案 ID")
    area: str = Field(min_length=1, max_length=100, description="读头所属区域标识")
    ts: datetime = Field(description="读头事件时刻，ISO 8601；缺省时区按 UTC 处理。"
                                     "早于盘点开始时刻的事件不参与定位")
    rssi: float = Field(description="信号强度（dBm，数值越大越强）")

    @field_validator("ts")
    @classmethod
    def _aware(cls, v: datetime) -> datetime:
        return ensure_aware(v)


class ReconcileRequest(BaseModel):
    start_at: datetime = Field(description="盘点开始时刻，ISO 8601；缺省时区按 UTC 处理。"
                                           "固定窗口从该时刻起划分")
    window_seconds: float = Field(gt=0, description="固定时间窗长度（秒）；窗口下标 "
                                                    "floor((ts - start_at)/window_seconds)")
    rssi_delta_threshold: float = Field(
        ge=0,
        description="信号差阈值：最后一个有事件窗口内出现多个区域时，最强与次强 RSSI 之差 "
                    "不足该值（严格小于）记为位置不明；恰好等于仍可定位",
    )
    expected: list[InventoryExpectedAreaIn] = Field(
        default_factory=list, max_length=10000,
        description="展品预期区域清单；别名解析后同一规范档案出现多次 → 422",
    )
    events: list[InventoryReaderEventIn] = Field(
        default_factory=list, max_length=10000, description="读头事件列表（顺序不影响结果）",
    )

    @field_validator("start_at")
    @classmethod
    def _start_aware(cls, v: datetime) -> datetime:
        return ensure_aware(v)


class InventoryExpectationOut(BaseModel):
    requested_id: str = Field(description="请求中使用的展品 ID（可能是别名）")
    canonical_id: str = Field(description="解析后的规范档案 ID")
    expected_area: str


class InventoryMissingOut(BaseModel):
    canonical_id: str
    expected_area: str = Field(description="应当出现的区域；盘点全程无有效读头事件")


class InventoryMisplacedOut(BaseModel):
    canonical_id: str
    expected_area: str
    actual_area: str = Field(description="按最后一个有事件窗口判定出的实际区域")


class InventoryUnexpectedOut(BaseModel):
    canonical_id: str
    actual_area: str = Field(description="不在预期清单中但被读到，且位置可判定的区域")


class InventoryUnknownPositionOut(BaseModel):
    canonical_id: str
    expected_area: str | None = Field(
        description="预期区域；该展品不在预期清单中时为 null",
    )


class InventoryRoundOut(BaseModel):
    """一轮盘点对账的固化结果（GET 取回的形态）。"""

    round_id: str
    request_digest: str = Field(description="规范化请求摘要（SHA-256）：轮次与该摘要一一绑定")
    start_at: str = Field(description="规范化为 UTC、'Z' 结尾的 ISO 8601")
    window_seconds: float
    rssi_delta_threshold: float
    expected_count: int
    observed_count: int = Field(description="在盘点时间范围内出现过有效事件的规范档案数")
    event_count: int = Field(description="请求中的读头事件总数")
    events_used_count: int = Field(description="不早于开始时刻、参与定位的事件数")
    events_before_start_count: int = Field(description="早于盘点开始时刻、被忽略的事件数")
    expectations: list[InventoryExpectationOut] = Field(
        description="解析后的预期区域清单（按请求顺序）",
    )
    missing: list[InventoryMissingOut] = Field(description="缺失：预期但全程无有效事件，按规范 ID 排序")
    misplaced: list[InventoryMisplacedOut] = Field(description="错位：判定区域与预期区域不同，按规范 ID 排序")
    unexpected: list[InventoryUnexpectedOut] = Field(
        description="意外出现：不在预期清单中、位置可判定的展品，按规范 ID 排序",
    )
    unknown_position: list[InventoryUnknownPositionOut] = Field(
        description="位置不明：最后窗口多区域且最强/次强信号差不足阈值，按规范 ID 排序；"
                    "不在预期清单中的歧义展品也列入本组、expected_area 为 null",
    )
    reconciled_at: str = Field(description="结果固化时刻（UTC，ISO 8601，Z 结尾）")


class ReconcileResponse(InventoryRoundOut):
    already_committed: bool = Field(
        description="false 表示本次首次固化；true 表示相同规范化请求重放，返回的是首次结果",
    )


# ---------- 展品应急撤离编排 ----------

class EvacuationClosureIn(BaseModel):
    """一条通道的半开封闭时段：通道在 ``[start, end)`` 内不可进入。"""

    model_config = ConfigDict(populate_by_name=True)

    start: int = Field(ge=0, description="封闭开始相对秒（含）")
    end: int = Field(ge=0, description="封闭结束相对秒（不含）；end 秒整点起可进入")

    @model_validator(mode="after")
    def _start_before_end(self) -> EvacuationClosureIn:
        if self.end <= self.start:
            raise ValueError("封闭时段必须满足 start < end（半开 [start, end)）")
        return self


class EvacuationEdgeIn(BaseModel):
    """有向通道：from→to 的单向通行；通行秒数与并发容量均为正整数。"""

    model_config = ConfigDict(populate_by_name=True)

    from_node: str = Field(
        alias="from", validation_alias="from", serialization_alias="from",
        min_length=1, max_length=200, description="通道起点节点",
    )
    to_node: str = Field(
        alias="to", validation_alias="to", serialization_alias="to",
        min_length=1, max_length=200, description="通道终点节点",
    )
    travel_seconds: int = Field(
        validation_alias="travel_seconds", serialization_alias="travel_seconds",
        ge=1, description="通过该通道需要的整数秒数（进入 + 该秒数 = 离开）",
    )
    capacity: int = Field(
        ge=1, description="通道并发容量：同一时刻最多容纳的展品数；按 [进入, 离开) 计数",
    )
    closures: list[EvacuationClosureIn] = Field(
        default_factory=list,
        description="半开封闭时段列表（相对计划起点的整数秒区间 [start, end)）；"
                    "重叠或相接自动合并",
    )


class EvacuationExhibitIn(BaseModel):
    """待编排展品：ID 先经别名解析为规范档案；优先级越大越早编排。"""

    exhibit_id: str = Field(
        min_length=1, max_length=200,
        description="展品 ID 或归并别名；提交时解析为规范档案 ID",
    )
    start_node: str = Field(min_length=1, max_length=200, description="该展品所在起点节点")
    priority: int = Field(
        description="优先级（整数，越大越先编排）：按优先级降序、规范 ID 升序逐件编排",
    )


class EvacuationRequest(BaseModel):
    """展品应急撤离编排请求：一次给出计划图与全部待撤离展品。"""

    start_at: datetime = Field(
        description="计划起点（绝对时刻，ISO 8601）；缺省时区按 UTC 处理。"
                    "图中的封闭时段均为相对该时刻的秒偏移",
    )
    nodes: list[Annotated[str, Field(min_length=1, max_length=200)]] = Field(
        min_length=1, max_length=10000, description="全部节点标识（起点、通道端点与出口）",
    )
    edges: list[EvacuationEdgeIn] = Field(
        default_factory=list, max_length=100000, description="有向通道列表",
    )
    exits: list[Annotated[str, Field(min_length=1, max_length=200)]] = Field(
        min_length=1, max_length=10000, description="出口节点列表（必须全部存在于 nodes）",
    )
    exhibits: list[EvacuationExhibitIn] = Field(
        min_length=1, max_length=10000, description="待撤离展品列表",
    )

    @field_validator("start_at")
    @classmethod
    def _aware(cls, v: datetime) -> datetime:
        return ensure_aware(v)

    @model_validator(mode="after")
    def _validate_graph(self) -> EvacuationRequest:
        # 节点不可重复
        seen_nodes: set[str] = set()
        dup_nodes: set[str] = set()
        for n in self.nodes:
            if n in seen_nodes:
                dup_nodes.add(n)
            seen_nodes.add(n)
        if dup_nodes:
            raise ValueError(f"节点重复: {sorted(dup_nodes)}")
        node_set = seen_nodes
        # 出口必须是已知节点且不可重复
        if len(self.exits) != len(set(self.exits)):
            raise ValueError("出口节点重复")
        unknown_exits = sorted(set(self.exits) - node_set)
        if unknown_exits:
            raise ValueError(f"出口不在节点列表中: {unknown_exits}")
        # 通道端点必须已知；同一有向通道（from, to）只能定义一次
        seen_edges: set[tuple[str, str]] = set()
        for e in self.edges:
            if e.from_node not in node_set:
                raise ValueError(f"通道起点不在节点列表中: {e.from_node}")
            if e.to_node not in node_set:
                raise ValueError(f"通道终点不在节点列表中: {e.to_node}")
            if (e.from_node, e.to_node) in seen_edges:
                raise ValueError(f"有向通道重复定义: {e.from_node} -> {e.to_node}")
            seen_edges.add((e.from_node, e.to_node))
        # 展品起点必须已知
        for x in self.exhibits:
            if x.start_node not in node_set:
                raise ValueError(f"展品 {x.exhibit_id} 的起点不在节点列表中: {x.start_node}")
        return self


class EvacuationLegOut(BaseModel):
    """一段通道的编排结果：``enter_at`` 进入、``leave_at`` 离开（=进入+通行秒数）。"""

    model_config = ConfigDict(populate_by_name=True)

    from_node: str = Field(alias="from", serialization_alias="from")
    to_node: str = Field(alias="to", serialization_alias="to")
    travel_seconds: int
    enter_offset_seconds: int = Field(description="进入时刻相对计划起点的整数秒")
    leave_offset_seconds: int = Field(description="离开时刻相对计划起点的整数秒（半开占用右端点）")
    enter_at: str = Field(description="进入时刻（UTC，ISO 8601，Z 结尾）")
    leave_at: str = Field(description="离开时刻（UTC，ISO 8601，Z 结尾）；此时刻起通道释放一份容量")
    wait_seconds: int = Field(
        description="进入该通道前的等待秒数（到达通道起点的最早时刻到实际进入之间）",
    )


class EvacuationExhibitOut(BaseModel):
    """单件展品的撤离编排结果。"""

    requested_id: str = Field(description="请求中使用的展品 ID（可能是别名）")
    canonical_id: str = Field(description="别名解析后的规范档案 ID")
    start_node: str
    priority: int
    exit_node: str = Field(description="时变最早到达搜索选出的出口")
    path: list[str] = Field(description="完整节点路径（含起点与出口）")
    legs: list[EvacuationLegOut] = Field(description="逐段通道编排（与 path 相邻节点对应）")
    wait_seconds: int = Field(description="全路径累计等待秒数")
    completion_offset_seconds: int = Field(
        description="总完成时间：到达出口时刻相对计划起点的整数秒",
    )
    completed_at: str = Field(description="到达出口时刻（UTC，ISO 8601，Z 结尾）")


class EvacuationResponse(BaseModel):
    """应急撤离编排响应。

    编排为纯只读计算：不写照度读数、不写日历、不写盘点轮次，也不生成或改写
    存证收据链。
    """

    start_at: str = Field(description="计划起点（规范化为 UTC，ISO 8601，Z 结尾）")
    results: list[EvacuationExhibitOut] = Field(
        description="逐件编排结果，按优先级降序、规范 ID 升序排列",
    )
    total_completion_offset_seconds: int = Field(
        description="全部展品撤离完成的总完成时间（最晚一件到达出口的相对秒）",
    )
