"""采集上报与告警判定接口。

- POST /api/alertengine/ingest      单条指标上报
- POST /api/alertengine/ingest-batch 采集批次上报（同批次幂等，并发只记一次）
- GET  /api/alertengine/runtime      各规则运行态（最后越阈时间、原始指标、合并次数等）
- GET  /api/alertengine/episodes     告警实例与状态轨迹
- GET  /api/alertengine/notifications 值班渠道通知记录
- GET  /api/alertengine/verify       告警与诊断相互印证校验
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.alertengine import alert_engine

router = APIRouter(prefix="/api/alertengine", tags=["采集告警判定"])


class IngestPayload(BaseModel):
    rule: Any = Field(description="规则主键 id 或告警编号，如 ALERT-06")
    value: Any = Field(description="本次采集到的指标值")
    ts: Any = Field(description="采集时间：ISO8601、YYYY-MM-DD HH:MM 或时间戳")
    batch_id: str | None = None


class BatchPayload(BaseModel):
    batch_id: str
    samples: list[dict[str, Any]]


@router.post("/ingest")
def ingest(payload: IngestPayload) -> dict[str, Any]:
    """单条采集样本上报，驱动静默、持续周期、稳定周期的完整判定。"""
    return alert_engine.ingest(payload.rule, payload.value, payload.ts, payload.batch_id)


@router.post("/ingest-batch")
def ingest_batch(payload: BatchPayload) -> dict[str, Any]:
    """采集批次上报；同一批次重复/并发上报只处理一次。"""
    return alert_engine.ingest_batch(payload.batch_id, payload.samples)


@router.get("/runtime")
def runtime() -> dict[str, Any]:
    rows = alert_engine.list_runtime()
    return {"total": len(rows), "items": rows}


@router.get("/episodes")
def episodes(rule: str | None = None) -> dict[str, Any]:
    rows = alert_engine.list_episodes(rule)
    return {"total": len(rows), "items": rows}


@router.get("/notifications")
def notifications(rule: str | None = None) -> dict[str, Any]:
    rows = alert_engine.list_notifications(rule)
    return {"total": len(rows), "items": rows}


@router.get("/verify")
def verify() -> dict[str, Any]:
    """告警实例、诊断报告、规则运行态三方相互印证。"""
    return alert_engine.verify()
