"""诊断记录业务规则：状态流转、字段校验与筛选口径都收在这里。

除通用 CRUD 外，还提供与告警引擎联动的开案/结案方法：
- 告警升级时新建「分析中」报告，并冻结当时的告警级别与阈值版本；
- 告警恢复关闭时给活动报告写入结案时间和结案结论；
- 已结案/已归档的历史报告不会被再次升级覆盖，再次升级一律新开报告。
"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "diagnose"
REQUIRED_FIELDS = ["报告编号", "关联任务", "异常类型"]
STATUS_ORDER = ["草稿", "分析中", "已结案", "已归档"]
ACTION_RULES = {"开始分析": "分析中", "标记结案": "已结案", "归档报告": "已归档"}
NEGATIVE_ACTIONS = []


class DiagnoseService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("报告编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        entry = self._build_entry({field: values.get(field) for field in REQUIRED_FIELDS})
        store.rows(MODULE).append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"诊断报告 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于诊断记录可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"诊断报告已{action}"

    # ------------------------------------------------------------- 告警联动方法
    def open_report(
        self,
        *,
        rule: dict[str, Any],
        episode: dict[str, Any],
        metric: float,
        moment: Any,
        seq_start: int = 1,
    ) -> dict[str, Any]:
        """告警升级时新建分析中报告。

        每次升级都新开报告：已经结案的历史报告保持原样，级别与阈值版本以
        报告生成时的规则口径冻结，后续规则调整不会回写历史报告。
        """
        snapshot = episode.get("阈值快照") or {}
        version = str(snapshot.get("规则版本") or rule.get("规则版本") or "v1")
        seq = self._next_seq(seq_start)
        entry = self._build_entry(
            {
                "报告编号": f"DIAG-{seq:03d}",
                "关联任务": f"{rule.get('告警编号')} / {episode.get('告警实例编号')}",
                "异常类型": f"{rule.get('监控指标')} 越阈告警",
            }
        )
        entry.update(
            {
                "status": "分析中",
                "pending": True,
                "abnormal": True,
                "告警编号": rule.get("告警编号"),
                "告警实例编号": episode.get("告警实例编号"),
                "异常堆栈": (
                    f"首次越阈 {episode.get('首次越阈时间')}，升级时刻 {moment.isoformat()}，"
                    f"触发指标={metric}，判定口径 {rule.get('比较运算符')}{rule.get('阈值数值')}"
                ),
                "发生时间": moment.isoformat(),
                "影响行数": "待评估",
                "根因分类": "待分析",
                "修复建议": "持续观察采集链路并结合关联任务排查",
                # 冻结生成时的级别与阈值版本，规则后续调整不影响本报告
                "告警级别": snapshot.get("告警级别", rule.get("告警级别")),
                "阈值版本": version,
                "阈值快照": dict(snapshot),
                "触发原始指标": metric,
                "结案时间": None,
                "结案结论": None,
            }
        )
        store.rows(MODULE).append(entry)
        return entry

    def close_active_report(
        self,
        *,
        episode: dict[str, Any],
        moment: Any,
        conclusion: str,
    ) -> dict[str, Any] | None:
        """告警恢复关闭时，给活动报告写结案结论；已结案报告直接跳过、不覆盖。"""
        code = episode.get("当前报告编号")
        target = None
        for row in store.rows(MODULE):
            if row.get("报告编号") == code:
                target = row
                break
        if target is None:
            return None
        if target.get("status") in ("已结案", "已归档"):
            # 历史报告已结案，再次升级/恢复都不得改写它
            return target
        target["status"] = "已结案"
        target["pending"] = False
        target["abnormal"] = False
        target["结案时间"] = moment.isoformat()
        target["结案结论"] = conclusion
        return target

    # ------------------------------------------------------------- 内部辅助
    def _next_seq(self, start: int) -> int:
        seqs = []
        for row in store.rows(MODULE):
            code = str(row.get("报告编号") or "")
            digits = code.split("DIAG-")[-1]
            if digits.isdigit():
                seqs.append(int(digits))
        return max(start - 1, *seqs) + 1 if seqs else start

    @staticmethod
    def _build_entry(seed: dict[str, Any]) -> dict[str, Any]:
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update(seed)
        entry.setdefault("status", STATUS_ORDER[0])
        entry.setdefault("pending", True)
        entry.setdefault("abnormal", False)
        return entry
