"""采集告警业务规则：状态流转、字段校验与筛选口径都收在这里。

规则的阈值、比较运算符、持续周期等口径调整会让「规则版本」递增，告警引擎
后续采集按新口径判断；已经结案的诊断报告保留生成时的级别与阈值版本，不回写。
"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "alertrule"
REQUIRED_FIELDS = ["告警编号", "监控指标", "比较运算符"]
# 参与判定口径、调整后需要升版本的字段
VERSION_FIELDS = ["比较运算符", "阈值数值", "持续周期", "稳定周期", "告警级别", "静默时段"]
RULE_FIELDS = [
    "告警编号", "监控指标", "比较运算符", "阈值数值", "持续周期",
    "稳定周期", "告警级别", "通知渠道", "静默时段",
]
STATUS_ORDER = ["草稿", "规则评审中", "已生效", "已关闭"]
ACTION_RULES = {"提交评审": "规则评审中", "生效规则": "已生效", "关闭规则": "已关闭"}
NEGATIVE_ACTIONS = []


class AlertruleService:
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
            rows = [row for row in rows if keyword in str(row.get("告警编号", ""))]
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
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        for field in RULE_FIELDS:
            if field in values:
                entry[field] = values.get(field)
        entry.setdefault("稳定周期", 2)
        entry["规则版本"] = "v1"
        entry["版本历史"] = [{"版本": "v1", "口径": self._snapshot(entry)}]
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def update_entry(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """调整规则口径。判定口径字段有实际变化时升版本，并通知引擎按新口径计数。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"告警规则 {entry_id} 不存在或已归档"
        changed = [
            field for field in VERSION_FIELDS
            if field in values and values.get(field) != entry.get(field)
        ]
        if not changed:
            for field in RULE_FIELDS:
                if field in values:
                    entry[field] = values.get(field)
            return entry, "规则内容无实质变化，版本保持不变"

        current = str(entry.get("规则版本") or "v1")
        history = entry.setdefault("版本历史", [])
        if not history:
            # 种子数据或历史数据可能没有版本历史，先补变更前的当前版本基线
            history.append({"版本": current, "口径": self._snapshot(entry)})
        for field in RULE_FIELDS:
            if field in values:
                entry[field] = values.get(field)
        version_no = int(current[1:]) if current.startswith("v") and current[1:].isdigit() else 1
        entry["规则版本"] = f"v{version_no + 1}"
        history.append(
            {
                "版本": entry["规则版本"],
                "变更字段": changed,
                "口径": self._snapshot(entry),
            }
        )
        # 延迟导入避免与告警引擎形成循环依赖
        from app.services.alertengine import alert_engine

        alert_engine.on_rule_updated(entry_id)
        return entry, f"规则口径已调整（{'、'.join(changed)}），版本升级为 {entry['规则版本']}，后续采集按新口径判断"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"告警规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集告警可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"告警规则已{action}"

    @staticmethod
    def _snapshot(entry: dict[str, Any]) -> dict[str, Any]:
        return {field: entry.get(field) for field in VERSION_FIELDS}
