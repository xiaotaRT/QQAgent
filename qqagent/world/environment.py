#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""环境感知中枢（从 src_m_reality.py 迁移）。"""
import time
from qqagent.core import CONFIG, logger, dm
from qqagent.core import context
from qqagent.core.utils import _now_str

class EnvironmentHub:
    """环境感知中枢：汇总设备/事件/状态，供人格层与 Agent 使用"""

    def __init__(self, dmgr):
        self._dm = dmgr
        self._notes = []
        self._dirty = False

    def note_event(self, text):
        self._notes.append({"ts": _now_str(), "text": text})
        self._notes = self._notes[-50:]
        self._dirty = True

    def build_awareness_summary(self):
        online = [d.name for d in self._dm.all_devices() if d.online and d.enabled]
        offline = [d.name for d in self._dm.all_devices() if not d.online or not d.enabled]
        lines = ["【环境感知】"]
        lines.append("在线设备：%s" % ("、".join(online) if online else "无"))
        lines.append("离线/禁用：%s" % ("、".join(offline) if offline else "无"))
        if self._notes:
            lines.append("最近事件：%s" % "；".join(n["text"] for n in self._notes[-3:]))
        return "\n".join(lines)

    def flush(self):
        try:
            dm.save("environment_awareness_data", {"notes": self._notes})
            dm.flush_all()
            self._dirty = False
        except Exception as e:
            logger.warning("[V13] 环境感知落盘失败: %s" % e)


# ---------- 感知闭环（十五） ----------

