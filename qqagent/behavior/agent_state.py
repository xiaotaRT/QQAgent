#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Agent State（统一 Agent 状态）（从 src_p_agent_state.py 迁移）。"""
import os
import re
import json
import time
import random
import threading
import logging
import hashlib
import copy
import math
import queue
import signal
import sys
import shutil
import socket
import struct
import urllib.request
import websocket
import requests
from datetime import datetime, timedelta
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed

from qqagent.core import CONFIG, logger, dm
from qqagent.core import context
from qqagent.core.utils import _FakeLock, _now_str, is_owner

# ============================================================
# V23-A Agent State：统一里克状态
# TODO第14块：统一 Agent State —— 当前任务 / 关注目标 / 环境 / 行为状态 / 近期事件
# 让 人格层 / 主动行为层 / 任务层 / 自检层 共享同一份状态，
# 避免各模块各自维护互相割裂的“里克状态”。
#
# 约定（写 Owner、读共享）：
#   - TaskManager    写 current_tasks / agent_status(tasking)   （V23-B 任务系统）
#   - GoalManager    写 active_goal / goals                      （V23-C 长期目标，后续）
#   - EnvironmentHub / WorldState 写 environment 摘要
#   - 行为层          写 last_action / recent_events
#   人格层与主动行为层只读。
# ============================================================
import time as _time
import threading as _threading


class AgentState:
    """统一 Agent State。

    所有模块读写同一个 context.agent_state 实例。带锁，线程安全；
    snapshot() 可供人格层/自检层/日志读取。
    """
    STATUS_IDLE = "idle"
    STATUS_OBSERVING = "observing"
    STATUS_ACTING = "acting"
    STATUS_TASKING = "tasking"     # 任务系统执行中（抑制同目标主动行为）
    STATUS_PAUSED = "paused"
    STATUS_ASKING = "asking"       # 正在请求小塔介入

    def __init__(self):
        self._lock = _threading.RLock()
        self.current_tasks = []        # 活动任务 id（TaskManager 维护）
        self.active_goal = None        # 当前关注长期目标 id（GoalManager 维护）
        self.goals = []                # 长期目标列表 [{id,name,progress,updated_at}]
        self.environment = {}          # 环境摘要（WorldState 快照）
        self.agent_status = self.STATUS_IDLE
        self.recent_events = []        # 近期事件，最多 20 条 [(ts, type, detail)]
        self.last_action = None        # 最近一次行动摘要
        self.last_action_ts = 0.0
        self.last_ask_ts = 0.0         # 最近一次向小塔请求介入时间
        self.started_at = _time.time()
        self._persist = None

    # ---- 状态读写（带锁） ----
    def snapshot(self):
        with self._lock:
            return {
                "current_tasks": list(self.current_tasks),
                "active_goal": self.active_goal,
                "goals": list(self.goals),
                "environment": dict(self.environment),
                "agent_status": self.agent_status,
                "recent_events": list(self.recent_events),
                "last_action": self.last_action,
                "last_action_ts": self.last_action_ts,
                "last_ask_ts": self.last_ask_ts,
                "started_at": self.started_at,
            }

    def set_status(self, status):
        with self._lock:
            self.agent_status = status

    def set_environment(self, **kw):
        with self._lock:
            self.environment.update(kw)
            if len(self.environment) > 64:
                # 只保留最新键（防止无限膨胀）
                keys = list(self.environment.keys())
                for k in keys[: len(keys) - 64]:
                    self.environment.pop(k, None)

    def record_event(self, event_type, detail=None, ts=None):
        with self._lock:
            self.recent_events.append((ts or _time.time(), event_type, detail))
            self.recent_events = self.recent_events[-20:]

    def record_action(self, action_summary, ts=None):
        with self._lock:
            self.last_action = action_summary
            self.last_action_ts = ts or _time.time()

    # ---- 任务系统接口 ----
    def begin_task(self, task_id):
        with self._lock:
            if task_id not in self.current_tasks:
                self.current_tasks.append(task_id)
            self.agent_status = self.STATUS_TASKING

    def end_task(self, task_id):
        with self._lock:
            if task_id in self.current_tasks:
                self.current_tasks.remove(task_id)
            if not self.current_tasks:
                self.agent_status = self.STATUS_IDLE

    def is_tasking(self):
        with self._lock:
            return self.agent_status == self.STATUS_TASKING and bool(self.current_tasks)

    def mark_asked(self, ts=None):
        with self._lock:
            self.last_ask_ts = ts or _time.time()
            self.agent_status = self.STATUS_ASKING

    # ---- 持久化（快照落盘，供崩溃恢复 / 自检读取） ----
    def attach_persistence(self, dm_ref, key):
        with self._lock:
            self._persist = (dm_ref, key)

    def save(self):
        try:
            if self._persist is not None:
                self._persist[0].save(self._persist[1], self.snapshot())
        except Exception:
            pass


# 全局实例：单文件拼接后即全局可访问；运行时由 main 的 boot_sequence
# 调用 _init_agent_state() 完成持久化挂接与崩溃恢复。
# 统一对外变量名为 context.agent_state（各模块通过 getattr(context, "context.agent_state", None) 读取）。
_agent_state = AgentState()
context.agent_state = _agent_state


def _init_agent_state():
    """boot_sequence 调用：挂持久化并恢复上次快照。"""
    global _agent_state
    try:
        _agent_state.attach_persistence(dm, "agent_state_data")
        snap = dm.load("agent_state_data", {})
        if snap:
            _agent_state.goals = list(snap.get("goals", []))
            _agent_state.active_goal = snap.get("active_goal")
            _agent_state.environment = dict(snap.get("environment", {}))
            # 崩溃恢复：遗留任务 id 交由 TaskManager 校验/清理，状态回到 idle
            _agent_state.current_tasks = []
            _agent_state.agent_status = AgentState.STATUS_IDLE
            _agent_state.recent_events = list(snap.get("recent_events", []))
    except Exception:
        pass
    return _agent_state

