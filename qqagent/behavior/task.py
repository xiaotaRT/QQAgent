#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""任务系统（Task/TaskManager）（从 src_q_task.py 迁移）。"""
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
from qqagent.core.commands import register_command

# ============================================================
# V23-B 完整任务系统（TODO优先级1：目标 → 拆分 → 执行 → 验证 → 重试/调整）
# ------------------------------------------------------------
# 定位：ActionEngine 之上的编排层。ActionEngine 负责"单步该不该做"，
#       TaskManager 负责"一件多步的事怎么做完"。
# 原则（与全项目一致）：
#   - 每一步真实行动必须走 ToolGateway（禁止任何旁路）；
#   - 每一步先经过 Gateway 自带安全链：急停→风险→权限→确认→分发；
#   - 高风险/需确认/权限不足 → 自动转 ASK，暂停任务等小塔「任务 继续」；
#   - 执行成功以"验证"为准，不信返回值的 ok 字面；
#   - 防止"动作→事件→动作"无限循环：同目标冷却 + 队列上限 + 任务超时
#     + AgentState.tasking 期间抑制同目标主动行为。
# ============================================================
import time as _t
import json as _json
import threading as _th
from concurrent.futures import ThreadPoolExecutor as _TPE


class Task:
    """一个可执行任务单元。"""
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    def __init__(self, task_id, goal, steps=None, priority=5, source="command",
                 max_attempts=2, deadline=None, cooldown_key=None,
                 fallback_steps=None):
        self.task_id = task_id
        self.goal = goal
        self.steps = steps or []          # [{action,target,params,expected_result,verify,exec_fn,verify_fn}]
        self.fallback_steps = fallback_steps or []
        self.priority = int(priority)
        self.source = source
        self.state = self.WAITING
        self.current_step = 0
        self.attempts = 0                 # 当前步骤已尝试次数
        self.max_attempts = int(max_attempts)
        self.deadline = deadline           # None 或绝对时间戳
        self.created_at = _t.time()
        self.updated_at = self.created_at
        self.result = {}                  # 每步结果 {step_idx: {"ok", "result", "attempts"}}
        self.error = ""
        self.needs_owner = False          # ASK：等待小塔「任务 继续」
        self.owner_reason = ""
        self.cooldown_key = cooldown_key or _norm_key(goal)
        self.fallback_in_use = False

    def touch(self):
        self.updated_at = _t.time()

    def is_expired(self):
        return self.deadline is not None and _t.time() > self.deadline


def _norm_key(text):
    """目标归一化（去空白/小写/截断），用于同目标冷却。"""
    return "".join((text or "").split()).lower()[:40]


class TaskManager:
    """任务队列 + 多步执行 + 验证 + 重试/替代方案 + ASK + 持久化。

    线程模型：单 worker 线程，按优先级依次执行；每步在独立线程池中运行，
    支持步骤超时（超时不会杀死底层线程，但结果被丢弃并记为超时失败）。
    """
    DEFAULT_COOLDOWN = 300      # 同目标冷却（秒）
    DEFAULT_QUEUE_LIMIT = 20    # 等待队列上限
    DEFAULT_STEP_TIMEOUT = 30.0 # 单步超时（秒）

    def __init__(self, cooldown=DEFAULT_COOLDOWN, queue_limit=DEFAULT_QUEUE_LIMIT,
                 step_timeout=DEFAULT_STEP_TIMEOUT):
        self._tasks = {}
        self._lock = _th.RLock()
        self._cooldown = {}               # cooldown_key -> ts
        self._cooldown_seconds = float(cooldown)
        self._queue_limit = int(queue_limit)
        self._step_timeout = float(step_timeout)
        self._seq = 0
        self._stop = _th.Event()
        self._wake = _th.Event()
        self._worker = None
        self._emergency = False
        self._executor = _TPE(max_workers=1, thread_name_prefix="task-step")
        self._history = {}                # 已完成任务快照（落盘）
        self._persist_key = "task_manager_data"

    # ================= 对外接口 =================
    def start(self):
        if self._worker is None or not self._worker.is_alive():
            self._stop.clear()
            self._worker = _th.Thread(target=self._worker_loop,
                                      name="task-worker", daemon=True)
            self._worker.start()

    def shutdown(self):
        try:
            self._stop.set()
            self._wake.set()
            self._executor.shutdown(wait=False)
        except Exception:
            pass

    def set_emergency(self, on):
        """任务级急停（联动全局 Gateway 急停判断）。"""
        with self._lock:
            self._emergency = bool(on)
        self._wake.set()

    def _global_emergency(self):
        try:
            gw = getattr(context, "context.tool_gateway", None)
            if gw is not None and callable(getattr(gw, "is_emergency_stopped", None)) \
                    and gw.is_emergency_stopped():
                return True
        except Exception:
            pass
        return self._emergency

    def submit(self, goal, steps=None, priority=5, source="command",
               max_attempts=None, deadline_seconds=None, fallback_steps=None,
               cooldown_key=None):
        """提交任务。steps 缺省时由 LLM 拆解（失败则转 ASK 小塔）。
        返回 Task；同目标冷却期内重复提交返回 None。"""
        goal = (goal or "").strip()
        if not goal:
            return None
        key = cooldown_key or _norm_key(goal)
        with self._lock:
            if key in self._cooldown and _t.time() - self._cooldown[key] < self._cooldown_seconds:
                return None
            waiting = [t for t in self._tasks.values()
                       if t.state in (Task.WAITING, Task.RUNNING, Task.PAUSED)]
            if len(waiting) >= self._queue_limit:
                return None
            self._seq += 1
            tid = "T%d" % self._seq
            t = Task(tid, goal, steps=steps, priority=priority, source=source,
                     max_attempts=max_attempts if max_attempts is not None else 2,
                     deadline=(_t.time() + deadline_seconds) if deadline_seconds else None,
                     cooldown_key=key, fallback_steps=fallback_steps)
            self._tasks[tid] = t
            self._cooldown[key] = _t.time()
        self._log("任务 %s 已提交：%s（优先级 %s）" % (tid, goal, priority))
        self._wake.set()
        return t

    def pause(self, task_id):
        with self._lock:
            t = self._tasks.get(task_id)
            if t is None:
                return "任务不存在：%s" % task_id
            if t.state == Task.WAITING:
                t.state = Task.PAUSED
            elif t.state == Task.RUNNING:
                t.state = Task.PAUSED  # worker 检查后停止推进
            else:
                return "任务 %s 当前状态 %s，不能暂停。" % (task_id, t.state)
            t.touch()
        return "任务 %s 已暂停。" % task_id

    def resume(self, task_id):
        with self._lock:
            t = self._tasks.get(task_id)
            if t is None:
                return "任务不存在：%s" % task_id
            if t.state != Task.PAUSED:
                return "任务 %s 当前状态 %s，不能恢复。" % (task_id, t.state)
            t.state = Task.WAITING
            t.touch()
        self._wake.set()
        return "任务 %s 已恢复。" % task_id

    def cancel(self, task_id):
        with self._lock:
            t = self._tasks.get(task_id)
            if t is None:
                return "任务不存在：%s" % task_id
            if t.state in (Task.DONE, Task.CANCELLED, Task.FAILED):
                return "任务 %s 已结束（%s），不能取消。" % (task_id, t.state)
            t.state = Task.CANCELLED
            t.error = "cancelled_by_owner"
            t.touch()
        return "任务 %s 已取消。" % task_id

    def approve_continue(self, task_id):
        """「任务 继续」：响应 ASK，放行该任务继续执行。"""
        with self._lock:
            t = self._tasks.get(task_id)
            if t is None:
                return "任务不存在：%s" % task_id
            if not t.needs_owner:
                return "任务 %s 当前不需要小塔介入（needs_owner=False）。" % task_id
            t.needs_owner = False
            t.owner_reason = ""
            t.state = Task.WAITING
            t.touch()
        self._log("任务 %s 已由小塔放行继续执行。" % task_id)
        self._wake.set()
        return "任务 %s 已放行，继续执行。" % task_id

    def retry(self, task_id):
        with self._lock:
            t = self._tasks.get(task_id)
            if t is None:
                return "任务不存在：%s" % task_id
            if t.state not in (Task.FAILED, Task.CANCELLED):
                return "任务 %s 当前状态 %s，可用「任务 重试」的是已失败/已取消的任务。" % (task_id, t.state)
            t.state = Task.WAITING
            t.attempts = 0
            t.current_step = 0
            t.error = ""
            t.result = {}
            t.needs_owner = False
            t.touch()
        self._wake.set()
        return "任务 %s 已重试。" % task_id

    def get(self, task_id):
        return self._tasks.get(task_id)

    def all_tasks(self):
        with self._lock:
            return sorted(self._tasks.values(),
                          key=lambda t: (-t.priority, t.created_at))

    def describe(self, t):
        steps_txt = "；".join("%d.%s" % (i + 1, s.get("action", "?"))
                              for i, s in enumerate(t.steps or []))
        return ("目标：%s\n状态：%s　优先级：%s\n步骤：%s\n%s"
                % (t.goal, t.state, t.priority, steps_txt or "（待LLM拆解）",
                   ("等待小塔介入：%s" % t.owner_reason) if t.needs_owner else ""))

    def list_tasks(self):
        ts = self.all_tasks()
        if not ts:
            return "当前没有任务。\n用法：任务 开始 <目标>；任务 列表"
        lines = ["当前任务（%d 个）：" % len(ts)]
        for t in ts:
            mark = "⚠需要小塔" if t.needs_owner else ""
            lines.append("%s [%s] %s（%s）%s" % (
                t.task_id, t.state, t.goal[:24], t.priority, mark))
        lines.append("操作：任务 暂停|恢复|取消|继续|重试 <id>；任务 开始 <目标>")
        return "\n".join(lines)

    # ================= 内部：worker =================
    def _worker_loop(self):
        while not self._stop.is_set():
            t = self._pick_next()
            if t is None:
                self._wake.wait(timeout=1.0)
                self._wake.clear()
                continue
            if self._global_emergency():
                with self._lock:
                    if t.state == Task.RUNNING:
                        t.state = Task.PAUSED
                        t.error = "emergency_stop"
                self._wake.wait(timeout=0.5)
                continue
            if t.state == Task.WAITING:
                self._run_task(t)

    def _pick_next(self):
        with self._lock:
            cand = [t for t in self._tasks.values() if t.state == Task.WAITING]
            if not cand:
                return None
            return max(cand, key=lambda t: (t.priority, -t.created_at))

    def _run_task(self, task):
        with self._lock:
            task.state = Task.RUNNING
            task.touch()
        self._agent_state_begin(task)
        try:
            # 未拆解：尝试 LLM 生成步骤
            if not task.steps:
                self._plan(task)
            if task.needs_owner:
                self._pause_for_owner(task, "无法自动拆解任务步骤，请提供具体做法或取消该任务。")
                return
            if task.is_expired():
                self._finish(task, Task.CANCELLED, "任务超时（deadline 已到）")
                return

            steps = task.fallback_steps if task.fallback_in_use else task.steps
            n = len(steps)
            idx = task.current_step
            while idx < n:
                if self._stop.is_set():
                    self._finish(task, Task.CANCELLED, "系统关停")
                    return
                if self._global_emergency():
                    self._finish(task, Task.PAUSED, "紧急停止，任务已暂停")
                    return
                with self._lock:
                    if task.state != Task.RUNNING:
                        pass  # 被暂停/取消，走下方统一退出
                    if task.is_expired():
                        self._finish(task, Task.CANCELLED, "任务超时（deadline 已到）")
                        return
                if task.state != Task.RUNNING:
                    # 统一退出：同步 AgentState + 记录（暂停/取消也落痕）
                    self._agent_state_end(task)
                    self._record(task)
                    return
                step = steps[idx]
                ok, detail = self._execute_step(task, step, idx)
                if ok:
                    task.current_step = idx + 1
                    task.attempts = 0
                    idx += 1
                    continue
                # 失败：ASK 或重试或 fallback
                reason = detail.get("reason", "verify_failed") if isinstance(detail, dict) else "verify_failed"
                if detail and isinstance(detail, dict) and detail.get("needs_owner"):
                    self._pause_for_owner(task, "步骤 %d（%s）需要小塔介入：%s"
                                          % (idx + 1, step.get("action", "?"), reason))
                    return
                task.attempts += 1
                if task.attempts <= task.max_attempts:
                    self._log("任务 %s 步骤 %d 失败（%s），重试 %d/%d"
                              % (task.task_id, idx + 1, reason, task.attempts, task.max_attempts))
                    continue  # 同一 idx 重试
                # 重试耗尽：尝试替代方案（注意：必须重新取 steps/n，
                # 否则 while 继续用旧步骤序列执行）
                if task.fallback_steps and not task.fallback_in_use:
                    task.fallback_in_use = True
                    task.current_step = 0
                    idx = 0
                    task.attempts = 0
                    steps = task.fallback_steps
                    n = len(steps)
                    self._log("任务 %s 重试耗尽，切换替代方案。" % task.task_id)
                    continue
                self._pause_for_owner(task, "步骤 %d（%s）多次失败（%s），请小塔决定：继续尝试 / 修改方案 / 取消。"
                                      % (idx + 1, step.get("action", "?"), reason))
                return
            # 全部步骤完成：收尾前再检查 急停/暂停/取消/超时
            if self._global_emergency():
                self._finish(task, Task.PAUSED, "紧急停止，任务已暂停")
                return
            with self._lock:
                if task.state != Task.RUNNING:
                    self._agent_state_end(task)
                    self._record(task)
                    return
                if task.is_expired():
                    self._finish(task, Task.CANCELLED, "任务超时（deadline 已到）")
                    return
            self._finish(task, Task.DONE, "")
        except Exception as e:
            self._finish(task, Task.FAILED, "任务异常：%s" % e)

    def _execute_step(self, task, step, idx):
        """执行单步。返回 (True, result) 或 (False, {"reason","needs_owner"})。
        执行：优先注入的 exec_fn（测试/定制），否则走 ToolGateway（唯一真实出口）。"""
        if step.get("exec_fn") is not None:
            try:
                fn = step["exec_fn"]
                result = self._run_with_timeout(fn, step.get("params") or {})
            except Exception as e:
                return False, {"reason": "exec_error:%s" % e, "needs_owner": False}
        else:
            gw = getattr(context, "context.tool_gateway", None)
            if gw is None:
                return False, {"reason": "no_tool_gateway", "needs_owner": True}
            try:
                result = self._run_with_timeout(
                    lambda: gw.execute(step.get("target", ""),
                                       step.get("action", ""),
                                       step.get("params") or {},
                                       actor="agent",
                                       require_confirm=bool(step.get("require_confirm", False))),
                    None)
            except Exception as e:
                return False, {"reason": "gateway_error:%s" % e, "needs_owner": False}
        # 验证：verify_fn 优先；否则按返回结构判断
        ok, note = self._verify_step(task, step, result)
        if ok:
            task.result[str(idx)] = {"ok": True, "result": result, "attempts": task.attempts}
            return True, result
        if isinstance(result, dict) and result.get("reason") in (
                "trust_risk_cap", "permission_denied", "high_power_limited",
                "forbidden_operation", "confirmation_rejected",
                "confirmation_unavailable", "emergency_stop", "driving_restriction"):
            return False, {"reason": result.get("reason"), "needs_owner": True}
        task.result[str(idx)] = {"ok": False, "result": result, "attempts": task.attempts,
                                 "verify_note": note}
        return False, {"reason": note or "verify_failed", "needs_owner": False}

    def _run_with_timeout(self, fn, arg):
        fut = self._executor.submit(fn, arg) if arg is not None else self._executor.submit(fn)
        return fut.result(timeout=self._step_timeout)

    def _verify_step(self, task, step, result):
        """执行后验证：不轻信返回值 ok，用 verify_fn 或期望结果核对。"""
        vf = step.get("verify_fn")
        if vf is not None:
            try:
                ok = bool(vf(step.get("expected_result"), result))
                return ok, ("verify_ok" if ok else "verify_failed")
            except Exception as e:
                return False, "verify_error:%s" % e
        if isinstance(result, dict):
            if result.get("ok"):
                return True, "ok"
            return False, result.get("reason", "not_ok")
        return False, "non_dict_result"

    def _pause_for_owner(self, task, reason):
        with self._lock:
            task.needs_owner = True
            task.owner_reason = reason
            task.state = Task.PAUSED
            task.touch()
        self._agent_state_end(task)
        self._record(task)
        self._agent_state_mark_asked(task)
        self._notify_owner("【任务 %s】%s\n%s\n回复「任务 继续 %s」放行，或「任务 取消 %s」终止。"
                           % (task.task_id, task.goal, reason, task.task_id, task.task_id))
        self._log("任务 %s 请求小塔介入：%s" % (task.task_id, reason))

    def _plan(self, task):
        """LLM 拆解：目标 → 步骤 JSON。失败转 ASK，不让任务裸奔。"""
        try:
            llm = getattr(context, "context.llm_client", None)
            if llm is None:
                raise RuntimeError("context.llm_client 不可用")
            sys_p = ("你是任务规划器。把用户目标拆成可执行的步骤序列，"
                     "输出 JSON 数组，每项格式："
                     '{"action":"操作名","target":"设备/对象id","params":{},"expected_result":"期望结果描述"}'
                     "只输出 JSON，不要多余文字。")
            resp = llm.call(sys_p, [{"role": "user", "content": task.goal}],
                            temperature=0.3, max_tokens=1200)
            txt = (resp or "").strip()
            start, end = txt.find("["), txt.rfind("]")
            if start < 0 or end < 0:
                raise RuntimeError("LLM 未返回 JSON")
            steps = _json.loads(txt[start:end + 1])
            if not isinstance(steps, list) or not steps:
                raise RuntimeError("空步骤")
            clean = []
            for st in steps[:10]:
                clean.append({
                    "action": str(st.get("action", "")),
                    "target": str(st.get("target", "")),
                    "params": st.get("params") or {},
                    "expected_result": str(st.get("expected_result", "")),
                })
            task.steps = clean
            self._log("任务 %s 已拆解为 %d 步。" % (task.task_id, len(clean)))
        except Exception as e:
            task.needs_owner = True
            task.owner_reason = "任务拆解失败：%s" % e
            self._log("任务 %s 拆解失败：%s" % (task.task_id, e))

    # ================= 收尾 =================
    def _finish(self, task, state, note):
        with self._lock:
            task.state = state
            if note:
                task.error = note
            task.touch()
        self._agent_state_end(task)
        self._record(task)
        if state == Task.DONE:
            self._notify_owner("【任务 %s】已完成 ✅\n%s\n%s"
                               % (task.task_id, task.goal, self._result_summary(task)))
        elif state == Task.FAILED:
            self._notify_owner("【任务 %s】失败 ❌\n%s\n%s"
                               % (task.task_id, task.goal, note))
        elif state == Task.CANCELLED:
            self._notify_owner("【任务 %s】已取消：%s" % (task.task_id, note))

    def _result_summary(self, task):
        lines = []
        for k in sorted(task.result, key=lambda x: int(x)):
            r = task.result[k]
            if isinstance(r.get("result"), dict):
                lines.append("步骤 %s：%s" % (int(k) + 1, r["result"].get("message", "ok")))
            else:
                lines.append("步骤 %s：ok" % (int(k) + 1))
        return "\n".join(lines) or "（无步骤记录）"

    def _record(self, task):
        """结果进：记忆（context.life_engine.record_event）+ AgentState + 落盘。"""
        summary = {"task_id": task.task_id, "goal": task.goal, "state": task.state,
                   "error": task.error, "source": task.source, "ts": task.updated_at,
                   "result": task.result}
        try:
            hist = getattr(context, "dm", None)
            if hist is not None:
                data = hist.load(self._persist_key, {})
                data[task.task_id] = summary
                keys = sorted(data.keys())
                for k in keys[: max(0, len(keys) - 50)]:
                    data.pop(k, None)
                hist.save(self._persist_key, data)
        except Exception:
            pass
        try:
            le = getattr(context, "life_engine", None)
            if le is not None and callable(getattr(le, "record_event", None)):
                le.record_event("system", "task_result",
                                "任务%s %s：%s" % (task.task_id,
                                                  "完成" if task.state == Task.DONE else task.state.lower(),
                                                  task.goal),
                                importance=0.6)
        except Exception:
            pass
        try:
            st = getattr(context, "agent_state", None)
            if st is not None and callable(getattr(st, "record_action", None)):
                st.record_action("task:%s %s" % (task.task_id, task.goal))
                st.record_event("task_result", task.state, task.goal)
                st.save()
        except Exception:
            pass

    def _agent_state_begin(self, task):
        try:
            st = getattr(context, "agent_state", None)
            if st is not None and callable(getattr(st, "begin_task", None)):
                st.begin_task(task.task_id)
        except Exception:
            pass

    def _agent_state_end(self, task):
        try:
            st = getattr(context, "agent_state", None)
            if st is not None and callable(getattr(st, "end_task", None)):
                st.end_task(task.task_id)
        except Exception:
            pass

    def _agent_state_mark_asked(self, task):
        try:
            st = getattr(context, "agent_state", None)
            if st is not None and callable(getattr(st, "mark_asked", None)):
                st.mark_asked()
                st.save()
        except Exception:
            pass

    def _notify_owner(self, text):
        try:
            _ref = getattr(context, "context.wsm", None)
            _ws = getattr(_ref, "_ws", None) if _ref is not None else None
            if _ws is None or not getattr(_ws, "sock", None):
                return
            owner = str(CONFIG.get("owner_qq", ""))
            if not owner:
                return
            send_private(_ws, owner, text)
        except Exception:
            pass

    def _log(self, text):
        try:
            logger.info("[任务] %s", text)
        except Exception:
            pass

# ================= 命令层：「任务」系列 =================
@register_command("private", ["任务"], perm_required=2, owner_only=True)
def _cmd_task(msg):
    parts = (msg or "").split()
    sub = parts[0] if parts else ""
    tm = getattr(context, "context.task_manager", None)
    if tm is None:
        return "任务系统未启动。"
    tid = parts[1] if len(parts) > 1 else ""
    if sub == "开始":
        goal = " ".join(parts[1:]).strip()
        if not goal:
            return "用法：任务 开始 <目标描述>"
        t = tm.submit(goal, source="command")
        if t is None:
            return "同目标任务冷却中（5 分钟内已提交过），请稍后再试。"
        return "任务 %s 已提交：%s\n%s" % (t.task_id, goal, tm.describe(t))
    if sub == "列表" or sub == "查看" or sub == "":
        return tm.list_tasks()
    if sub == "详情":
        if not tid:
            return "用法：任务 详情 <id>"
        t = tm.get(tid)
        return tm.describe(t) if t else "任务不存在：%s" % tid
    if sub == "暂停" and tid:
        return tm.pause(tid)
    if sub == "恢复" and tid:
        return tm.resume(tid)
    if sub == "取消" and tid:
        return tm.cancel(tid)
    if sub == "继续" and tid:
        return tm.approve_continue(tid)
    if sub == "重试" and tid:
        return tm.retry(tid)
    return ("任务系统用法：\n"
            "任务 开始 <目标>　—— 提交任务（LLM 自动拆解步骤）\n"
            "任务 列表 / 详情 <id>\n"
            "任务 暂停|恢复|取消 <id>\n"
            "任务 继续 <id>　　—— 放行等待你介入的任务（ASK）\n"
            "任务 重试 <id>　　—— 重跑已失败/已取消的任务")

