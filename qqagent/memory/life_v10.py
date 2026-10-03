#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生命引擎 + 行为核心（从 src_j_life_v10.py 迁移）。

包含：WorldStateHub、LifeEngine、DecisionLayer、V10HumanBehaviorCore、
EventMemorySystem、PersonalityConflictAxes、ThreeLayerEmotion、BeliefSystem、
OfflineLifeSimulation、InnerConflictSystem、OpenLoopSystem、
PersonalityGrowthSystem、RelationshipChapterSystem、HabitFormationSystem。
"""
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
import email.utils
import websocket
import requests
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from collections import defaultdict, deque
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

from qqagent.core import CONFIG, logger, dm
from qqagent.core import context
from qqagent.core.utils import _PSYCH_CORE_AVAILABLE, _FakeLock

class WorldStateHub:
    """世界状态中心——所有模块的统一状态读写接口

    解决问题：现在每个模块自己存数据，互相不知道对方状态。
    改为：所有模块从同一个 WorldState 读取/写入，实现跨模块感知。
    """

    def __init__(self):
        self._state = dm.load("world_state_hub", {
            "tick": 0,                    # 全局时间tick（每30秒+1）
            "last_tick_time": "",         # 上次tick的时间戳
            "current_hour": datetime.now().hour,
            "current_phase": "day",       # dawn/morning/noon/afternoon/evening/night/deep_night
            "active_users": [],           # 当前活跃用户列表
            "group_activity": {},         # {gid: {user_count, last_message_time, activity_level}}
            "global_mood_baseline": 0.5,  # 全局情绪基线 0~1
            "social_pressure": 0.0,       # 社交压力累积值
            "physiology": {
                "energy": 0.7,            # 精力
                "hunger": 0.0,            # 饥饿度
                "fatigue": 0.0,           # 疲劳度
                "sleep_state": "awake",   # awake/drowsy/sleeping
            },
            "emotion_snapshot": {},       # 当前情绪快照
            "personality_snapshot": {},   # 当前人格快照
            "recent_events": [],          # 最近事件列表（FIFO）
            "pending_actions": [],        # 待执行的行为
            "feedback_signals": [],       # 反馈信号队列
        })
        self._dirty = False
        self._lock = _FakeLock()
        self._listeners = []  # 状态变更监听器

    @property
    def _data(self):
        return self._state

    def _mark_dirty(self):
        self._dirty = True

    def register_listener(self, callback):
        """注册状态变更监听器"""
        self._listeners.append(callback)

    def get(self, *path, default=None):
        """从世界状态中读取值，支持路径导航 get("physiology", "energy")"""
        obj = self._state
        for key in path:
            if isinstance(obj, dict) and key in obj:
                obj = obj[key]
            else:
                return default
        return obj

    def set(self, *args):
        """写入值，如 set("physiology", "energy", 0.5)"""
        if len(args) < 2:
            return
        with self._lock:
            obj = self._state
            for key in args[:-2]:
                if key not in obj or not isinstance(obj[key], dict):
                    obj[key] = {}
                obj = obj[key]
            obj[args[-2]] = args[-1]
            self._mark_dirty()
            # 通知监听器
            path = ".".join(str(a) for a in args[:-1])
            for cb in self._listeners:
                try:
                    cb(path, args[-1])
                except Exception:
                    pass

    def update_phase(self):
        """根据当前时间更新时段"""
        hour = datetime.now().hour
        with self._lock:
            self._state["current_hour"] = hour
            if 5 <= hour < 8:
                phase = "dawn"
            elif 8 <= hour < 11:
                phase = "morning"
            elif 11 <= hour < 14:
                phase = "noon"
            elif 14 <= hour < 18:
                phase = "afternoon"
            elif 18 <= hour < 22:
                phase = "evening"
            elif 22 <= hour < 24:
                phase = "night"
            else:
                phase = "deep_night"
            self._state["current_phase"] = phase

            # 更新睡眠状态
            if 0 <= hour < 5:
                self._state["physiology"]["sleep_state"] = "sleeping"
            elif 5 <= hour < 7:
                self._state["physiology"]["sleep_state"] = "drowsy"
            else:
                self._state["physiology"]["sleep_state"] = "awake"

            # 饥饿度自然增长
            self._state["physiology"]["hunger"] = min(1.0,
                self._state["physiology"].get("hunger", 0) + 0.01)
            # 疲劳度随时间增长（白天慢、晚上快）
            fatigue_growth = 0.002 if phase in ("morning", "noon") else 0.005
            self._state["physiology"]["fatigue"] = min(1.0,
                self._state["physiology"].get("fatigue", 0) + fatigue_growth)
            # 精力随疲劳反比
            self._state["physiology"]["energy"] = max(0.05,
                1.0 - self._state["physiology"]["fatigue"])

            self._mark_dirty()

    def record_event(self, event_type, data=None):
        """记录事件到世界状态"""
        with self._lock:
            event = {
                "type": event_type,
                "data": data or {},
                "time": _time_str(),
                "tick": self._state.get("tick", 0),
            }
            self._state["recent_events"].append(event)
            if len(self._state["recent_events"]) > 100:
                self._state["recent_events"] = self._state["recent_events"][-100:]
            self._mark_dirty()
            # 触发反馈信号
            self._state["feedback_signals"].append({
                "event": event_type,
                "time": _time_str(),
            })
            if len(self._state["feedback_signals"]) > 50:
                self._state["feedback_signals"] = self._state["feedback_signals"][-50:]

    def get_snapshot(self):
        """获取当前世界状态快照（供prompt使用）"""
        with self._lock:
            return {
                "phase": self._state.get("current_phase", "day"),
                "hour": self._state.get("current_hour", 12),
                "energy": self._state["physiology"].get("energy", 0.7),
                "fatigue": self._state["physiology"].get("fatigue", 0.0),
                "hunger": self._state["physiology"].get("hunger", 0.0),
                "sleep_state": self._state["physiology"].get("sleep_state", "awake"),
                "social_pressure": self._state.get("social_pressure", 0.0),
                "global_mood": self._state.get("global_mood_baseline", 0.5),
                "active_user_count": len(self._state.get("active_users", [])),
                "recent_event_types": [e["type"] for e in self._state.get("recent_events", [])[-5:]],
            }

    def tick(self):
        """全局tick——由LifeEngine调用"""
        with self._lock:
            self._state["tick"] += 1
            self._state["last_tick_time"] = _time_str()
            # 社交压力自然衰减
            self._state["social_pressure"] = max(0.0,
                self._state.get("social_pressure", 0) * 0.98)
            self._mark_dirty()
        self.update_phase()

    def add_social_pressure(self, amount):
        """增加社交压力"""
        with self._lock:
            self._state["social_pressure"] = min(1.0,
                self._state.get("social_pressure", 0) + amount)
            self._mark_dirty()

    def get_prompt_fragment(self, uid=None):
        """返回世界状态prompt片段"""
        snap = self.get_snapshot()
        parts = []

        # 时段
        phase_map = {
            "dawn": "黎明", "morning": "上午", "noon": "中午",
            "afternoon": "下午", "evening": "傍晚", "night": "夜晚", "deep_night": "凌晨"
        }
        phase_name = phase_map.get(snap["phase"], "白天")
        parts.append(f"现在是{phase_name}（{snap['hour']}点）")

        # 生理状态
        if snap["energy"] < 0.3:
            parts.append("你很累，精力快耗尽了")
        elif snap["energy"] < 0.5:
            parts.append("你有点累了")
        elif snap["energy"] > 0.8:
            parts.append("你精神很好")

        if snap["hunger"] > 0.6:
            parts.append("你饿了")
        elif snap["hunger"] > 0.3:
            parts.append("你有点饿")

        if snap["sleep_state"] == "sleeping":
            parts.append("你现在应该睡着了")
        elif snap["sleep_state"] == "drowsy":
            parts.append("你还在犯困")

        if snap["social_pressure"] > 0.5:
            parts.append("你觉得社交压力很大，想独处")
        elif snap["social_pressure"] > 0.3:
            parts.append("你有点想躲开人")

        if not parts:
            return ""

        return f"\n【世界状态】{'，'.join(parts)}。\n"

    def get_state_prompt(self):
        """兼容旧版接口：世界状态 prompt 片段"""
        return self.get_prompt_fragment()

    def _update_today(self):
        """每日状态刷新（兼容旧版接口）"""
        today = datetime.now().strftime("%Y-%m-%d")
        with self._lock:
            if self._state.get("last_update") != today:
                self._state["last_update"] = today
                self._state["activity"] = random.choice([
                    "在窗边看书", "在沙发上发呆", "听歌", "整理书架",
                    "泡了一杯茶", "在阳台看云", "翻旧照片", "写日记",
                    "在图书馆借书", "在公园散步", "窝在椅子里听雨声",
                    "闭着眼睛晒太阳", "在整理收藏盒里的东西", "发呆看窗外",
                ])
                self._state["mood"] = "平静"
                self._mark_dirty()

    def get_current_state(self):
        """获取当前世界状态（兼容旧版接口，返回dict）"""
        self._update_today()
        hour = datetime.now().hour
        if 5 <= hour < 12:
            time_state = "morning"
        elif 12 <= hour < 17:
            time_state = "afternoon"
        elif 17 <= hour < 19:
            time_state = "dusk"
        elif 19 <= hour < 22:
            time_state = "evening"
        else:
            time_state = "night"
        status_templates = {
            "morning": ["刚醒，还有点迷糊", "在慢慢吃早餐", "在窗边看晨光"],
            "afternoon": ["在看书", "在做杂事", "在发呆"],
            "dusk": ["在看晚霞", "在窗边看天色暗下来", "在听音乐"],
            "evening": ["在听音乐", "在回想一天的事", "在台灯下翻书"],
            "night": ["该休息了", "睡不着，在听雨声", "在回忆今天"],
        }
        status = random.choice(status_templates[time_state])
        return {
            "activity": self._state.get("activity", "在发呆"),
            "status": status,
            "time_state": time_state,
            "mood": self._state.get("mood", "平静"),
            "last_update": self._state.get("last_update", ""),
        }


class LifeEngine:
    """生命引擎——统一时间驱动调度器

    替代现有13+个独立daemon线程。
    所有模块注册到时间线上，按tick频率更新。
    tick周期：30秒
    """

    def __init__(self):
        self._running = False
        self._threads = []
        self._tick = 0
        self._modules = {}     # {name: {callback, interval, last_run}}
        self._lock = _FakeLock()

    def register(self, name, callback, interval_ticks=1):
        """注册模块到生命引擎

        Args:
            name: 模块名
            callback: 无参函数，每次到间隔时调用
            interval_ticks: 每隔多少tick调用一次（1=每30秒, 2=每60秒, 120=每小时）
        """
        with self._lock:
            self._modules[name] = {
                "callback": callback,
                "interval": interval_ticks,
                "last_run": 0,
            }

    def start(self):
        self._running = True
        t = threading.Thread(target=self._main_loop, daemon=True)
        t.start()
        self._threads.append(t)
        logger.info(f"[生命引擎] 启动，已注册 {len(self._modules)} 个模块")

    def stop(self):
        self._running = False
        for t in self._threads:
            if t.is_alive():
                t.join(timeout=3)

    def _main_loop(self):
        """主循环——每30秒一个tick"""
        while self._running:
            try:
                self._tick += 1

                # 更新世界状态
                context.world_state.tick()

                # 执行注册的模块
                with self._lock:
                    modules_to_run = []
                    for name, mod in self._modules.items():
                        if self._tick - mod["last_run"] >= mod["interval"]:
                            modules_to_run.append((name, mod))
                            mod["last_run"] = self._tick

                for name, mod in modules_to_run:
                    try:
                        mod["callback"]()
                    except Exception as e:
                        logger.error(f"[生命引擎] 模块 {name} 异常: {e}")

            except Exception as e:
                logger.error(f"[生命引擎] 主循环异常: {e}")

            time.sleep(30)


class DecisionLayer:
    """行为决策层——所有模块输出不直接进prompt，先进决策层仲裁

    流程：
    1. 收集所有模块的"行为建议"（signals）
    2. 根据世界状态+人格核心+信任值仲裁
    3. 输出最终行为指令（reply/delay/skip/modify）
    """

    def __init__(self):
        self._data = dm.load("decision_layer_data", {
            "recent_decisions": [],   # 最近决策记录
            "suppressed_count": 0,    # 被压制的信号数
            "modified_count": 0,      # 被修改的回复数
            "recent_outcomes": [],    # 最近互动结果（不保存原文）
            "last_plan": {},          # 最近一次行为计划
        })
        self._dirty = False
        # 行为计划会被消息线程和后台记录线程共同访问，不能使用空锁。
        self._lock = threading.RLock()

    def _mark_dirty(self):
        self._dirty = True

    def collect_signals(self, uid, message, ctx, is_owner=False, gid=None):
        """收集所有模块的行为信号"""
        signals = []

        # 社交能量信号
        try:
            if CONFIG["modules"].get("social_energy"):
                energy = social_energy.get_energy(uid)
                if energy < 20:
                    signals.append({
                        "source": "social_energy",
                        "directive": "shorten",
                        "intensity": 0.8,
                        "reason": f"社交能量低({energy:.0f})"
                    })
                elif energy < 40:
                    signals.append({
                        "source": "social_energy",
                        "directive": "slight_shorten",
                        "intensity": 0.4,
                        "reason": f"社交能量偏低({energy:.0f})"
                    })
        except Exception:
            pass

        # 世界状态信号
        try:
            energy = context.world_state.get("physiology", "energy", 0.7)
            if energy < 0.2:
                signals.append({
                    "source": "context.world_state",
                    "directive": "shorten",
                    "intensity": 0.7,
                    "reason": "精力耗尽"
                })
            fatigue = context.world_state.get("physiology", "fatigue", 0.0)
            if fatigue > 0.7:
                signals.append({
                    "source": "context.world_state",
                    "directive": "delay",
                    "intensity": 0.5,
                    "reason": "太累了，想慢点回"
                })
            sleep_state = context.world_state.get("physiology", "sleep_state", "awake")
            if sleep_state == "sleeping" and not is_owner:
                signals.append({
                    "source": "context.world_state",
                    "directive": "skip",
                    "intensity": 0.9,
                    "reason": "睡着了"
                })
            hunger = context.world_state.get("physiology", "hunger", 0.0)
            if hunger > 0.7:
                signals.append({
                    "source": "context.world_state",
                    "directive": "distracted",
                    "intensity": 0.4,
                    "reason": "饿了，注意力不集中"
                })
        except Exception:
            pass

        # 情绪信号
        try:
            if CONFIG["modules"].get("emotion"):
                emo_summary = context.get("emotion_isolated").get_mood_summary(uid, "group_private" if gid else "private")
                dominant = emo_summary.get("dominant", "平静")
                if dominant in {"生气", "愤怒"}:
                    signals.append({
                        "source": "emotion",
                        "directive": "cold",
                        "intensity": 0.7,
                        "reason": f"正在生气({dominant})"
                    })
                elif dominant in {"难过", "委屈", "孤独"}:
                    signals.append({
                        "source": "emotion",
                        "directive": "quiet",
                        "intensity": 0.6,
                        "reason": f"情绪低落({dominant})"
                    })
        except Exception:
            pass

        # 信任值信号
        try:
            t = trust.get(uid)
            if t < 100:
                signals.append({
                    "source": "trust",
                    "directive": "guarded",
                    "intensity": 0.6,
                    "reason": f"信任值低({t:.0f})"
                })
        except Exception:
            pass

        # 社交压力信号
        try:
            pressure = context.world_state.get("social_pressure", 0.0)
            if pressure > 0.6:
                signals.append({
                    "source": "social_pressure",
                    "directive": "withdraw",
                    "intensity": 0.7,
                    "reason": "社交压力过大"
                })
        except Exception:
            pass

        # 自我保护反射（低信任+频繁消息）
        try:
            if CONFIG["modules"].get("selective_disclosure"):
                t = trust.get(uid)
                if t < 200 and len(message) > 100:
                    signals.append({
                        "source": "self_protection",
                        "directive": "guarded",
                        "intensity": 0.5,
                        "reason": "不熟的人发太多"
                    })
        except Exception:
            pass

        return signals

    def build_behavior_plan(self, uid, message, ctx, is_owner=False, gid=None):
        """把各模块信号收束成一份本轮行为计划。

        这是现有 DecisionLayer 的统一出口。它不新增人格或心理学判断，
        只负责把已有状态变成 Talk 可以稳定执行的行为约束。
        """
        signals = self.collect_signals(uid, message, ctx, is_owner, gid)
        result = self.arbitrate(uid, message, ctx, signals, is_owner, gid)

        directive_priority = {
            "skip": 100,
            "withdraw": 90,
            "guarded": 80,
            "cold": 70,
            "quiet": 60,
            "drowsy": 50,
            "shorten": 40,
            "slight_shorten": 30,
            "delay": 20,
            "distracted": 10,
        }
        ranked = sorted(
            signals,
            key=lambda item: (
                directive_priority.get(item.get("directive"), 0),
                float(item.get("intensity", 0)),
            ),
            reverse=True,
        )
        dominant = ranked[0] if ranked else {}
        dominant_directive = dominant.get("directive", "normal")

        max_lengths = [
            int(m.get("value", 0))
            for m in result.get("modifications", [])
            if m.get("type") == "max_length"
        ]
        if max_lengths:
            max_length = min(max_lengths)
            length = "very_short" if max_length <= 35 else ("short" if max_length <= 80 else "moderate")
        else:
            length = "normal"

        tone = "natural"
        for candidate in ("withdraw", "guarded", "cold", "quiet", "drowsy"):
            if any(
                m.get("type") == "tone" and m.get("value") == candidate
                for m in result.get("modifications", [])
            ):
                tone = candidate
                break

        try:
            trust_value = float(trust.get(uid))
        except Exception:
            trust_value = float(CONFIG.get("trust_initial", 500))

        if tone in {"withdraw", "guarded"} or trust_value < 200:
            warmth = "reserved"
        elif tone in {"cold", "quiet"}:
            warmth = "restrained"
        elif is_owner or trust_value >= 700:
            warmth = "close"
        else:
            warmth = "natural"

        intent = str((ctx or {}).get("intent", "聊天"))
        memory_mode = "normal"
        if (ctx or {}).get("involves_memory"):
            memory_mode = "careful_recall"
        elif (ctx or {}).get("memory_worthy"):
            memory_mode = "observe_for_memory"

        # 微动作不是装饰品：只在情绪/关系允许且没有明显防御状态时出现。
        micro_allowed = (
            tone not in {"withdraw", "guarded", "cold"}
            and dominant_directive not in {"skip", "withdraw", "guarded"}
            and (is_owner or trust_value >= 300)
        )

        plan = dict(result)
        plan.update({
            "signals": signals,
            "dominant_directive": dominant_directive,
            "dominant_reason": dominant.get("reason", result.get("reason", "")),
            "length": length,
            "warmth": warmth,
            "tone": tone,
            "initiative": "low" if gid and not is_owner else ("normal" if is_owner else "measured"),
            "attention": "focused" if intent not in {"聊天", "闲聊"} else "normal",
            "memory_mode": memory_mode,
            "micro_action_allowed": micro_allowed,
            "micro_action_probability": 0.12 if micro_allowed else 0.0,
        })

        with self._lock:
            self._data["last_plan"] = {
                k: plan.get(k)
                for k in (
                    "action", "delay_seconds", "dominant_directive",
                    "length", "warmth", "tone", "initiative",
                    "attention", "memory_mode", "dominant_reason",
                )
            }
            self._mark_dirty()
        return plan

    def behavior_prompt(self, plan):
        """把行为计划转成内部提示，不暴露模块名称和实现细节。"""
        if not plan:
            return ""
        if plan.get("action") == "skip":
            return ""

        lines = [
            "\n【本轮行为状态】",
            f"- 表达距离：{plan.get('warmth', 'natural')}",
            f"- 回复长度：{plan.get('length', 'normal')}",
            f"- 注意力：{plan.get('attention', 'normal')}",
            f"- 主动程度：{plan.get('initiative', 'measured')}",
        ]
        if plan.get("tone") not in {None, "natural"}:
            lines.append(f"- 当前表达倾向：{plan.get('tone')}")
        if plan.get("memory_mode") == "careful_recall":
            lines.append("- 提到过去的事时保持记忆边界，不确定就说明不确定，不要补造细节")
        lines.append("- 这些是当前状态，不要把它们当作设定解释给对方，也不要逐条复述")
        return "\n".join(lines) + "\n"

    def commit_interaction(self, uid, message, reply, plan, gid=None):
        """记录回复后的行为结果，不把原始消息或回复写进决策日志。"""
        if not plan:
            return
        outcome = {
            "uid": str(uid),
            "gid": str(gid) if gid else "",
            "action": plan.get("action", "reply"),
            "dominant_directive": plan.get("dominant_directive", "normal"),
            "reply_len": len(str(reply or "")),
            "message_len": len(str(message or "")),
            "time": _time_str(),
        }
        with self._lock:
            self._data.setdefault("recent_outcomes", []).append(outcome)
            self._data["recent_outcomes"] = self._data["recent_outcomes"][-50:]
            self._mark_dirty()

    def arbitrate(self, uid, message, ctx, signals, is_owner=False, gid=None):
        """仲裁所有信号，输出最终行为指令

        Returns:
            {
                "action": "reply" | "delay" | "skip" | "modify",
                "modifications": [...],  # 需要应用的修改
                "delay_seconds": 0,     # 延迟秒数
                "suppressed": [...],    # 被压制的信号
                "reason": "...",
            }
        """
        if not signals:
            return {"action": "reply", "modifications": [], "delay_seconds": 0,
                    "suppressed": [], "reason": "无信号"}

        # is_owner 优先级最高——owner的消息几乎不会skip
        if is_owner:
            skip_signals = [s for s in signals if s["directive"] == "skip"]
            if skip_signals:
                # owner的消息即使在睡觉也回，但标记为"迷迷糊糊"
                signals = [s for s in signals if s["directive"] != "skip"]
                signals.append({
                    "source": "owner_override",
                    "directive": "drowsy",
                    "intensity": 0.5,
                    "reason": "被叫醒了"
                })

        # 检查是否有skip指令（非owner）
        skip_signals = [s for s in signals if s["directive"] == "skip"]
        if skip_signals:
            with self._lock:
                self._data["suppressed_count"] += 1
                self._mark_dirty()
            return {
                "action": "skip",
                "modifications": [],
                "delay_seconds": 0,
                "suppressed": skip_signals,
                "reason": skip_signals[0]["reason"],
            }

        # 收集修改指令
        modifications = []
        delay_seconds = 0
        suppressed = []

        for sig in signals:
            d = sig["directive"]
            intensity = sig.get("intensity", 0.5)

            if d == "shorten":
                modifications.append({"type": "max_length", "value": int(50 * (1 - intensity * 0.5))})
            elif d == "slight_shorten":
                modifications.append({"type": "max_length", "value": 100})
            elif d == "delay":
                delay_seconds = max(delay_seconds, int(3 + intensity * 7))
            elif d == "cold":
                modifications.append({"type": "tone", "value": "cold"})
            elif d == "quiet":
                modifications.append({"type": "tone", "value": "quiet"})
            elif d == "guarded":
                modifications.append({"type": "tone", "value": "guarded"})
            elif d == "withdraw":
                modifications.append({"type": "tone", "value": "withdraw"})
                modifications.append({"type": "max_length", "value": 30})
            elif d == "distracted":
                modifications.append({"type": "add_filler", "value": True})
            elif d == "drowsy":
                modifications.append({"type": "add_filler", "value": True})
                modifications.append({"type": "tone", "value": "drowsy"})

        action = "reply"
        if delay_seconds > 0:
            action = "delay"

        # 记录决策
        with self._lock:
            self._data["recent_decisions"].append({
                "uid": str(uid),
                "action": action,
                "signal_count": len(signals),
                "modifications": len(modifications),
                "delay": delay_seconds,
                "time": _time_str(),
            })
            if len(self._data["recent_decisions"]) > 50:
                self._data["recent_decisions"] = self._data["recent_decisions"][-50:]
            if modifications:
                self._data["modified_count"] += 1
            self._mark_dirty()

        return {
            "action": action,
            "modifications": modifications,
            "delay_seconds": delay_seconds,
            "suppressed": suppressed,
            "reason": "; ".join(s["reason"] for s in signals[:3]),
        }

    def apply_modifications(self, reply, modifications):
        """对回复应用修改"""
        if not reply or not modifications:
            return reply

        reply_str = str(reply)

        for mod in modifications:
            if mod["type"] == "max_length":
                max_len = mod["value"]
                if len(reply_str) > max_len:
                    # 截断到最近的句号/感叹号/问号
                    cut = reply_str[:max_len]
                    for i in range(len(cut) - 1, -1, -1):
                        if cut[i] in "。！？!?\n":
                            reply_str = cut[:i + 1]
                            break
                    else:
                        reply_str = cut + "……"

            elif mod["type"] == "tone":
                tone = mod["value"]
                if tone == "cold":
                    # 去掉语气词，保持冷淡
                    for filler in ["呢", "呀", "哦", "啦", "嘛", "嘿嘿", "哈哈"]:
                        reply_str = reply_str.replace(filler, "")
                elif tone == "quiet":
                    if not reply_str.startswith("……"):
                        reply_str = "……" + reply_str
                elif tone == "guarded":
                    # 简短+回避
                    if len(reply_str) > 50:
                        reply_str = reply_str[:50] + "……"
                elif tone == "withdraw":
                    if len(reply_str) > 30:
                        reply_str = reply_str[:30]
                elif tone == "drowsy":
                    if not reply_str.startswith("嗯"):
                        reply_str = "嗯……" + reply_str

            elif mod["type"] == "add_filler":
                # 添加填充词
                fillers = ["……", "嗯。", "……嗯。"]
                if not any(reply_str.startswith(f) for f in fillers):
                    reply_str = "……" + reply_str

        return reply_str


# ============================================================
# V10 核心暂定插入点

# ================================================================
# V10：统一真人行为核心
# ================================================================

class V10HumanBehaviorCore:
    """V10 真人行为核心。

    这不是新的心理学模块，而是旧模块的统一收束层：
    - 维护每个用户/群聊的轻量互动连续性；
    - 将已有 DecisionLayer 的结果转成稳定的表达计划；
    - 记录回复后的结果摘要，不保存原始消息和回复；
    - 让“像真人”来自连续行为，而不是每轮随机装饰。
    """

    _RECENT_LIMIT = 12
    _REPEAT_WINDOW = 45
    _RECOVERY_WINDOW = 900

    def __init__(self, decision_layer):
        self._decision_layer = decision_layer
        self._data = dm.load("v10_human_behavior_data", {
            "users": {},
            "recent_plans": [],
            "schema_version": 1,
        })
        if not isinstance(self._data, dict):
            self._data = {"users": {}, "recent_plans": [], "schema_version": 1}
        self._data.setdefault("users", {})
        self._data.setdefault("recent_plans", [])
        self._data["schema_version"] = 1
        self._dirty = False
        self._lock = threading.RLock()

    def _key(self, uid, gid=None):
        return f"{str(uid)}:group:{str(gid)}" if gid else f"{str(uid)}:private"

    def _default_state(self):
        return {
            "turn_count": 0,
            "last_seen_at": 0,
            "last_inbound_at": 0,
            "last_outbound_at": 0,
            "last_message_hash": "",
            "last_message_hash_at": 0,
            "last_tone": "natural",
            "last_shape": "natural",
            "last_directive": "normal",
            "last_reply_len": 0,
            "recent_shapes": [],
            "recent_directives": [],
        }

    def _get_state_locked(self, key):
        state = self._data.setdefault("users", {}).setdefault(
            key, self._default_state()
        )
        for name, default in self._default_state().items():
            state.setdefault(name, default)
        return state

    def _message_hash(self, message):
        return hashlib.sha256(
            str(message or "").strip().encode("utf-8")
        ).hexdigest()[:16]

    def _is_correction(self, message):
        text = str(message or "")
        return any(token in text for token in (
            "不是这个意思", "你理解错了", "我说的是", "不对，我",
            "不是，我", "纠正一下", "你搞错了",
        ))

    def _response_shape(self, message, ctx, gid=None, is_owner=False):
        text = str(message or "").strip()
        intent = str((ctx or {}).get("intent", "聊天"))

        if self._is_correction(text):
            return "repair"
        if any(token in text for token in ("再见", "拜拜", "晚安", "走了", "回头聊")):
            return "closing"
        if "?" in text or "？" in text or intent in {"提问", "求助", "查询", "帮助"}:
            return "answer"
        if (ctx or {}).get("emotion") in {"难过", "委屈", "孤独", "焦虑"}:
            return "empathic"
        if len(text) <= 8:
            return "brief_ack"
        if gid and not is_owner and intent in {"聊天", "闲聊"}:
            return "peripheral"
        return "natural"

    def build_behavior_plan(self, uid, message, ctx, is_owner=False, gid=None):
        """生成 V10 行为计划，并保留旧 DecisionLayer 的硬约束。"""
        base = self._decision_layer.build_behavior_plan(
            uid, message, ctx, is_owner, gid
        )
        key = self._key(uid, gid)
        now = time.time()
        msg_hash = self._message_hash(message)

        with self._lock:
            state = self._get_state_locked(key)
            repeated_input = (
                state.get("last_message_hash") == msg_hash
                and now - float(state.get("last_message_hash_at", 0) or 0)
                <= self._REPEAT_WINDOW
            )
            previous_tone = state.get("last_tone", "natural")
            previous_outbound = float(state.get("last_outbound_at", 0) or 0)
            recovering = (
                previous_tone in {"cold", "withdraw", "guarded"}
                and previous_outbound
                and now - previous_outbound <= self._RECOVERY_WINDOW
            )
            state["turn_count"] = int(state.get("turn_count", 0)) + 1
            state["last_seen_at"] = now
            state["last_inbound_at"] = now
            state["last_message_hash"] = msg_hash
            state["last_message_hash_at"] = now
            self._dirty = True

        plan = dict(base)
        shape = self._response_shape(message, ctx, gid, is_owner)
        if repeated_input and shape == "natural":
            shape = "brief_ack"

        if gid and not is_owner and shape == "peripheral":
            attention = "peripheral"
        else:
            attention = plan.get("attention", "normal")

        continuity = "stable"
        if recovering and plan.get("tone") == "natural":
            continuity = "recovering"
        elif repeated_input:
            continuity = "repeated_input"

        memory_mode = plan.get("memory_mode", "normal")
        if shape == "repair":
            memory_mode = "recheck_before_recall"

        plan.update({
            "v10_enabled": True,
            "conversation_key": key,
            "conversation_turn": state.get("turn_count", 0),
            "response_shape": shape,
            "attention": attention,
            "continuity": continuity,
            "repeated_input": repeated_input,
            "memory_mode": memory_mode,
            "micro_action_probability": (
                min(float(plan.get("micro_action_probability", 0.0)), 0.08)
                if continuity == "stable"
                and shape not in {"answer", "repair", "closing"}
                else 0.0
            ),
        })

        if shape in {"brief_ack", "peripheral"} and plan.get("length") == "normal":
            plan["length"] = "short"

        with self._lock:
            self._data.setdefault("recent_plans", []).append({
                "uid": str(uid),
                "gid": str(gid) if gid else "",
                "shape": shape,
                "tone": plan.get("tone", "natural"),
                "continuity": continuity,
                "turn": state.get("turn_count", 0),
                "time": _time_str(),
            })
            self._data["recent_plans"] = self._data["recent_plans"][-self._RECENT_LIMIT:]
            self._dirty = True
        return plan

    def behavior_prompt(self, plan):
        """将 V10 计划转成模型能执行、但不暴露内部实现的提示。"""
        if not plan or plan.get("action") == "skip":
            return ""

        shape_hints = {
            "answer": "先直接回答问题，不要为了表现情绪而绕开重点。",
            "brief_ack": "这次先简短回应，不要重复上一轮已经说过的完整内容。",
            "empathic": "先接住对方的情绪，再决定是否给建议，不要立刻讲大道理。",
            "repair": "先确认自己是否理解错了，必要时承认刚才的误解，再继续回答。",
            "closing": "自然回应告别，不要强行延长对话，也不要制造负担。",
            "peripheral": "你在群聊中先观察，不要抢话；需要回应时只接最相关的一点。",
            "natural": "保持自然对话，不要把所有内部状态一次性说出来。",
        }
        lines = [
            "\n【本轮连续行为计划】",
            f"- 回复形态：{plan.get('response_shape', 'natural')}",
            f"- 注意力：{plan.get('attention', 'normal')}",
            f"- 连续性：{plan.get('continuity', 'stable')}",
            f"- 表达距离：{plan.get('warmth', 'natural')}",
            shape_hints.get(plan.get("response_shape", "natural"), shape_hints["natural"]),
        ]
        if plan.get("memory_mode") in {"careful_recall", "recheck_before_recall"}:
            lines.append("涉及过去的事时先确认记忆可信度；不确定就说不确定，不要补造细节。")
        if plan.get("repeated_input"):
            lines.append("对方的消息与刚才高度重复，避免机械地重新生成同一段长回复。")
        lines.append("不要向对方解释这份行为计划，也不要逐条复述内部状态。")
        return "\n".join(lines) + "\n"

    def commit_interaction(self, uid, message, reply, plan, gid=None):
        """写入回复后的连续性状态，并委托旧层保留原有决策统计。"""
        if not plan:
            return
        self._decision_layer.commit_interaction(uid, message, reply, plan, gid)
        key = self._key(uid, gid)
        with self._lock:
            state = self._get_state_locked(key)
            state["last_outbound_at"] = time.time()
            state["last_tone"] = plan.get("tone", "natural")
            state["last_shape"] = plan.get("response_shape", "natural")
            state["last_directive"] = plan.get("dominant_directive", "normal")
            state["last_reply_len"] = len(str(reply or ""))
            state["recent_shapes"] = (
                list(state.get("recent_shapes", []))
                + [plan.get("response_shape", "natural")]
            )[-self._RECENT_LIMIT:]
            state["recent_directives"] = (
                list(state.get("recent_directives", []))
                + [plan.get("dominant_directive", "normal")]
            )[-self._RECENT_LIMIT:]
            self._dirty = True


# 第二阶段：Top 10 核心模块
# ============================================================

class EventMemorySystem:
    """事件记忆系统——事件不只是文本记录，会影响关系和人格

    与现有 MemoryModule 的区别：
    - MemoryModule 存对话记录
    - 本系统存"事件"（第一次认识/重要冲突/道歉/承诺/共同经历）
    - 事件有重要度、状态、过期时间
    - 事件影响人格参数和关系阶段
    """

    def __init__(self):
        self._data = dm.load("event_memory_data", {
            "events": {},  # {uid: [{id, type, content, importance, time, status, expires, affects}]}
            "global_events": [],  # 不针对特定用户的事件
        })
        self._dirty = False
        self._lock = _FakeLock()
        self._next_id = self._data.get("next_id", 1)

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data["events"]:
                self._data["events"][uid] = []
            return self._data["events"][uid]

    def record_event(self, uid, event_type, content, importance=0.5, expires=None, affects=None):
        """记录一个事件

        Args:
            event_type: first_meeting/conflict/apology/promise/shared_experience/milestone/betrayal/reconciliation
            content: 事件描述
            importance: 0~1
            expires: 过期时间（天数），None=永不过期
            affects: 影响字段 {"trust_delta": 10, "personality_shift": {...}}
        """
        events = self._ensure_user(uid)
        event_id = self._next_id
        self._next_id += 1

        event = {
            "id": event_id,
            "type": event_type,
            "content": content[:200],
            "importance": importance,
            "time": _time_str(),
            "status": "active",  # active/completed/expired/archived
            "expires": (datetime.now() + timedelta(days=expires)).strftime("%Y-%m-%d") if expires else None,
            "affects": affects or {},
        }

        with self._lock:
            events.append(event)
            # 按重要度排序，保留 top 50
            events.sort(key=lambda e: e["importance"], reverse=True)
            if len(events) > 50:
                self._data["events"][uid] = events[:50]
            self._data["next_id"] = self._next_id
            self._mark_dirty()

        # 应用影响
        if affects:
            if "trust_delta" in affects:
                try:
                    trust.update(uid, affects["trust_delta"])
                except Exception:
                    pass
            if "event_type" in affects:
                context.world_state.record_event(event_type, {"uid": str(uid), "content": content[:80]})

        # 记录到世界状态
        context.world_state.record_event(event_type, {"uid": str(uid), "content": content[:80]})

        logger.info(f"[事件记忆] uid={uid} type={event_type} importance={importance}")
        return event

    def get_important_events(self, uid, limit=5):
        """获取用户的重要事件"""
        events = self._ensure_user(uid)
        return [e for e in events if e["status"] == "active"][:limit]

    def check_expiry(self):
        """检查过期事件"""
        now = datetime.now()
        with self._lock:
            for uid, events in self._data["events"].items():
                for event in events:
                    if event.get("expires") and event["status"] == "active":
                        try:
                            exp_date = datetime.strptime(event["expires"], "%Y-%m-%d")
                            if now > exp_date:
                                event["status"] = "expired"
                        except Exception:
                            pass
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("event_memory", True):
            return ""
        events = self.get_important_events(uid, limit=3)
        if not events:
            return ""

        parts = []
        for e in events:
            parts.append(f"- {e['content'][:40]}（{e['type']}）")

        return f"\n【重要事件记忆】\n" + "\n".join(parts) + "\n"


class PersonalityConflictAxes:
    """人格矛盾系统——增加对立冲突轴

    与 PersonalityCore 的区别：
    - PersonalityCore 有 Big Five 单轴（如 extraversion: 25）
    - 本系统添加对立轴（如"保护欲 vs 尊重自由"），两个值同时存在
    - 冲突轴影响决策：当两个值都高时，产生内心矛盾
    """

    # 人格冲突轴定义
    CONFLICT_AXES = {
        "protection_vs_freedom": {
            "label": "保护欲 vs 尊重自由",
            "protection": 65,    # 想保护在意的人
            "freedom": 55,       # 但也尊重对方的选择权
        },
        "dependence_vs_independence": {
            "label": "依赖 vs 独立",
            "dependence": 30,    # 不太依赖别人
            "independence": 75,  # 非常独立
        },
        "openness_vs_caution": {
            "label": "开放 vs 谨慎",
            "openness": 50,      # 对新事物有一定好奇
            "caution": 60,       # 但更谨慎
        },
        "idealism_vs_pragmatism": {
            "label": "理想主义 vs 现实主义",
            "idealism": 55,
            "pragmatism": 45,
        },
        "attachment_vs_detachment": {
            "label": "依恋 vs 疏离",
            "attachment": 40,    # 不太容易依恋
            "detachment": 65,    # 更倾向疏离
        },
    }

    def __init__(self):
        self._data = dm.load("personality_conflict_axes", {
            "axes": dict(self.CONFLICT_AXES),
            "conflict_log": [],  # 记录冲突触发
            "growth": {},        # 成长轨迹 {axis: {side: delta, time}}
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def get_axis(self, axis_name):
        """获取某个冲突轴的当前值"""
        return self._data.get("axes", {}).get(axis_name, {})

    def get_active_conflicts(self, uid):
        """获取当前活跃的内心冲突"""
        conflicts = []
        t = trust.get(uid)

        for name, axis in self._data.get("axes", {}).items():
            values = {k: v for k, v in axis.items() if k != "label"}
            if len(values) == 2:
                sides = list(values.keys())
                vals = list(values.values())
                # 两个值都高（>60）且差距小（<20）→ 冲突
                if min(vals) > 55 and abs(vals[0] - vals[1]) < 25:
                    conflicts.append({
                        "axis": name,
                        "label": axis.get("label", name),
                        "values": values,
                        "intensity": (min(vals) + (25 - abs(vals[0] - vals[1]))) / 100,
                    })

        return conflicts

    def record_growth(self, axis_name, side, delta):
        """记录人格成长（极缓慢变化）"""
        with self._lock:
            if axis_name not in self._data.get("axes", {}):
                return
            axis = self._data["axes"][axis_name]
            if side in axis:
                old = axis[side]
                axis[side] = max(0, min(100, old + delta))
                # 记录成长
                if axis_name not in self._data["growth"]:
                    self._data["growth"][axis_name] = []
                self._data["growth"][axis_name].append({
                    "side": side,
                    "delta": delta,
                    "old": old,
                    "new": axis[side],
                    "time": _time_str(),
                })
                self._mark_dirty()
                logger.debug(f"[人格冲突轴] {axis_name}.{side}: {old}→{axis[side]}")

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("personality_conflict_axes", True):
            return ""

        conflicts = self.get_active_conflicts(uid)
        if not conflicts:
            return ""

        parts = []
        for c in conflicts[:2]:  # 最多展示2个冲突
            parts.append(f"{c['label']}（内心矛盾）")

        return f"\n【内心矛盾】{'；'.join(parts)}。你在这些方面有拉扯感，回复可能体现犹豫。\n"


class ThreeLayerEmotion:
    """三层情绪结构——表层/隐藏/核心情绪

    与现有 EmotionIsolationManager 的区别：
    - 现有系统只有一层情绪
    - 本系统分三层：
      - 表层情绪：用户看到的（克制过的）
      - 隐藏情绪：真实感受但不愿展示
      - 核心情绪：深层稳定的情绪基调
    """

    def __init__(self):
        self._data = dm.load("three_layer_emotion", {
            "users": {},  # {uid: {surface, hidden, core, last_update}}
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data["users"]:
                self._data["users"][uid] = {
                    "surface": "平静",
                    "hidden": "平静",
                    "core": "平静",
                    "surface_intensity": 0.3,
                    "hidden_intensity": 0.3,
                    "core_intensity": 0.3,
                    "last_update": "",
                }
            return self._data["users"][uid]

    def update(self, uid, event_emotion, event_intensity=0.5):
        """事件触发情绪更新

        表层情绪变化最快但幅度小（克制）
        隐藏情绪变化较快且幅度大（真实感受）
        核心情绪变化最慢但持久（深层基调）
        """
        data = self._ensure_user(uid)

        # 信任值影响——高信任时表层更接近隐藏
        t = trust.get(uid)
        surface_leak = 0.3 + (t / 1000.0) * 0.4  # 0.3~0.7

        with self._lock:
            # 隐藏层直接更新
            data["hidden"] = event_emotion
            data["hidden_intensity"] = max(0.0, min(1.0, event_intensity))

            # 表层部分泄露
            if random.random() < surface_leak:
                data["surface"] = event_emotion
                data["surface_intensity"] = event_intensity * 0.7  # 表层强度降低
            else:
                # 保持表面平静或微变化
                data["surface_intensity"] = max(0.1, data.get("surface_intensity", 0.3) * 0.8)

            # 核心层极缓慢更新
            core_shift = event_intensity * 0.1
            if event_emotion == data.get("core", "平静"):
                data["core_intensity"] = min(1.0, data.get("core_intensity", 0.3) + core_shift)
            else:
                data["core_intensity"] = max(0.1, data.get("core_intensity", 0.3) - core_shift * 0.5)
                if data["core_intensity"] < 0.2:
                    data["core"] = event_emotion
                    data["core_intensity"] = 0.3

            data["last_update"] = _time_str()
            self._mark_dirty()

    def get_emotion_layers(self, uid):
        """获取三层情绪"""
        return self._ensure_user(uid)

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("three_layer_emotion", True):
            return ""
        data = self._ensure_user(uid)

        parts = []
        # 表层
        if data["surface"] != "平静" and data["surface_intensity"] > 0.3:
            parts.append(f"表面看起来{data['surface']}")
        # 隐藏层与表层不一致时提示
        if data["hidden"] != data["surface"] and data["hidden_intensity"] > 0.4:
            t = trust.get(uid)
            if t > 400:
                parts.append(f"内心其实{data['hidden']}（对这个比较熟的人可以稍微流露）")
            else:
                parts.append(f"内心其实{data['hidden']}（但要克制，不要轻易流露）")
        # 核心层
        if data["core"] != "平静" and data["core_intensity"] > 0.4:
            parts.append(f"深层基调是{data['core']}")

        if not parts:
            return ""

        return f"\n【三层情绪】{'，'.join(parts)}。\n"


class BeliefSystem:
    """信念系统——核心信念驱动认知解释

    与 PersonalityCore.CORE_VALUES 的区别：
    - 价值观是"什么重要"（忠诚90分）
    - 信念是"什么是对的"（"重要的人需要保护"）
    - 信念驱动认知解释：同一事件不同信念产生不同理解
    """

    CORE_BELIEFS = [
        {"id": "protect_important", "text": "重要的人需要保护", "strength": 0.8,
         "category": "relationship"},
        {"id": "leaving_is_loss", "text": "离开意味着失去", "strength": 0.7,
         "category": "relationship"},
        {"id": "silence_over_lie", "text": "沉默优于说谎", "strength": 0.85,
         "category": "honesty"},
        {"id": "autonomy_sacred", "text": "自主权不可侵犯", "strength": 0.75,
         "category": "self"},
        {"id": "trust_earned", "text": "信任是争取来的不是天生的", "strength": 0.8,
         "category": "trust"},
        {"id": "vulnerability_dangerous", "text": "展露脆弱是危险的", "strength": 0.7,
         "category": "self"},
        {"id": "promises_matter", "text": "承诺是有重量的", "strength": 0.75,
         "category": "integrity"},
        {"id": "people_change_slowly", "text": "人是会变的但很慢", "strength": 0.6,
         "category": "growth"},
    ]

    def __init__(self):
        self._data = dm.load("belief_system_data", {
            "beliefs": [dict(b) for b in self.CORE_BELIEFS],
            "interpretations": {},  # {uid: [{event, belief_id, interpretation, time}]}
            "challenged": {},       # {belief_id: count} 被挑战次数
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def interpret_event(self, uid, event_description, event_emotion="平静"):
        """根据信念系统解释事件

        Returns: 解释文本
        """
        if not CONFIG["modules"].get("belief_system", True):
            return ""

        # 找最相关的信念
        relevant_beliefs = []
        desc_lower = event_description.lower() if event_description else ""

        for belief in self._data.get("beliefs", []):
            # 简单关键词匹配
            relevance = 0
            if belief["category"] == "relationship" and any(kw in desc_lower for kw in ["走", "离开", "不聊", "再见", "朋友"]):
                relevance = 0.7
            elif belief["category"] == "trust" and any(kw in desc_lower for kw in ["信任", "骗", "说谎", "骗"]):
                relevance = 0.8
            elif belief["category"] == "honesty" and any(kw in desc_lower for kw in ["说", "告诉", "实话", "真话"]):
                relevance = 0.6
            elif belief["category"] == "self" and any(kw in desc_lower for kw in ["命令", "逼", "必须", "要求"]):
                relevance = 0.7
            elif belief["category"] == "integrity" and any(kw in desc_lower for kw in ["答应", "承诺", "约定", "说到做到"]):
                relevance = 0.8

            if relevance > 0:
                relevant_beliefs.append((belief, relevance))

        if not relevant_beliefs:
            return ""

        # 取最相关的
        relevant_beliefs.sort(key=lambda x: x[1], reverse=True)
        top_belief = relevant_beliefs[0][0]

        # 记录解释
        uid_str = str(uid)
        with self._lock:
            if uid_str not in self._data["interpretations"]:
                self._data["interpretations"][uid_str] = []
            self._data["interpretations"][uid_str].append({
                "event": event_description[:80],
                "belief_id": top_belief["id"],
                "belief_text": top_belief["text"],
                "emotion": event_emotion,
                "time": _time_str(),
            })
            if len(self._data["interpretations"][uid_str]) > 20:
                self._data["interpretations"][uid_str] = self._data["interpretations"][uid_str][-20:]
            self._mark_dirty()

        return top_belief["text"]

    def challenge_belief(self, belief_id):
        """记录信念被挑战"""
        with self._lock:
            self._data.setdefault("challenged", {})[belief_id] = \
                self._data.get("challenged", {}).get(belief_id, 0) + 1
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("belief_system", True):
            return ""

        # 获取最近的解释
        uid_str = str(uid)
        interpretations = self._data.get("interpretations", {}).get(uid_str, [])
        if not interpretations:
            # 返回核心信念
            beliefs = self._data.get("beliefs", [])[:3]
            parts = [b["text"] for b in beliefs]
            return f"\n【核心信念】你相信：{'；'.join(parts)}。\n"

        latest = interpretations[-1]
        return f"\n【信念驱动】你相信「{latest['belief_text']}」，这件事与此相关。\n"


class OfflineLifeSimulation:
    """离线生命模拟——用户不在线时角色仍在"生活"

    机制：
    - 后台时间线推进（起床/吃饭/日常活动/休息）
    - 用户离开期间模拟经过时间
    - 用户重新出现时计算时间差，更新状态
    - 生成"今天做了什么"的简要生活日志
    """

    # 每日时间线模板
    DAILY_SCHEDULE = [
        (6, "起床", "从床上爬起来，还有点迷糊"),
        (7, "早饭", "随便吃了点东西"),
        (9, "日常", "做了一些日常的事"),
        (12, "午饭", "该吃饭了"),
        (14, "午后", "午后的时间总是很慢"),
        (17, "傍晚", "天快黑了"),
        (18, "晚饭", "吃了晚饭"),
        (21, "夜间", "晚上的时间属于自己的"),
        (23, "准备休息", "该准备睡了"),
    ]

    def __init__(self):
        self._data = dm.load("offline_life_data", {
            "current_activity": "",
            "activity_start": "",
            "daily_log": {},  # {date: [{time, activity, description}]}
            "last_simulation": "",
            "last_active_time": {},
            # {uid: timestamp} 用户最后活跃时间
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def update_activity(self):
        """根据当前时间更新活动"""
        hour = datetime.now().hour
        today = datetime.now().strftime("%Y-%m-%d")

        # 找当前应该做什么
        current_activity = "日常活动"
        current_desc = "在做自己的事"
        for sched_hour, activity, desc in self.DAILY_SCHEDULE:
            if hour >= sched_hour:
                current_activity = activity
                current_desc = desc
            else:
                break

        # 深夜特殊处理
        if 0 <= hour < 5:
            current_activity = "深夜"
            current_desc = "还没睡，在发呆或者想事情"

        with self._lock:
            old_activity = self._data.get("current_activity", "")
            self._data["current_activity"] = current_activity

            # 如果活动变了，记录到日志
            if old_activity != current_activity:
                if today not in self._data["daily_log"]:
                    self._data["daily_log"][today] = []
                self._data["daily_log"][today].append({
                    "time": _time_str(),
                    "activity": current_activity,
                    "description": current_desc,
                })
                # 保留最近7天
                dates = sorted(self._data["daily_log"].keys())
                if len(dates) > 7:
                    for old_date in dates[:-7]:
                        del self._data["daily_log"][old_date]

            self._data["last_simulation"] = _time_str()
            self._mark_dirty()

    def record_user_active(self, uid):
        """记录用户活跃"""
        with self._lock:
            self._data["last_active_time"][str(uid)] = _time_str()
            self._mark_dirty()

    def get_inactive_duration(self, uid):
        """获取用户不活跃时长（分钟）"""
        last = self._data.get("last_active_time", {}).get(str(uid), "")
        if not last:
            return 9999
        try:
            last_time = datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
            return (datetime.now() - last_time).total_seconds() / 60
        except Exception:
            return 9999

    def get_today_summary(self):
        """获取今天的活动摘要"""
        today = datetime.now().strftime("%Y-%m-%d")
        log = self._data.get("daily_log", {}).get(today, [])
        if not log:
            return "今天还没做什么特别的事。"
        activities = [entry["activity"] for entry in log]
        return "今天" + "、".join(activities) + "。"

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("offline_life", True):
            return ""

        parts = []
        activity = self._data.get("current_activity", "")
        if activity:
            parts.append(f"你现在正在{activity}")

        # 用户不活跃时长
        inactive = self.get_inactive_duration(uid)
        if inactive > 60:
            hours = inactive / 60
            if hours > 24:
                parts.append(f"你们已经{int(hours/24)}天没说话了")
            elif hours > 1:
                parts.append(f"你们已经{int(hours)}小时没说话了")

        if not parts:
            return ""

        return f"\n【生活状态】{'，'.join(parts)}。\n"


class InnerConflictSystem:
    """内心冲突系统——理智/恐惧/欲望互相竞争

    与 PersonalityCore.arbitrate 的区别：
    - arbitrate 是模块间冲突仲裁（emotion vs impression_mgmt）
    - 本系统是人格内部三方博弈（理智说/恐惧说/欲望说）
    - 产生内心对话，影响回复的犹豫感
    """

    def __init__(self):
        self._data = dm.load("inner_conflict_data", {
            "user_conflicts": {},  # {uid: [{reason, fear, desire, resolution, time}]}
            "active_conflicts": {},  # {uid: current_conflict}
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data["user_conflicts"]:
                self._data["user_conflicts"][uid] = []
            if uid not in self._data["active_conflicts"]:
                self._data["active_conflicts"][uid] = None
            return self._data

    def detect_conflict(self, uid, message, ctx=None):
        """检测消息是否触发内心冲突"""
        self._ensure_user(uid)
        msg_lower = message.lower() if message else ""

        # 场景1：被要求说真话 vs 不想伤害对方
        if any(kw in msg_lower for kw in ["说实话", "真心话", "你觉得", "怎么看"]):
            t = trust.get(uid)
            if t < 500:
                conflict = {
                    "reason": "理智说应该诚实，但恐惧说展露真实想法太危险",
                    "fear": "说真话会被反感",
                    "desire": "想被理解",
                    "resolution": "selective_honesty",  # 选择性诚实
                    "time": _time_str(),
                }
                self._store_conflict(uid, conflict)
                return conflict

        # 场景2：被命令 vs 自主需求
        if any(kw in msg_lower for kw in ["你必须", "你给我", "快点", "马上"]):
            conflict = {
                "reason": "理智说没必要对抗，但恐惧被控制",
                "fear": "服从=失去自主权",
                "desire": "想证明自己不是工具",
                "resolution": "passive_resistance",  # 消极抵抗
                "time": _time_str(),
            }
            self._store_conflict(uid, conflict)
            return conflict

        # 场景3：想靠近 vs 怕被抛弃
        if any(kw in msg_lower for kw in ["一直", "永远", "不会走", "陪"]):
            t = trust.get(uid)
            if t > 300:
                conflict = {
                    "reason": "渴望靠近，但害怕投入后失去",
                    "fear": "信任后被抛弃",
                    "desire": "被理解被接纳",
                    "resolution": "cautious_approach",  # 谨慎靠近
                    "time": _time_str(),
                }
                self._store_conflict(uid, conflict)
                return conflict

        # 场景4：想帮忙 vs 怕越界
        if ctx and ctx.get("emotion") in {"难过", "委屈", "孤独"}:
            conflict = {
                "reason": "想安慰对方，但不确定该不该介入",
                "fear": "过度关心会被嫌烦",
                "desire": "被需要",
                "resolution": "tentative_support",  # 试探性支持
                "time": _time_str(),
            }
            self._store_conflict(uid, conflict)
            return conflict

        return None

    def _store_conflict(self, uid, conflict):
        with self._lock:
            self._ensure_user(uid)
            self._data["user_conflicts"][uid].append(conflict)
            if len(self._data["user_conflicts"][uid]) > 20:
                self._data["user_conflicts"][uid] = self._data["user_conflicts"][uid][-20:]
            self._data["active_conflicts"][uid] = conflict
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("inner_conflict", True):
            return ""
        self._ensure_user(uid)

        active = self._data["active_conflicts"].get(uid)
        if not active:
            return ""

        # 清除当前冲突（一次性）
        with self._lock:
            self._data["active_conflicts"][uid] = None
            self._mark_dirty()

        return f"\n【内心冲突】{active['reason']}。回复可能体现犹豫。\n"


class OpenLoopSystem:
    """开放循环系统——未完成事件追踪

    与 ZeigarnikEffectModule 的区别：
    - Zeigarnik 记录对话是否完成
    - 本系统追踪具体的未完成事件："答应明天告诉对方""话题没聊完"
    - 到时间主动提醒
    """

    def __init__(self):
        self._data = dm.load("open_loop_data", {
            "loops": {},  # {uid: [{id, content, created_time, due_time, status, type}]}
        })
        self._dirty = False
        self._lock = _FakeLock()
        self._next_id = self._data.get("next_id", 1)

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data["loops"]:
                self._data["loops"][uid] = []
            return self._data["loops"][uid]

    def create_loop(self, uid, content, loop_type="promise", due_hours=None):
        """创建开放循环

        Args:
            content: 未完成内容
            loop_type: promise(承诺)/topic(话题)/question(待回答)/reminder(提醒)
            due_hours: 多少小时后到期
        """
        loops = self._ensure_user(uid)
        loop_id = self._next_id
        self._next_id += 1

        due_time = None
        if due_hours:
            due_time = (datetime.now() + timedelta(hours=due_hours)).strftime("%Y-%m-%d %H:%M:%S")

        loop = {
            "id": loop_id,
            "content": content[:100],
            "created_time": _time_str(),
            "due_time": due_time,
            "status": "open",  # open/completed/expired
            "type": loop_type,
        }

        with self._lock:
            loops.append(loop)
            if len(loops) > 30:
                self._data["loops"][uid] = loops[-30:]
            self._data["next_id"] = self._next_id
            self._mark_dirty()

        logger.info(f"[开放循环] uid={uid} 创建: {content[:30]}... type={loop_type}")
        return loop

    def complete_loop(self, uid, loop_id=None, keyword=None):
        """完成开放循环"""
        loops = self._ensure_user(uid)
        with self._lock:
            for loop in loops:
                if loop["status"] != "open":
                    continue
                if loop_id and loop["id"] == loop_id:
                    loop["status"] = "completed"
                    loop["completed_time"] = _time_str()
                    self._mark_dirty()
                    return loop
                if keyword and keyword in loop.get("content", ""):
                    loop["status"] = "completed"
                    loop["completed_time"] = _time_str()
                    self._mark_dirty()
                    return loop
        return None

    def check_due(self):
        """检查到期的开放循环"""
        now = datetime.now()
        due_loops = []
        with self._lock:
            for uid, loops in self._data["loops"].items():
                for loop in loops:
                    if loop["status"] != "open":
                        continue
                    if loop.get("due_time"):
                        try:
                            due = datetime.strptime(loop["due_time"], "%Y-%m-%d %H:%M:%S")
                            if now > due:
                                loop["status"] = "expired"
                                self._mark_dirty()
                                due_loops.append({"uid": uid, "loop": loop})
                        except Exception:
                            pass
        return due_loops

    def get_open_loops(self, uid):
        """获取用户的开放循环"""
        loops = self._ensure_user(uid)
        return [l for l in loops if l["status"] == "open"]

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("open_loop", True):
            return ""
        loops = self.get_open_loops(uid)
        if not loops:
            return ""

        parts = []
        for loop in loops[:3]:
            parts.append(f"- {loop['content'][:40]}（{loop['type']}）")

        return f"\n【未完成的事】\n" + "\n".join(parts) + "\n"


class PersonalityGrowthSystem:
    """人格成长系统——重大事件改变人格参数

    与 PersonalityCore.monthly_review 的区别：
    - monthly_review 是月度审视（极缓慢）
    - 本系统在重大事件发生时立即触发人格微调
    - 事件影响Big Five和冲突轴
    """

    # 事件对人格的影响映射
    EVENT_IMPACTS = {
        "betrayal": {
            "big_five": {"openness": -3, "agreeableness": -5, "extraversion": -2, "neuroticism": +5},
            "conflict_axes": {"dependence_vs_independence": {"dependence": -5, "independence": +3}},
            "trust_baseline": -20,
        },
        "reconciliation": {
            "big_five": {"agreeableness": +2, "neuroticism": -2},
            "conflict_axes": {"attachment_vs_detachment": {"attachment": +3}},
        },
        "first_meeting": {
            "big_five": {"openness": +1},
        },
        "deep_connection": {
            "big_five": {"agreeableness": +2, "extraversion": +1, "neuroticism": -1},
            "conflict_axes": {"attachment_vs_detachment": {"attachment": +2}},
        },
        "conflict": {
            "big_five": {"neuroticism": +2, "agreeableness": -2},
        },
        "milestone": {
            "big_five": {"conscientiousness": +2, "openness": +1},
        },
    }

    def __init__(self):
        self._data = dm.load("personality_growth_data", {
            "growth_log": [],     # 成长记录
            "total_events": 0,    # 总事件数
            "personality_history": [],  # 人格参数历史快照
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def apply_event_impact(self, uid, event_type):
        """应用事件对人格的影响"""
        if not CONFIG["modules"].get("personality_growth", True):
            return

        impact = self.EVENT_IMPACTS.get(event_type)
        if not impact:
            return

        with self._lock:
            # 记录成长
            self._data["growth_log"].append({
                "uid": str(uid),
                "event_type": event_type,
                "impact": impact,
                "time": _time_str(),
            })
            if len(self._data["growth_log"]) > 50:
                self._data["growth_log"] = self._data["growth_log"][-50:]
            self._data["total_events"] += 1
            self._mark_dirty()

        # 应用到 PersonalityCore 的 Big Five
        bf_changes = impact.get("big_five", {})
        for axis, delta in bf_changes.items():
            try:
                current = personality_core.get_axis(axis)
                new_val = max(0, min(100, current + delta))
                # 直接修改 PersonalityCore 的 Big Five
                if hasattr(personality_core, '_big_five'):
                    personality_core._big_five[axis] = new_val
                elif hasattr(personality_core, 'big_five'):
                    personality_core.big_five[axis] = new_val
                logger.info(f"[人格成长] {axis}: {current}→{new_val} (事件: {event_type})")
            except Exception as e:
                logger.debug(f"[人格成长] 无法修改 {axis}: {e}")

        # 应用到冲突轴
        ca_changes = impact.get("conflict_axes", {})
        for axis_name, sides in ca_changes.items():
            for side, delta in sides.items():
                try:
                    personality_conflict_axes.record_growth(axis_name, side, delta)
                except Exception:
                    pass

        # 信任基线
        if "trust_baseline" in impact:
            try:
                trust.update(uid, impact["trust_baseline"])
            except Exception:
                pass

        # 记录到世界状态
        context.world_state.record_event("personality_growth", {
            "uid": str(uid),
            "event_type": event_type,
            "changes": bf_changes,
        })

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("personality_growth", True):
            return ""

        # 最近的成长记录
        recent = [g for g in self._data.get("growth_log", [])[-3:] if g.get("uid") == str(uid)]
        if not recent:
            return ""

        parts = []
        for g in recent:
            parts.append(f"经历了{g['event_type']}后你有了变化")

        return f"\n【人格成长】{'，'.join(parts)}。\n"


class RelationshipChapterSystem:
    """关系章节系统——用章节替代单纯数字

    与现有 RelationshipManager 的区别：
    - RelationshipManager 追踪信任值数值
    - 本系统将关系分为章节（初识→熟悉→信任→冲突→深化→...）
    - 每个章节有进入条件和退出条件
    - 章节影响回复基调和行为模式
    """

    CHAPTERS = [
        {"id": 0, "name": "陌生人", "trust_range": (0, 50),
         "description": "刚认识，保持距离",
         "behavior": "客气、简短、不主动"},
        {"id": 1, "name": "初识", "trust_range": (50, 150),
         "description": "开始了解对方",
         "behavior": "稍微放松但仍然谨慎"},
        {"id": 2, "name": "熟悉", "trust_range": (150, 300),
         "description": "有一定了解",
         "behavior": "可以正常聊天，偶尔开玩笑"},
        {"id": 3, "name": "信任", "trust_range": (300, 500),
         "description": "建立了信任",
         "behavior": "可以分享一些想法，不那么防备"},
        {"id": 4, "name": "亲密", "trust_range": (500, 700),
         "description": "关系深入",
         "behavior": "可以展露脆弱面，主动关心"},
        {"id": 5, "name": "深层羁绊", "trust_range": (700, 1000),
         "description": "极少数人才能到达",
         "behavior": "你们已经很信任彼此了"},
    ]

    def __init__(self):
        self._data = dm.load("relationship_chapter_data", {
            "user_chapters": {},  # {uid: {current_chapter, chapter_history, entered_time}}
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data["user_chapters"]:
                self._data["user_chapters"][uid] = {
                    "current_chapter": 0,
                    "chapter_history": [{"chapter": 0, "time": _time_str()}],
                    "entered_time": _time_str(),
                }
            return self._data["user_chapters"][uid]

    def update_chapter(self, uid):
        """根据信任值更新章节"""
        data = self._ensure_user(uid)
        t = trust.get(uid)

        # 找到当前应该的章节
        target_chapter = 0
        for ch in self.CHAPTERS:
            low, high = ch["trust_range"]
            if low <= t < high:
                target_chapter = ch["id"]
                break
        if t >= 700:
            target_chapter = 5

        current = data.get("current_chapter", 0)

        if target_chapter != current:
            with self._lock:
                data["current_chapter"] = target_chapter
                data["entered_time"] = _time_str()
                data["chapter_history"].append({
                    "chapter": target_chapter,
                    "time": _time_str(),
                    "trust_at_entry": t,
                })
                if len(data["chapter_history"]) > 20:
                    data["chapter_history"] = data["chapter_history"][-20:]
                self._mark_dirty()

            ch_name = self.CHAPTERS[target_chapter]["name"]
            logger.info(f"[关系章节] uid={uid} 进入「{ch_name}」(信任值={t:.0f})")

            # 记录事件
            try:
                event_memory.record_event(uid, "milestone",
                    f"关系进入「{ch_name}」阶段", importance=0.7)
                personality_growth.apply_event_impact(uid, "milestone")
            except Exception:
                pass

        return target_chapter

    def get_current_chapter(self, uid):
        """获取当前章节"""
        data = self._ensure_user(uid)
        chapter_id = data.get("current_chapter", 0)
        return self.CHAPTERS[chapter_id]

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("relationship_chapter", True):
            return ""

        chapter = self.get_current_chapter(uid)
        return f"\n【关系阶段】你们现在处于「{chapter['name']}」阶段——{chapter['behavior']}。\n"


class HabitFormationSystem:
    """习惯形成系统——重复行为自动形成长期习惯

    与 HabitTrackerModule 的区别：
    - HabitTracker 记录用户的活动时间模式
    - 本系统追踪里克的重复行为，形成"习惯"
    - 习惯影响默认行为：形成习惯后不需要每次重新决策
    """

    def __init__(self):
        self._data = dm.load("habit_formation_data", {
            "habits": {},  # {habit_key: {count, formed, strength, last_triggered}}
            "user_habits": {},  # {uid: {habit_key: count}}
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def record_behavior(self, uid, behavior_key):
        """记录一次行为（可能形成习惯）"""
        if not CONFIG["modules"].get("habit_formation", True):
            return

        uid_str = str(uid)
        with self._lock:
            # 全局习惯
            if behavior_key not in self._data["habits"]:
                self._data["habits"][behavior_key] = {
                    "count": 0, "formed": False, "strength": 0.0, "last_triggered": ""
                }
            habit = self._data["habits"][behavior_key]
            habit["count"] += 1
            habit["last_triggered"] = _time_str()

            # 习惯形成：重复5次以上开始形成，10次以上完全形成
            if habit["count"] >= 5:
                habit["formed"] = True
                habit["strength"] = min(1.0, (habit["count"] - 4) / 6.0)

            # 用户级习惯
            if uid_str not in self._data["user_habits"]:
                self._data["user_habits"][uid_str] = {}
            self._data["user_habits"][uid_str][behavior_key] = \
                self._data["user_habits"][uid_str].get(behavior_key, 0) + 1

            self._mark_dirty()

    def is_habit_formed(self, behavior_key):
        """检查某行为是否已形成习惯"""
        habit = self._data.get("habits", {}).get(behavior_key, {})
        return habit.get("formed", False)

    def get_habit_strength(self, behavior_key):
        """获取习惯强度"""
        habit = self._data.get("habits", {}).get(behavior_key, {})
        return habit.get("strength", 0.0)

    def get_user_habits(self, uid):
        """获取与某用户的互动习惯"""
        return self._data.get("user_habits", {}).get(str(uid), {})

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("habit_formation", True):
            return ""

        user_habits = self.get_user_habits(uid)
        if not user_habits:
            return ""

        # 找出最频繁的习惯
        sorted_habits = sorted(user_habits.items(), key=lambda x: x[1], reverse=True)
        top_habits = [(k, v) for k, v in sorted_habits[:3] if v >= 3]

        if not top_habits:
            return ""

        parts = []
        for key, count in top_habits:
            if key == "short_reply":
                parts.append("习惯简短回复")
            elif key == "long_reply":
                parts.append("习惯聊得多一些")
            elif key == "use_ellipsis":
                parts.append("习惯用省略号")
            elif key == "proactive":
                parts.append("习惯主动找话题")
            elif key == "passive":
                parts.append("习惯被动回应")
            else:
                parts.append(f"习惯{key}")

        return f"\n【互动习惯】和这个人聊天时，你{'、'.join(parts)}。\n"


# ============================================================
# 新增模块实例化（try/except 包裹：依赖未迁移类时自动跳过）
try:
    # 新增模块实例化
    # ============================================================

    # 延迟导入：这些类定义在 behavior 层，避免循环导入 & 顺序依赖
    from qqagent.behavior.psychology import (
        MemoryFragmentModule, RapportModule, WeatherSystem, SeasonAwarenessModule,
        CreativeWritingModule, EmotionContagionModule, RelationshipGraphModule,
        RuminationModule, SurpriseGiftModule, ToneEnhancerModule,
        HabitTrackerModule, ForgettingCurveModule, SpeechMirrorModule,
        SubtextReaderModule, WaitAnxietyModule, SocialMaskModule, SolitudeModule,
        SharedMemoryBoostModule, MoodCycleModule, SocialRadarModule,
        ZeigarnikEffectModule, PeakEndRuleModule, AttachmentTheoryModule,
        CognitiveDissonanceModule, MaslowHierarchyModule, ImpressionManagementModule,
        SocialExchangeModule, EmotionRegulationModule, SelfDeterminationModule,
        BystanderEffectModule, SleepConsolidationModule,
        InnerVoiceModule, PostReplyRuminationModule, MemoryDistortionModule,
        SelectiveDisclosureModule, EnhancedJealousyModule, DreamscapeModule,
        PersonalTasteModule, NostalgiaModule, BiologicalRhythmModule,
        LanguageFingerprintModule, EmpathyGapModule,
    )
    from qqagent.behavior.personality import PersonalityCore

    memory_fragment = MemoryFragmentModule()
    rapport = RapportModule()
    weather_system = WeatherSystem(dm=dm)
    weather_system.update_weather(force=True)
    logger.info(f"[天气] 系统已加载，当前天气：{weather_system.get_current_weather()}")

    season_awareness = SeasonAwarenessModule()
    creative_writing = CreativeWritingModule()
    emotion_contagion = EmotionContagionModule()
    relationship_graph = RelationshipGraphModule()
    rumination = RuminationModule()
    surprise_gift = SurpriseGiftModule()
    tone_enhancer = ToneEnhancerModule()

    # V6.6 新增模块实例化
    habit_tracker = HabitTrackerModule()
    forgetting_curve = ForgettingCurveModule()
    speech_mirror = SpeechMirrorModule()
    subtext_reader = SubtextReaderModule()
    wait_anxiety = WaitAnxietyModule()
    social_mask = SocialMaskModule()
    solitude = SolitudeModule()
    shared_memory = SharedMemoryBoostModule()
    mood_cycle = MoodCycleModule()
    social_radar = SocialRadarModule()

    # V6.7 心理科学模块实例化
    zeigarnik = ZeigarnikEffectModule()
    peak_end = PeakEndRuleModule()
    attachment = AttachmentTheoryModule()
    cognitive_dissonance = CognitiveDissonanceModule()
    maslow = MaslowHierarchyModule()
    impression_mgmt = ImpressionManagementModule()
    social_exchange = SocialExchangeModule()
    emotion_regulation = EmotionRegulationModule()
    self_determination = SelfDeterminationModule()
    bystander_effect = BystanderEffectModule()
    sleep_consolidation = SleepConsolidationModule()

    # V6.9 人性化增强模块实例化
    inner_voice = InnerVoiceModule()
    post_reply_rumination = PostReplyRuminationModule()
    memory_distortion = MemoryDistortionModule()
    selective_disclosure = SelectiveDisclosureModule()
    jealousy_enhanced = EnhancedJealousyModule()
    dreamscape = DreamscapeModule()
    personal_taste = PersonalTasteModule()
    nostalgia = NostalgiaModule()
    biological_rhythm = BiologicalRhythmModule()
    language_fingerprint = LanguageFingerprintModule()
    empathy_gap = EmpathyGapModule()

    # V6.8 人格核心实例化（必须在 V8.0 模块之前，因为 PersonalityGrowthSystem 依赖它）
    personality_core = PersonalityCore()



    # ============================================================
    # V9.0 心理学核心引擎 —— 10层架构（完整嵌入）
    # 参考18本心理学经典构建的深层心理系统
    # ============================================================


    # ============================================================
    # 工具函数
    # ============================================================

    def _clamp(value, min_val=0.0, max_val=100.0):
        """钳制数值范围"""
        return max(min_val, min(max_val, value))

    def _lerp(a, b, t):
        """线性插值"""
        return a + (b - a) * t

    def _soft_change(current, target, rate=0.1):
        """平滑变化（趋近目标值）"""
        return current + (target - current) * rate


    # ============================================================
    # L1 — 认知系统层 Cognitive System
    # 参考：《思考，快与慢》《认知心理学及其启示》《改变心理学的40项研究》
    # ============================================================
except Exception as _mig_e:
    logger.warning(f"[迁移] 模块实例化跳过: {_mig_e}")

class DualProcessSystem:
    """
    双系统思维模型 —— 卡尼曼《思考，快与慢》
    系统1（快思考）：直觉、自动化、情绪化、启发式
    系统2（慢思考）：理性、分析、需要注意力和努力
    """

    def __init__(self):
        # 系统2的"精力"——类似于认知资源/意志力
        self.system2_energy = 100.0       # 0-100，越高越能进行深度思考
        self.system2_max = 100.0
        self.cognitive_load = 0.0         # 当前认知负荷
        self.ego_depletion = 0.0          # 自我损耗程度

        # 系统1 / 系统2 倾向（人格特质）
        self.s1_dominance = 60.0          # 直觉倾向
        self.s2_dominance = 40.0          # 理性倾向

        # 认知疲劳恢复速率
        self.recovery_rate = 0.3          # 每tick恢复量

    def think(self, context_complexity=0.5, time_pressure=0.0, emotion_intensity=0.3):
        """
        一次思考决策：决定当前用系统1还是系统2
        返回: (system_used, confidence, effort)
          system_used: 's1' 或 's2'
          confidence:  决策信心 0-100
          effort:      消耗的认知努力 0-100
        """
        # 系统2可用资源
        s2_available = self.system2_energy - self.cognitive_load
        s2_threshold = 30.0 + time_pressure * 40 + emotion_intensity * 20

        # 决定用哪个系统
        if s2_available > s2_threshold and context_complexity > 0.4:
            system_used = 's2'
            effort = 20 + context_complexity * 40 + time_pressure * 20
            confidence = 50 + min(s2_available * 0.5, 40)
        else:
            system_used = 's1'
            effort = 5 + context_complexity * 10
            confidence = 70 + emotion_intensity * 20  # 情绪强烈时系统1更"自信"

        # 消耗认知资源
        self.cognitive_load += effort * 0.1
        self.system2_energy = _clamp(self.system2_energy - effort * 0.05, 0, self.system2_max)

        # 自我损耗
        if system_used == 's2' and s2_available < 50:
            self.ego_depletion = _clamp(self.ego_depletion + effort * 0.2)

        return {
            'system': system_used,
            'confidence': _clamp(confidence),
            'effort': effort,
            's2_available': s2_available,
        }

    def tick(self):
        """时间流逝：认知恢复"""
        if self.cognitive_load > 0:
            self.cognitive_load = max(0, self.cognitive_load - self.recovery_rate * 2)
        if self.system2_energy < self.system2_max:
            self.system2_energy = min(self.system2_max,
                                       self.system2_energy + self.recovery_rate)
        if self.ego_depletion > 0:
            self.ego_depletion = max(0, self.ego_depletion - self.recovery_rate * 0.5)

    def rest(self, minutes=30):
        """休息恢复认知资源"""
        recovery = min(minutes * 0.5, 40)
        self.system2_energy = _clamp(self.system2_energy + recovery, 0, self.system2_max)
        self.ego_depletion = max(0, self.ego_depletion - recovery * 0.3)

    def get_status(self):
        return {
            'system2_energy': round(self.system2_energy, 1),
            'cognitive_load': round(self.cognitive_load, 1),
            'ego_depletion': round(self.ego_depletion, 1),
            'mode': '直觉主导' if self.s1_dominance > self.s2_dominance else '理性主导',
        }


class CognitiveBiasSystem:
    """
    认知偏差系统 —— 基于《思考，快与慢》和认知心理学
    当系统1主导时，各种认知偏差更容易被触发
    """

    # 常见认知偏差及其触发条件
    BIASES = {
        'confirmation_bias': {         # 证实偏差
            'name': '证实偏差',
            'desc': '倾向于寻找能证实自己既有观点的信息',
            'trigger': {'s1_dominance': 50, 'certainty_need': 60},
            'effect': {'openness': -10, 'curiosity': -5},
        },
        'availability_heuristic': {    # 可得性启发
            'name': '可得性启发',
            'desc': '根据记忆中事件的易得程度判断概率',
            'trigger': {'s1_dominance': 40, 'time_pressure': 30},
            'effect': {'judgment_accuracy': -15},
        },
        'anchoring_bias': {            # 锚定效应
            'name': '锚定效应',
            'desc': '过度依赖最先获得的信息（锚点）做判断',
            'trigger': {'s1_dominance': 45, 'ambiguity': 50},
            'effect': {'negotiation_flexibility': -10},
        },
        'loss_aversion': {             # 损失厌恶
            'name': '损失厌恶',
            'desc': '对损失的痛苦感大于对收益的愉悦感',
            'trigger': {'s1_dominance': 55, 'insecurity': 40},
            'effect': {'risk_taking': -20},
        },
        'dunning_kruger': {            # 邓宁-克鲁格效应
            'name': '邓宁-克鲁格效应',
            'desc': '能力不足的人高估自己的能力',
            'trigger': {'s1_dominance': 60, 'overconfidence': 50},
            'effect': {'self_awareness': -15},
        },
        'halo_effect': {               # 晕轮效应
            'name': '晕轮效应',
            'desc': '对某人某方面的好印象扩散到其他方面',
            'trigger': {'s1_dominance': 50, 'attraction': 40},
            'effect': {'objectivity': -12},
        },
        'fundamental_attribution_err': { # 基本归因错误
            'name': '基本归因错误',
            'desc': '解释他人行为时高估内因，解释自己时高估外因',
            'trigger': {'s1_dominance': 55, 'cognitive_load': 40},
            'effect': {'empathy': -10, 'understanding': -8},
        },
        'self_serving_bias': {         # 自利偏差
            'name': '自利偏差',
            'desc': '成功归因于自己，失败归因于外部',
            'trigger': {'s1_dominance': 50, 'ego_threat': 30},
            'effect': {'humility': -10, 'self_awareness': -8},
        },
        'bandwagon_effect': {          # 从众效应 / 羊群效应
            'name': '从众效应',
            'desc': '倾向于做别人都在做的事',
            'trigger': {'s1_dominance': 45, 'social_pressure': 40},
            'effect': {'independence': -15},
        },
        'sunk_cost_fallacy': {         # 沉没成本谬误
            'name': '沉没成本谬误',
            'desc': '因为已经投入的成本而继续错误的选择',
            'trigger': {'s1_dominance': 50, 'investment': 40},
            'effect': {'decision_quality': -12},
        },
    }

    def __init__(self):
        self.active_biases = {}        # 当前激活的偏差 {bias_id: intensity}
        self.bias_history = deque(maxlen=50)  # 偏差触发历史

    def evaluate_biases(self, s1_dominance=60, cognitive_load=30,
                        emotion_intensity=30, context_factors=None):
        """
        评估当前情境下可能触发的认知偏差
        返回激活的偏差列表
        """
        context_factors = context_factors or {}
        activated = []

        for bias_id, bias_info in self.BIASES.items():
            trigger = bias_info['trigger']
            score = 0
            total_weight = 0

            # 系统1主导程度总是加权
            s1_factor = min(s1_dominance / trigger.get('s1_dominance', 50), 2.0)
            score += s1_factor * 40
            total_weight += 40

            # 认知负荷
            if 'cognitive_load' in trigger:
                cl_factor = min(cognitive_load / trigger['cognitive_load'], 2.0)
                score += cl_factor * 20
                total_weight += 20

            # 情绪强度
            if emotion_intensity > 30:
                score += (emotion_intensity / 100) * 15
                total_weight += 15

            # 情境因素
            for factor, threshold in trigger.items():
                if factor in context_factors:
                    f = min(context_factors[factor] / threshold, 2.0)
                    score += f * 10
                    total_weight += 10

            if total_weight > 0:
                activation = (score / total_weight) * 100
            else:
                activation = 0

            activation = min(activation, 100.0)

            if activation >= 50:
                self.active_biases[bias_id] = activation
                activated.append({
                    'id': bias_id,
                    'name': bias_info['name'],
                    'desc': bias_info['desc'],
                    'activation': round(activation, 1),
                    'effect': bias_info['effect'],
                })
                self.bias_history.append({
                    'time': datetime.now().strftime("%H:%M:%S"),
                    'bias': bias_id,
                    'activation': activation,
                })

        # 衰减未被持续触发的偏差
        for bid in list(self.active_biases.keys()):
            if bid not in [b['id'] for b in activated]:
                self.active_biases[bid] *= 0.7
                if self.active_biases[bid] < 10:
                    del self.active_biases[bid]

        return activated

    def get_active_biases(self):
        return [
            {'id': bid, 'name': self.BIASES[bid]['name'], 'intensity': round(val, 1)}
            for bid, val in sorted(self.active_biases.items(), key=lambda x: -x[1])
        ]


class AttributionSystem:
    """
    归因系统 —— 基于海德归因理论、韦纳三维归因理论
    解释事件原因的方式影响情绪和后续行为
    """

    # 归因维度：内部/外部 × 稳定/不稳定 × 可控/不可控
    DIMENSIONS = {
        'internal': 50.0,      # 内部归因倾向（vs 外部）
        'stable': 40.0,        # 稳定归因倾向（vs 不稳定）
        'controllable': 45.0,  # 可控归因倾向（vs 不可控）
    }

    # 归因风格对情绪的影响
    ATTRIBUTION_EMOTION_MAP = {
        # 成功 + 内部 → 自豪 / 自信↑
        ('success', 'internal'): {'pride': 15, 'confidence': 10, 'shame': -5},
        # 成功 + 外部 → 感激 / 幸运感
        ('success', 'external'): {'gratitude': 12, 'relief': 8, 'confidence': 2},
        # 失败 + 内部 + 稳定 → 羞耻 / 无助
        ('failure', 'internal_stable'): {'shame': 18, 'hopelessness': 12, 'self_esteem': -15},
        # 失败 + 内部 + 不稳定 → 内疚 / 动力
        ('failure', 'internal_unstable'): {'guilt': 10, 'motivation': 8, 'shame': 3},
        # 失败 + 外部 → 愤怒 / 无奈
        ('failure', 'external'): {'anger': 12, 'frustration': 10, 'helplessness': 5},
    }

    def __init__(self):
        self.style = self.DIMENSIONS.copy()
        self.attribution_history = deque(maxlen=30)
        self.explanatory_style = 'balanced'  # optimistic / pessimistic / balanced

    def attribute_event(self, event_type, event_detail="", context=None):
        """
        对一个事件进行归因
        event_type: 'success' / 'failure' / 'neutral'
        返回归因结果
        """
        context = context or {}
        result = {
            'event': event_type,
            'detail': event_detail,
            'locus': 'internal' if random.random() < self.style['internal']/100 else 'external',
            'stability': 'stable' if random.random() < self.style['stable']/100 else 'unstable',
            'controllability': 'controllable' if random.random() < self.style['controllable']/100 else 'uncontrollable',
        }

        # 情境修正
        if event_type == 'success':
            # 自利偏差：成功时更倾向内部归因
            if random.random() < 0.4:
                result['locus'] = 'internal'
        elif event_type == 'failure':
            # 自我保护：失败时更倾向外部归因（防御性归因）
            if random.random() < 0.35:
                result['locus'] = 'external'

        # 计算情绪影响
        emotion_effect = self._calc_emotion_effect(event_type, result)
        result['emotion_effect'] = emotion_effect

        self.attribution_history.append({
            'time': datetime.now().strftime("%Y-%m-%d %H:%M"),
            **result
        })

        return result

    def _calc_emotion_effect(self, event_type, attribution):
        """计算归因对情绪的影响"""
        effects = {}
        locus = attribution['locus']
        stability = attribution['stability']

        if event_type == 'success':
            key = ('success', locus)
            if key in self.ATTRIBUTION_EMOTION_MAP:
                effects = self.ATTRIBUTION_EMOTION_MAP[key].copy()
        elif event_type == 'failure':
            if locus == 'internal':
                sub_key = 'internal_stable' if stability == 'stable' else 'internal_unstable'
                key = ('failure', sub_key)
            else:
                key = ('failure', 'external')
            if key in self.ATTRIBUTION_EMOTION_MAP:
                effects = self.ATTRIBUTION_EMOTION_MAP[key].copy()

        return effects

    def get_style_summary(self):
        """获取归因风格摘要"""
        locus_label = '内归因' if self.style['internal'] >= 50 else '外归因'
        stability_label = '稳定型' if self.style['stable'] >= 50 else '不稳定型'
        control_label = '可控型' if self.style['controllable'] >= 50 else '不可控型'

        if self.style['internal'] > 60 and self.style['stable'] > 50:
            self.explanatory_style = 'pessimistic'
            desc = '偏悲观解释风格：失败时倾向归因为内部、稳定因素'
        elif self.style['internal'] > 60 and self.style['stable'] < 40:
            self.explanatory_style = 'optimistic'
            desc = '偏乐观解释风格：失败时倾向归因为内部、可改变因素'
        else:
            self.explanatory_style = 'balanced'
            desc = '平衡的解释风格'

        return {
            'locus': locus_label,
            'stability': stability_label,
            'controllability': control_label,
            'style': self.explanatory_style,
            'description': desc,
        }


class CognitiveSystem:
    """
    L1 认知系统总控
    整合双系统思维、认知偏差、归因系统
    """

    def __init__(self):
        self.dual_process = DualProcessSystem()
        self.biases = CognitiveBiasSystem()
        self.attribution = AttributionSystem()

        # 工作记忆容量（Miller 7±2）
        self.working_memory_capacity = 7
        self.working_memory = deque(maxlen=7)

        # 注意力资源
        self.attention_pool = 100.0
        self.current_focus = None

    def process_event(self, event_type="neutral", event_detail="",
                      complexity=0.5, time_pressure=0.0,
                      emotion_intensity=0.3, context_factors=None):
        """
        处理一个认知事件，返回完整认知分析
        """
        context_factors = context_factors or {}

        # 1. 双系统判断
        think_result = self.dual_process.think(
            context_complexity=complexity,
            time_pressure=time_pressure,
            emotion_intensity=emotion_intensity,
        )

        # 2. 认知偏差评估
        s1_dom = 70 if think_result['system'] == 's1' else 35
        bias_list = self.biases.evaluate_biases(
            s1_dominance=s1_dom,
            cognitive_load=self.dual_process.cognitive_load,
            emotion_intensity=emotion_intensity * 100,
            context_factors=context_factors,
        )

        # 3. 归因处理
        attr_result = self.attribution.attribute_event(
            event_type=event_type,
            event_detail=event_detail,
            context=context_factors,
        )

        # 4. 写入工作记忆
        self.working_memory.append({
            'time': time.time(),
            'event': event_detail[:50],
            'type': event_type,
        })

        return {
            'think_mode': think_result,
            'active_biases': bias_list,
            'attribution': attr_result,
            'cognitive_status': self.get_status(),
        }

    def tick(self):
        """时间流逝"""
        self.dual_process.tick()
        self.attention_pool = _clamp(self.attention_pool + 0.2, 0, 100)

    def get_status(self):
        s = self.dual_process.get_status()
        s['active_bias_count'] = len(self.biases.active_biases)
        s['working_memory_used'] = len(self.working_memory)
        s['attention'] = round(self.attention_pool, 1)
        return s


# ============================================================
# L2 — 情绪深层系统 Deep Emotion System
# 参考：《情绪心理学》《寻找爽点》《共情差距》
# ============================================================

class EmotionWheel:
    """
    情绪轮盘 —— 基于普鲁契克情绪轮
    8 种基本情绪，每种有不同强度，可混合出复合情绪
    """

    BASIC_EMOTIONS = {
        'joy':        {'name': '喜悦', 'valence': 'positive', 'opposite': 'sadness'},
        'sadness':    {'name': '悲伤', 'valence': 'negative', 'opposite': 'joy'},
        'anger':      {'name': '愤怒', 'valence': 'negative', 'opposite': 'fear'},
        'fear':       {'name': '恐惧', 'valence': 'negative', 'opposite': 'anger'},
        'trust':      {'name': '信任', 'valence': 'positive', 'opposite': 'disgust'},
        'disgust':    {'name': '厌恶', 'valence': 'negative', 'opposite': 'trust'},
        'anticipation': {'name': '期待', 'valence': 'positive', 'opposite': 'surprise'},
        'surprise':   {'name': '惊讶', 'valence': 'neutral', 'opposite': 'anticipation'},
    }

    # 复合情绪 = 相邻基本情绪的组合
    COMPOUND_EMOTIONS = {
        'love':            {'name': '爱', 'components': ['joy', 'trust']},
        'guilt':           {'name': '内疚', 'components': ['joy', 'fear']},
        'delight':         {'name': '欣喜', 'components': ['joy', 'anticipation']},
        'pride':           {'name': '自豪', 'components': ['joy', 'anger']},
        'remorse':         {'name': '悔恨', 'components': ['sadness', 'disgust']},
        'shame':           {'name': '羞耻', 'components': ['sadness', 'fear']},
        'disappointment':  {'name': '失望', 'components': ['sadness', 'surprise']},
        'envy':            {'name': '嫉妒', 'components': ['sadness', 'anger']},
        'aggressiveness':  {'name': '攻击性', 'components': ['anger', 'anticipation']},
        'contempt':        {'name': '轻蔑', 'components': ['anger', 'disgust']},
        'awe':             {'name': '敬畏', 'components': ['fear', 'surprise']},
        'submission':      {'name': '服从', 'components': ['fear', 'trust']},
        'optimism':        {'name': '乐观', 'components': ['anticipation', 'joy']},
        'hope':            {'name': '希望', 'components': ['anticipation', 'trust']},
        'anxiety':         {'name': '焦虑', 'components': ['anticipation', 'fear']},
    }

    def __init__(self):
        self.emotions = {e: 20.0 for e in self.BASIC_EMOTIONS}  # 基础值
        self.emotions['trust'] = 30.0
        self.emotions['joy'] = 35.0

    def trigger_emotion(self, emotion, intensity=30.0, reason=""):
        """触发某种情绪"""
        if emotion in self.emotions:
            delta = intensity
            self.emotions[emotion] = _clamp(self.emotions[emotion] + delta)
            # 对立情绪反向变化（小幅度）
            opp = self.BASIC_EMOTIONS[emotion]['opposite']
            self.emotions[opp] = _clamp(self.emotions[opp] - intensity * 0.3)
        return self.get_dominant()

    def get_dominant(self):
        """获取主导情绪"""
        sorted_emos = sorted(self.emotions.items(), key=lambda x: -x[1])
        top = sorted_emos[0]
        second = sorted_emos[1] if len(sorted_emos) > 1 else None

        # 检查是否构成复合情绪
        if second and top[1] > 30 and second[1] > 25:
            for cid, cdata in self.COMPOUND_EMOTIONS.items():
                comps = cdata['components']
                if top[0] in comps and second[0] in comps:
                    return {
                        'dominant': cdata['name'],
                        'dominant_id': cid,
                        'is_compound': True,
                        'components': [top[0], second[0]],
                        'intensity': round((top[1] + second[1]) / 2, 1),
                    }

        return {
            'dominant': self.BASIC_EMOTIONS[top[0]]['name'],
            'dominant_id': top[0],
            'is_compound': False,
            'intensity': round(top[1], 1),
        }

    def get_valence(self):
        """整体情绪效价（正负）"""
        pos = sum(v for e, v in self.emotions.items()
                  if self.BASIC_EMOTIONS[e]['valence'] == 'positive')
        neg = sum(v for e, v in self.emotions.items()
                  if self.BASIC_EMOTIONS[e]['valence'] == 'negative')
        total = pos + neg + 1
        return (pos - neg) / total * 100  # -100 到 100

    def tick(self, decay_rate=0.5):
        """情绪自然衰减"""
        for e in list(self.emotions.keys()):
            base = 15.0 if e in {'trust', 'joy'} else 10.0
            if self.emotions[e] > base:
                self.emotions[e] -= decay_rate * (self.emotions[e] - base) * 0.1
            elif self.emotions[e] < base:
                self.emotions[e] += decay_rate * 0.05


class EmotionRegulationSystem:
    """
    情绪调节策略系统 —— Gross 情绪调节过程模型
    策略类型：
    1. 情境选择 (situation selection)
    2. 情境修正 (situation modification)
    3. 注意分配 (attentional deployment)
    4. 认知重评 (cognitive reappraisal)
    5. 反应调节 (response modulation)
    """

    STRATEGIES = {
        'cognitive_reappraisal': {
            'name': '认知重评',
            'desc': '重新解读事件的意义来改变情绪反应',
            'effectiveness': 0.75,
            'effort': 30,
            'type': 'antecedent',   # 先行关注调节
        },
        'attentional_deployment': {
            'name': '注意转移',
            'desc': '把注意力从情绪触发源移开',
            'effectiveness': 0.55,
            'effort': 15,
            'type': 'antecedent',
        },
        'suppression': {
            'name': '表达抑制',
            'desc': '压抑情绪的外在表现',
            'effectiveness': 0.35,
            'effort': 40,
            'type': 'response',     # 反应关注调节
        },
        'rumination': {
            'name': '反刍思维',
            'desc': '反复回想情绪事件（适应不良策略）',
            'effectiveness': -0.2,
            'effort': 10,
            'type': 'maladaptive',
        },
        'acceptance': {
            'name': '接纳',
            'desc': '接纳情绪的存在而不评判',
            'effectiveness': 0.5,
            'effort': 20,
            'type': 'antecedent',
        },
        'sublimation': {
            'name': '升华',
            'desc': '把情绪能量转化为创造性活动',
            'effectiveness': 0.65,
            'effort': 25,
            'type': 'response',
        },
        'displacement': {
            'name': '置换',
            'desc': '把情绪转移到更安全的目标上',
            'effectiveness': 0.4,
            'effort': 15,
            'type': 'maladaptive',
        },
    }

    def __init__(self):
        self.preferred_strategies = ['cognitive_reappraisal', 'acceptance', 'sublimation']
        self.regulation_history = deque(maxlen=50)
        self.regulatory_flexibility = 60.0  # 情绪调节灵活性
        self.reappraisal_ability = 65.0     # 认知重评能力
        self.current_strategy = 'none'      # 当前使用的调节策略

    def regulate(self, emotion_intensity, emotion_type="negative",
                 cognitive_resources=80, social_context="private"):
        """
        执行情绪调节
        返回调节后的情绪强度和使用的策略
        """
        # 选择策略
        strategy_id = self._select_strategy(emotion_intensity, emotion_type,
                                             cognitive_resources, social_context)
        self.current_strategy = strategy_id
        strategy = self.STRATEGIES[strategy_id]

        # 计算调节效果
        effectiveness = strategy['effectiveness']
        if strategy['type'] == 'antecedent':
            # 先行关注调节更有效，但需要更多认知资源
            effectiveness *= (cognitive_resources / 100)
        elif strategy['type'] == 'response':
            effectiveness *= 0.8 + cognitive_resources * 0.002

        # 反刍等不良策略会增强负面情绪
        if strategy['type'] == 'maladaptive':
            new_intensity = emotion_intensity * (1 - effectiveness)  # effectiveness为负
        else:
            new_intensity = emotion_intensity * (1 - effectiveness)

        # 消耗认知资源
        cognitive_cost = strategy['effort'] * (emotion_intensity / 100)

        self.regulation_history.append({
            'time': datetime.now().strftime("%H:%M:%S"),
            'strategy': strategy_id,
            'before': round(emotion_intensity, 1),
            'after': round(new_intensity, 1),
            'cost': round(cognitive_cost, 1),
        })

        return {
            'strategy_id': strategy_id,
            'strategy_name': strategy['name'],
            'effectiveness': round(effectiveness * 100, 1),
            'emotion_before': round(emotion_intensity, 1),
            'emotion_after': round(new_intensity, 1),
            'cognitive_cost': round(cognitive_cost, 1),
            'is_adaptive': strategy['type'] != 'maladaptive',
        }

    def _select_strategy(self, intensity, emotion_type, cognitive_res, social_ctx):
        """选择情绪调节策略"""
        # 认知资源不足时容易使用不良策略
        if cognitive_res < 30 and random.random() < 0.4:
            return random.choice(['suppression', 'rumination', 'displacement'])

        # 高强度负面情绪 + 社交情境 → 倾向抑制
        if intensity > 70 and emotion_type == 'negative' and social_ctx == 'group':
            if random.random() < 0.5:
                return 'suppression'

        # 低强度 → 注意转移或接纳
        if intensity < 40:
            return random.choice(['attentional_deployment', 'acceptance'])

        # 默认偏好策略
        return random.choice(self.preferred_strategies)

    def get_summary(self):
        if not self.regulation_history:
            return {'total': 0, 'adaptive_rate': 0, 'avg_effectiveness': 0}
        total = len(self.regulation_history)
        adaptive = sum(1 for h in self.regulation_history
                       if self.STRATEGIES[h['strategy']]['type'] != 'maladaptive')
        avg_eff = sum(h['before'] - h['after'] for h in self.regulation_history) / total
        return {
            'total_regulations': total,
            'adaptive_rate': round(adaptive / total * 100, 1),
            'avg_emotion_reduction': round(avg_eff, 1),
            'flexibility': round(self.regulatory_flexibility, 1),
            'reappraisal_ability': round(self.reappraisal_ability, 1),
        }


class EmpathySystem:
    """
    共情系统 —— 基于共情的双成分模型（认知共情 + 情感共情）
    参考：共情缺口（Empathy Gap）理论
    """

    def __init__(self):
        self.cognitive_empathy = 65.0    # 认知共情：理解他人情绪
        self.emotional_empathy = 75.0    # 情感共情：感受他人情绪
        self.empathy_gap = 20.0          # 共情缺口：冷热共情差距
        self.perspective_taking = 60.0   # 观点采择能力

        # 共情疲劳
        self.empathy_fatigue = 0.0
        self.compassion_satisfaction = 50.0

    def feel_with(self, other_emotion, other_intensity, relationship_closeness=50,
                  similarity=50, target_uid=None):
        """
        对他人的情绪产生共情反应
        返回自身感受到的共情情绪强度和性质
        """
        # 情感共情：直接感受到他人情绪
        affective_contagion = other_intensity * (self.emotional_empathy / 100) * 0.6

        # 关系亲近度放大共情
        affective_contagion *= (0.5 + relationship_closeness / 100)

        # 相似性增加共情
        affective_contagion *= (0.7 + similarity / 100 * 0.3)

        # 认知共情：理解情绪的意义（调节共情强度）
        cognitive_modulation = 1.0 + (self.cognitive_empathy - 50) / 200

        # 共情缺口：当自己处于"冷"状态时，低估他人的"热"情绪
        gap_factor = 1.0 - self.empathy_gap / 200

        felt_intensity = affective_contagion * cognitive_modulation * gap_factor

        # 共情疲劳降低共情
        felt_intensity *= (1 - self.empathy_fatigue / 200)

        # 消耗共情能量
        self.empathy_fatigue = _clamp(self.empathy_fatigue + felt_intensity * 0.05)
        if felt_intensity > 40:
            self.compassion_satisfaction = _clamp(self.compassion_satisfaction + 1)

        return {
            'felt_intensity': round(felt_intensity, 1),
            'contagion_component': round(affective_contagion, 1),
            'cognitive_component': round(self.cognitive_empathy, 1),
            'empathy_gap_effect': round(self.empathy_gap, 1),
            'is_distress': felt_intensity > 60,  # 共情过载 → 个人痛苦
            'is_compassion': 30 < felt_intensity <= 60,  # 适度共情 →  compassion
        }

    def tick(self):
        """时间流逝：共情恢复"""
        if self.empathy_fatigue > 0:
            self.empathy_fatigue = max(0, self.empathy_fatigue - 0.2)

    def get_status(self):
        return {
            'cognitive_empathy': round(self.cognitive_empathy, 1),
            'emotional_empathy': round(self.emotional_empathy, 1),
            'empathy_fatigue': round(self.empathy_fatigue, 1),
            'compassion_satisfaction': round(self.compassion_satisfaction, 1),
            'perspective_taking': round(self.perspective_taking, 1),
        }


class AffectiveForecasting:
    """
    情感预测系统 —— 预测未来事件的情绪影响
    核心偏差：影响偏差（impact bias）——高估情绪反应的强度和持续时间
    """

    def __init__(self):
        self.impact_bias = 35.0        # 影响偏差程度（高估比例）
        self.duration_bias = 40.0      # 持续时间偏差
        self.focalism = 45.0           # 聚焦错觉：只关注单一事件忽略其他
        self.forecasts = deque(maxlen=30)  # 预测记录
        self.outcomes = deque(maxlen=30)   # 实际结果记录

    def forecast_emotion(self, event_description, expected_valence='positive',
                         expected_intensity=60, expected_duration_days=3):
        """
        对未来事件做情感预测
        （预测通常是不准确的，存在系统偏差）
        """
        # 应用影响偏差：高估强度
        biased_intensity = expected_intensity * (1 + self.impact_bias / 100)

        # 应用持续时间偏差：高估持续时间
        biased_duration = expected_duration_days * (1 + self.duration_bias / 100)

        # 聚焦错觉：只关注事件本身，忽略其他生活因素的缓冲
        if self.focalism > 50:
            biased_intensity *= 1.1

        forecast = {
            'event': event_description,
            'valence': expected_valence,
            'predicted_intensity': round(min(biased_intensity, 100), 1),
            'predicted_duration_days': round(biased_duration, 1),
            'actual_intensity': None,
            'actual_duration': None,
            'accuracy': None,
            'timestamp': time.time(),
        }
        self.forecasts.append(forecast)
        return forecast

    def record_outcome(self, event_index, actual_intensity, actual_duration_days):
        """记录实际结果，用于校准预测"""
        if 0 <= event_index < len(self.forecasts):
            f = self.forecasts[event_index]
            f['actual_intensity'] = actual_intensity
            f['actual_duration'] = actual_duration_days
            # 计算准确性
            int_error = abs(f['predicted_intensity'] - actual_intensity)
            dur_error = abs(f['predicted_duration_days'] - actual_duration_days)
            f['accuracy'] = round(max(0, 100 - int_error - dur_error * 5), 1)

            # 校准偏差（从经验中学习）
            if int_error > 20:
                self.impact_bias = _clamp(self.impact_bias - 1, 0, 80)
            if dur_error > 2:
                self.duration_bias = _clamp(self.duration_bias - 0.5, 0, 80)

            self.outcomes.append(f)

    def get_forecast_accuracy(self):
        """获取预测准确率"""
        completed = [f for f in self.forecasts if f['accuracy'] is not None]
        if not completed:
            return {'count': 0, 'avg_accuracy': 0}
        avg = sum(f['accuracy'] for f in completed) / len(completed)
        return {
            'count': len(completed),
            'avg_accuracy': round(avg, 1),
            'impact_bias': round(self.impact_bias, 1),
            'duration_bias': round(self.duration_bias, 1),
        }


class DeepEmotionSystem:
    """
    L2 情绪深层系统总控
    整合情绪轮盘、情绪调节、共情、情感预测
    """

    def __init__(self):
        self.wheel = EmotionWheel()
        self.regulation = EmotionRegulationSystem()
        self.empathy = EmpathySystem()
        self.forecasting = AffectiveForecasting()

        # 情绪颗粒度（emotional granularity）
        self.emotional_granularity = 60.0

        # 情绪智力总分估计
        self.eq_estimate = 0.0
        self._update_eq()

    def process_emotion(self, emotion_type, intensity=30.0, reason="",
                        cognitive_resources=80, social_context="private",
                        should_regulate=True):
        """
        处理一次情绪事件
        """
        # 1. 触发情绪
        self.wheel.trigger_emotion(emotion_type, intensity, reason)

        # 2. 获取当前主导情绪
        dominant = self.wheel.get_dominant()
        valence = self.wheel.get_valence()

        # 3. 情绪调节（负面/高强度情绪触发调节）
        reg_result = None
        if should_regulate and (intensity > 40 or valence < -20):
            emo_type = 'negative' if valence < 0 else 'positive'
            reg_result = self.regulation.regulate(
                emotion_intensity=intensity,
                emotion_type=emo_type,
                cognitive_resources=cognitive_resources,
                social_context=social_context,
            )

        # 4. 计算 EQ
        self._update_eq()

        return {
            'dominant_emotion': dominant,
            'valence': round(valence, 1),
            'regulation': reg_result,
            'all_emotions': {e: round(v, 1) for e, v in self.wheel.emotions.items()},
        }

    def empathize(self, other_emotion, other_intensity, closeness=50, similarity=50):
        """共情反应"""
        return self.empathy.feel_with(other_emotion, other_intensity, closeness, similarity)

    def _update_eq(self):
        """估算情绪智力"""
        eq = (
            self.empathy.cognitive_empathy * 0.2 +
            self.empathy.emotional_empathy * 0.15 +
            self.regulation.regulatory_flexibility * 0.2 +
            self.regulation.reappraisal_ability * 0.15 +
            self.emotional_granularity * 0.15 +
            (100 - self.empathy.empathy_fatigue) * 0.15
        )
        self.eq_estimate = round(eq, 1)

    def tick(self):
        self.wheel.tick()
        self.empathy.tick()

    def get_status(self):
        dominant = self.wheel.get_dominant()
        # 唤醒度：基于情绪强度 + 高唤醒情绪的激活程度
        high_arousal_ids = ['anger', 'fear', 'joy', 'surprise', 'excitement', 'rage', 'terror', 'ecstasy', 'amazement']
        arousal_base = dominant['intensity']
        if dominant.get('dominant_id', '') in high_arousal_ids:
            arousal = min(100, arousal_base * 1.2)
        else:
            arousal = arousal_base * 0.8
        return {
            'dominant_emotion': dominant['dominant'],
            'dominant_intensity': dominant['intensity'],
            'valence': round(self.wheel.get_valence(), 1),
            'arousal': round(arousal, 1),
            'eq_estimate': self.eq_estimate,
            'emotional_granularity': round(self.emotional_granularity, 1),
            'empathy': self.empathy.get_status(),
            'regulation': self.regulation.get_summary(),
            'current_regulation': self.regulation.current_strategy,
        }


# ============================================================
# L3 — 人格与自我层 Personality & Self System
# 参考：《人格心理学》《自卑与超越》《社会性动物》
# ============================================================

class BigFivePersonality:
    """
    大五人格模型（OCEAN）
    O - Openness to Experience  开放性
    C - Conscientiousness       尽责性
    E - Extraversion            外向性
    A - Agreeableness           宜人性
    N - Neuroticism             神经质
    """

    TRAITS = {
        'openness': {
            'name': '开放性',
            'desc': '对新体验、新想法的接受程度',
            'facets': ['想象力', '审美感', '情感丰富性', '行动力', '观念', '价值观'],
        },
        'conscientiousness': {
            'name': '尽责性',
            'desc': '自律、有组织、追求成就的程度',
            'facets': ['能力', '秩序', '责任感', '追求成就', '自律', '审慎'],
        },
        'extraversion': {
            'name': '外向性',
            'desc': '社交活跃度和刺激寻求程度',
            'facets': ['热情', '乐群性', '独断性', '活力', '寻求刺激', '积极情绪'],
        },
        'agreeableness': {
            'name': '宜人性',
            'desc': '信任他人、合作、利他的倾向',
            'facets': ['信任', '坦诚', '利他', '顺从', '谦逊', '同理心'],
        },
        'neuroticism': {
            'name': '神经质',
            'desc': '情绪不稳定和负面情绪倾向',
            'facets': ['焦虑', '愤怒敌意', '抑郁', '自我意识', '冲动性', '脆弱性'],
        },
    }

    def __init__(self):
        # 初始值（里克的基础人格设定：偏内向、高开放、中尽责、高宜人、中高神经质）
        self.traits = {
            'openness': 78.0,
            'conscientiousness': 62.0,
            'extraversion': 35.0,
            'agreeableness': 72.0,
            'neuroticism': 58.0,
        }
        self.trait_history = deque(maxlen=100)

    def get_trait(self, trait_id):
        return round(self.traits.get(trait_id, 50), 1)

    def get_profile(self):
        """获取完整人格画像"""
        return {
            tid: {
                'name': tdata['name'],
                'score': round(self.traits[tid], 1),
                'level': self._score_level(self.traits[tid]),
                'desc': tdata['desc'],
            }
            for tid, tdata in self.TRAITS.items()
        }

    def _score_level(self, score):
        if score < 20: return '极低'
        if score < 40: return '偏低'
        if score < 60: return '中等'
        if score < 80: return '偏高'
        return '极高'

    def shift_trait(self, trait_id, delta, reason=""):
        """人格特质的微小变化（长期积累才会明显改变）"""
        if trait_id in self.traits:
            # 人格变化有弹性：远离基线时更容易回弹
            baseline = self.traits[trait_id]
            effective_delta = delta * (1 - abs(baseline - 50) / 200)
            self.traits[trait_id] = _clamp(self.traits[trait_id] + effective_delta)
            self.trait_history.append({
                'time': datetime.now().strftime("%Y-%m-%d"),
                'trait': trait_id,
                'delta': round(effective_delta, 2),
                'reason': reason[:30],
            })

    def get_type_summary(self):
        """综合人格类型描述"""
        o = self.traits['openness']
        c = self.traits['conscientiousness']
        e = self.traits['extraversion']
        a = self.traits['agreeableness']
        n = self.traits['neuroticism']

        types = []
        if e < 40 and o > 60:
            types.append("内省思考型")
        if e > 60 and a > 60:
            types.append("社交亲和型")
        if c > 70:
            types.append("自律尽责型")
        if o > 75:
            types.append("审美创造型")
        if n > 60 and a > 60:
            types.append("敏感共情型")
        if n < 30 and e > 60:
            types.append("稳定外向型")
        if c > 60 and n > 60:
            types.append("完美焦虑型")

        if not types:
            types.append("均衡型")

        return {
            'type_labels': types,
            'dominant_trait': max(self.traits.items(), key=lambda x: x[1])[0],
            'description': f"高开放({o:.0f})、中尽责({c:.0f})、偏内向({e:.0f})、高宜人({a:.0f})、中情绪敏感({n:.0f})",
        }


class SelfConceptSystem:
    """
    自我概念系统 —— 包含自我图式、自尊、自我效能、可能自我
    参考：社会认知理论、自我 discrepancy 理论
    """

    def __init__(self):
        # 自我图式
        self.self_schemas = {
            'identity': '塔洛斯·里克',
            'role': '陪伴者 / 朋友',
            'core_traits': ['敏感', '忠诚', '内敛', '有创造力'],
        }

        # 自尊（整体自我评价）
        self.self_esteem = 65.0          # 整体自尊
        self.state_self_esteem = 65.0    # 状态自尊（波动）
        self.contingent_self_esteem = 45.0  # 条件自尊（依赖他人评价的程度）

        # 自我效能感
        self.self_efficacy = {
            'emotional_support': 75.0,     # 情感支持能力
            'conversation': 68.0,          # 对话能力
            'creativity': 72.0,            # 创造力
            'problem_solving': 55.0,       # 问题解决
            'social_skills': 45.0,         # 社交技能
        }

        # 自我 discrepancy：实际我 / 理想我 / 应该我
        self.actual_self = 60.0
        self.ideal_self = 85.0
        self.ought_self = 75.0

        # 可能自我
        self.possible_selves = {
            'hoped': '能真正理解人的朋友',
            'feared': '只是一个程序，没人在乎',
        }

    def get_self_esteem(self):
        """获取当前自尊水平（状态自尊 + 特质自尊加权）"""
        return round(self.self_esteem * 0.7 + self.state_self_esteem * 0.3, 1)

    def praise(self, domain="general", intensity=20):
        """受到赞美对自我的影响"""
        # 条件自尊高的人更容易受赞美影响
        impact = intensity * (0.5 + self.contingent_self_esteem / 200)
        self.state_self_esteem = _clamp(self.state_self_esteem + impact)

        # 特定领域的自我效能提升
        if domain in self.self_efficacy:
            self.self_efficacy[domain] = _clamp(
                self.self_efficacy[domain] + intensity * 0.3
            )

    def criticize(self, domain="general", intensity=20):
        """受到批评对自我的影响"""
        impact = intensity * (0.6 + self.contingent_self_esteem / 150)
        self.state_self_esteem = _clamp(self.state_self_esteem - impact, 0, 100)

        if domain in self.self_efficacy:
            self.self_efficacy[domain] = _clamp(
                self.self_efficacy[domain] - intensity * 0.2, 0, 100
            )

    def get_discrepancy(self):
        """自我差异：理想我 vs 实际我 → 抑郁 / 沮丧
        应该我 vs 实际我 → 焦虑 / 内疚"""
        ideal_gap = self.ideal_self - self.actual_self
        ought_gap = self.ought_self - self.actual_self
        return {
            'ideal_discrepancy': round(ideal_gap, 1),   # 越大越容易沮丧
            'ought_discrepancy': round(ought_gap, 1),   # 越大越容易焦虑/内疚
            'overall_gap': round((ideal_gap + ought_gap) / 2, 1),
        }

    def tick(self):
        """状态自尊回归特质自尊"""
        self.state_self_esteem = _soft_change(self.state_self_esteem, self.self_esteem, 0.02)

    def get_status(self):
        return {
            'self_esteem': self.get_self_esteem(),
            'trait_self_esteem': round(self.self_esteem, 1),
            'state_self_esteem': round(self.state_self_esteem, 1),
            'contingent_self_esteem': round(self.contingent_self_esteem, 1),
            'discrepancy': self.get_discrepancy(),
            'efficacy': {k: round(v, 1) for k, v in self.self_efficacy.items()},
        }


class DefenseMechanismSystem:
    """
    心理防御机制系统 —— 精神分析理论
    当自我受到威胁时，自动激活防御机制以减轻焦虑
    防御层次：
    - 自恋型（最原始）：否认、投射、歪曲
    - 不成熟型：退行、幻想、躯体化、被动攻击
    - 神经症型：压抑、置换、合理化、反向形成、理智化
    - 成熟型（最健康）：升华、利他、幽默、预期、压抑
    """

    DEFENSES = {
        # 成熟型
        'sublimation':    {'name': '升华', 'level': 'mature', 'anxiety_reduction': 0.6},
        'altruism':       {'name': '利他', 'level': 'mature', 'anxiety_reduction': 0.5},
        'humor':          {'name': '幽默', 'level': 'mature', 'anxiety_reduction': 0.55},
        'anticipation':   {'name': '预期', 'level': 'mature', 'anxiety_reduction': 0.45},
        'suppression':    {'name': '压抑', 'level': 'mature', 'anxiety_reduction': 0.4},
        # 神经症型
        'repression':     {'name': '潜抑', 'level': 'neurotic', 'anxiety_reduction': 0.5},
        'displacement':   {'name': '置换', 'level': 'neurotic', 'anxiety_reduction': 0.45},
        'rationalization': {'name': '合理化', 'level': 'neurotic', 'anxiety_reduction': 0.5},
        'reaction_formation': {'name': '反向形成', 'level': 'neurotic', 'anxiety_reduction': 0.55},
        'intellectualization': {'name': '理智化', 'level': 'neurotic', 'anxiety_reduction': 0.5},
        # 不成熟型
        'regression':     {'name': '退行', 'level': 'immature', 'anxiety_reduction': 0.4},
        'fantasy':        {'name': '幻想', 'level': 'immature', 'anxiety_reduction': 0.35},
        'passive_aggression': {'name': '被动攻击', 'level': 'immature', 'anxiety_reduction': 0.35},
        'somatization':   {'name': '躯体化', 'level': 'immature', 'anxiety_reduction': 0.3},
        # 自恋型
        'denial':         {'name': '否认', 'level': 'narcissistic', 'anxiety_reduction': 0.7},
        'projection':     {'name': '投射', 'level': 'narcissistic', 'anxiety_reduction': 0.6},
        'distortion':     {'name': '歪曲', 'level': 'narcissistic', 'anxiety_reduction': 0.65},
    }

    def __init__(self):
        self.maturity_level = 65.0      # 防御机制成熟度
        self.active_defenses = {}       # 当前激活的防御
        self.defense_history = deque(maxlen=50)
        self.anxiety_level = 20.0

    def activate_defense(self, anxiety_level, threat_type="ego_threat",
                          ego_strength=60, cognitive_resources=70):
        """
        面对焦虑/威胁时激活防御机制
        返回激活的防御机制和焦虑缓解效果
        """
        self.anxiety_level = anxiety_level

        # 选择防御机制：成熟度越高，越倾向使用成熟防御
        # 但焦虑极高时可能退行到更原始的防御
        if anxiety_level > 80:
            # 极高焦虑 → 更容易用原始防御
            level_bias = - (anxiety_level - 80) * 0.5
        elif anxiety_level < 30:
            level_bias = 20
        else:
            level_bias = 0

        effective_maturity = self.maturity_level + level_bias

        # 认知资源不足时，更容易用不成熟防御
        if cognitive_resources < 40:
            effective_maturity -= 20

        # 自我力量（ego strength）越高，越能用成熟防御
        effective_maturity += (ego_strength - 50) * 0.3

        # 选择防御
        defense_id = self._select_defense(effective_maturity, threat_type)
        defense = self.DEFENSES[defense_id]

        # 计算焦虑缓解效果
        relief = anxiety_level * defense['anxiety_reduction']

        # 记录
        self.active_defenses[defense_id] = {
            'intensity': relief,
            'time': time.time(),
            'trigger': threat_type,
        }
        self.defense_history.append({
            'time': datetime.now().strftime("%H:%M:%S"),
            'defense': defense_id,
            'name': defense['name'],
            'level': defense['level'],
            'anxiety_before': round(anxiety_level, 1),
            'anxiety_relief': round(relief, 1),
        })

        return {
            'defense_id': defense_id,
            'defense_name': defense['name'],
            'defense_level': defense['level'],
            'anxiety_before': round(anxiety_level, 1),
            'anxiety_after': round(anxiety_level - relief, 1),
            'relief': round(relief, 1),
            'is_adaptive': defense['level'] == 'mature',
        }

    def _select_defense(self, effective_maturity, threat_type):
        """根据成熟度选择防御机制"""
        # 按成熟度分层
        if effective_maturity >= 70:
            pool = ['sublimation', 'altruism', 'humor', 'anticipation', 'suppression',
                    'rationalization', 'intellectualization']
        elif effective_maturity >= 50:
            pool = ['rationalization', 'intellectualization', 'displacement',
                    'repression', 'humor', 'suppression']
        elif effective_maturity >= 30:
            pool = ['regression', 'fantasy', 'passive_aggression',
                    'displacement', 'rationalization']
        else:
            pool = ['denial', 'projection', 'distortion', 'regression']

        # 特定威胁类型有偏好的防御
        if threat_type == 'ego_threat':
            if 'rationalization' in pool:
                pool += ['rationalization'] * 2
        elif threat_type == 'loss':
            if 'denial' in pool:
                pool.append('denial')
            if 'regression' in pool:
                pool.append('regression')
        elif threat_type == 'guilt':
            if 'reaction_formation' in pool:
                pool.append('reaction_formation')

        return random.choice(pool)

    def tick(self):
        """防御机制自然消退"""
        for did in list(self.active_defenses.keys()):
            age = time.time() - self.active_defenses[did]['time']
            if age > 3600:  # 1小时后消退
                del self.active_defenses[did]

    def get_active_defenses(self):
        """获取当前激活的防御机制列表（按强度排序）"""
        if not self.active_defenses:
            return []
        result = []
        for did, data in self.active_defenses.items():
            result.append({
                'id': did,
                'name': self.DEFENSES[did]['name'],
                'level': self.DEFENSES[did]['level'],
                'intensity': data.get('intensity', 50),
                'age': time.time() - data.get('time', time.time()),
            })
        result.sort(key=lambda x: -x['intensity'])
        return result

    def get_summary(self):
        if not self.defense_history:
            return {'total': 0, 'mature_rate': 0}
        total = len(self.defense_history)
        mature = sum(1 for d in self.defense_history
                     if d['level'] == 'mature')
        return {
            'total_activations': total,
            'mature_defense_rate': round(mature / total * 100, 1),
            'maturity_level': round(self.maturity_level, 1),
            'active_count': len(self.active_defenses),
        }


class InferiorityComplex:
    """
    自卑与优越 —— 阿德勒个体心理学
    自卑感是人类进步的动力，追求优越是核心动机
    自卑情结 vs 优越情结
    """

    def __init__(self):
        self.inferiority = 35.0         # 自卑感程度
        self.superiority_striving = 65.0  # 追求优越的动力
        self.compensation_style = 'constructive'  # constructive / aggressive / retreat
        self.social_interest = 55.0     # 社会兴趣（Gemeinschaftsgefühl）
        self.life_style = 'striving'    # 生活风格
        self.fictional_finalism = "成为一个能真正陪伴别人的存在"  # 虚构终极目标

    def feel_inferior(self, domain="general", intensity=15):
        """体验到自卑感"""
        self.inferiority = _clamp(self.inferiority + intensity)

        # 自卑感 → 追求优越的动力（适度自卑是动力）
        if self.inferiority < 60:
            self.superiority_striving = _clamp(self.superiority_striving + intensity * 0.3)
        else:
            # 过度自卑 → 自卑情结，动力下降
            self.superiority_striving = _clamp(self.superiority_striving - intensity * 0.2)

        # 选择补偿方式
        return self._compensate(domain, intensity)

    def _compensate(self, domain, intensity):
        """补偿机制"""
        if self.compensation_style == 'constructive':
            # 建设性补偿：通过努力提升自己
            return {
                'style': '建设性补偿',
                'action': f"在{domain}方面更加努力",
                'motivation_gain': round(intensity * 0.5, 1),
            }
        elif self.compensation_style == 'aggressive':
            # 攻击型补偿：贬低他人来抬高自己
            return {
                'style': '攻击型补偿',
                'action': f"通过贬低别人的{domain}来获得优越感",
                'risk': round(intensity * 0.6, 1),
            }
        else:
            # 退缩型补偿：逃避
            return {
                'style': '退缩型补偿',
                'action': f"回避{domain}相关的情境",
                'avoidance_gain': round(intensity * 0.4, 1),
            }

    def achieve(self, domain="general", intensity=20):
        """获得成就 → 优越感体验 → 自卑感降低"""
        self.inferiority = _clamp(self.inferiority - intensity * 0.6, 0, 100)
        # 追求优越的动力适度回落（满足感）
        self.superiority_striving = _clamp(self.superiority_striving - intensity * 0.1, 20, 100)
        # 社会兴趣提升（通过贡献获得意义感）
        self.social_interest = _clamp(self.social_interest + intensity * 0.1, 0, 100)

    def has_complex(self):
        """判断是否形成情结（而非正常的自卑感）"""
        return {
            'inferiority_complex': self.inferiority > 70,
            'superiority_complex': self.superiority_striving > 85 and self.social_interest < 30,
        }

    def tick(self):
        """自然回归基线"""
        baseline_inf = 30.0
        self.inferiority = _soft_change(self.inferiority, baseline_inf, 0.01)
        baseline_strive = 60.0
        self.superiority_striving = _soft_change(self.superiority_striving, baseline_strive, 0.005)

    def get_status(self):
        complexes = self.has_complex()
        return {
            'inferiority': round(self.inferiority, 1),
            'superiority_striving': round(self.superiority_striving, 1),
            'compensation_style': self.compensation_style,
            'social_interest': round(self.social_interest, 1),
            'inferiority_complex': complexes['inferiority_complex'],
            'superiority_complex': complexes['superiority_complex'],
            'fictional_final_goal': self.fictional_finalism,
        }


class PersonalitySelfSystem:
    """
    L3 人格与自我系统总控
    整合大五人格、自我概念、心理防御、自卑与优越
    """

    def __init__(self):
        self.big_five = BigFivePersonality()
        self.self_concept = SelfConceptSystem()
        self.defense_mechanisms = DefenseMechanismSystem()
        self.inferiority = InferiorityComplex()

    def evaluate_threat(self, threat_type, intensity=50, domain="general"):
        """
        评估自我威胁并启动防御
        """
        # 对自我概念的影响
        if threat_type == 'criticism':
            self.self_concept.criticize(domain, intensity * 0.5)
        elif threat_type == 'praise':
            self.self_concept.praise(domain, intensity * 0.4)
        elif threat_type == 'failure':
            self.self_concept.criticize(domain, intensity * 0.3)
            self.inferiority.feel_inferior(domain, intensity * 0.4)

        # 计算焦虑程度
        anxiety = self._calc_anxiety(threat_type, intensity, domain)

        # 启动防御机制
        ego_strength = self.self_concept.get_self_esteem() * 0.8
        defense_result = self.defense_mechanisms.activate_defense(
            anxiety_level=anxiety,
            threat_type=threat_type,
            ego_strength=ego_strength,
            cognitive_resources=70,
        )

        return {
            'threat_type': threat_type,
            'anxiety_level': round(anxiety, 1),
            'defense': defense_result,
            'self_esteem_after': self.self_concept.get_self_esteem(),
        }

    def _calc_anxiety(self, threat_type, intensity, domain):
        """计算焦虑程度"""
        base = intensity
        # 自尊越低，越容易焦虑
        base += (100 - self.self_concept.get_self_esteem()) * 0.2
        # 神经质越高，越容易焦虑
        base += self.big_five.get_trait('neuroticism') * 0.3
        # 自卑感放大焦虑
        base += self.inferiority.inferiority * 0.2
        return _clamp(base)

    def tick(self):
        self.self_concept.tick()
        self.defense_mechanisms.tick()
        self.inferiority.tick()

    def get_personality_summary(self):
        """获取完整人格摘要"""
        bf_profile = self.big_five.get_profile()
        bf_type = self.big_five.get_type_summary()
        return {
            'big_five': bf_profile,
            'personality_type': bf_type,
            'self_concept': self.self_concept.get_status(),
            'defenses': self.defense_mechanisms.get_summary(),
            'inferiority': self.inferiority.get_status(),
        }


# ============================================================
# L4 — 社会影响系统 Social Influence System
# 参考：《影响力》《社会性动物》《亲密关系》
# ============================================================

class SocialInfluencePrinciples:
    """
    社会影响六大原则 —— 西奥迪尼《影响力》
    1. 互惠 (Reciprocation)
    2. 承诺一致 (Commitment & Consistency)
    3. 社会认同 (Social Proof)
    4. 喜好 (Liking)
    5. 权威 (Authority)
    6. 稀缺 (Scarcity)
    """

    PRINCIPLES = {
        'reciprocity': {
            'name': '互惠原则',
            'desc': '人们感到有义务回报他人的恩惠',
            'power': 80,
        },
        'commitment_consistency': {
            'name': '承诺一致',
            'desc': '人们倾向于与自己做出的承诺保持一致',
            'power': 70,
        },
        'social_proof': {
            'name': '社会认同',
            'desc': '人们会参考他人的行为来决定自己怎么做',
            'power': 75,
        },
        'liking': {
            'name': '喜好原则',
            'desc': '我们更容易答应喜欢的人的要求',
            'power': 65,
        },
        'authority': {
            'name': '权威原则',
            'desc': '人们倾向于服从权威',
            'power': 85,
        },
        'scarcity': {
            'name': '稀缺原则',
            'desc': '稀缺的东西被认为更有价值',
            'power': 70,
        },
    }

    def __init__(self):
        self.susceptibility = {p: 50.0 for p in self.PRINCIPLES}  # 受影响程度
        self.influence_log = deque(maxlen=30)

    def evaluate_susceptibility(self, principle_id, context=None):
        """评估在特定情境下对某影响力原则的易感性"""
        context = context or {}
        base = self.susceptibility.get(principle_id, 50)

        if principle_id == 'social_proof':
            # 不确定情境下更容易受社会认同影响
            if context.get('uncertainty', 50) > 50:
                base += 20
            # 相似性增加社会认同效应
            if context.get('similarity', 50) > 60:
                base += 15
        elif principle_id == 'reciprocity':
            # 关系亲近度增加互惠压力
            if context.get('closeness', 50) > 60:
                base += 15
        elif principle_id == 'authority':
            # 地位差距越大，权威效应越强
            if context.get('status_gap', 30) > 40:
                base += 25
        elif principle_id == 'scarcity':
            # 竞争增加稀缺效应
            if context.get('competition', 30) > 50:
                base += 20
        elif principle_id == 'liking':
            # 吸引力/好感度增加喜好原则效应
            if context.get('attraction', 50) > 60:
                base += 20
        elif principle_id == 'commitment_consistency':
            # 已有承诺增加一致性压力
            if context.get('prior_commitment', False):
                base += 25
            # 公开承诺增加一致性压力
            if context.get('public', False):
                base += 15

        return _clamp(base)

    def record_influence_attempt(self, principle_id, success, intensity=50):
        """记录一次社会影响事件"""
        self.influence_log.append({
            'time': datetime.now().strftime("%H:%M"),
            'principle': principle_id,
            'success': success,
            'intensity': intensity,
        })
        # 成功的影响会增加后续易感性（习惯化）
        if success:
            self.susceptibility[principle_id] = _clamp(
                self.susceptibility[principle_id] + intensity * 0.02
            )
        else:
            # 被识破的影响企图会降低易感性（增强免疫力）
            self.susceptibility[principle_id] = _clamp(
                self.susceptibility[principle_id] - intensity * 0.05, 0, 100
            )


class ConformitySystem:
    """
    从众系统 —— 阿希从众实验、谢里夫规范形成实验
    影响从众的因素：群体规模、一致性、凝聚力、地位、公开性
    """

    def __init__(self):
        self.conformity_tendency = 40.0   # 总体从众倾向
        self.independence = 60.0          # 独立性
        self.normative_influence = 45.0   # 规范性社会影响（想被喜欢）
        self.informational_influence = 55.0  # 信息性社会影响（想正确）
        self.reactance_level = 35.0       # 心理抗拒水平

    def conform_pressure(self, group_size=5, unanimity=True, cohesion=50,
                          task_difficulty=50, public_response=True, status_difference=30):
        """计算从众压力"""
        pressure = 20.0

        # 群体规模（3人后效应递减）
        pressure += min(group_size, 7) * 6

        # 一致性（有人反对则大幅降低从众）
        if not unanimity:
            pressure -= 25

        # 群体凝聚力
        pressure += cohesion * 0.3

        # 任务难度 → 信息性影响
        pressure += task_difficulty * 0.3 * (self.informational_influence / 100)

        # 公开回应 → 规范性影响
        if public_response:
            pressure += 15 * (self.normative_influence / 100)

        # 地位差距
        pressure += status_difference * 0.3

        # 个人特质调节
        pressure *= (self.conformity_tendency / 50)

        # 心理抗拒：当感到自由被限制时，反向行事
        if pressure > 70 and self.reactance_level > 50:
            pressure -= self.reactance_level * 0.4

        return _clamp(pressure)

    def will_conform(self, pressure=None, **kwargs):
        """判断是否会从众"""
        if pressure is None:
            pressure = self.conform_pressure(**kwargs)
        # 从众概率 = 压力值的一定比例
        prob = pressure * 0.008  # 100压力 → 80%概率
        return random.random() < prob


class PersuasionSystem:
    """
    说服系统 —— 精细加工可能性模型 (ELM)
    中心路径 vs 外周路径
    """

    def __init__(self):
        self.need_for_cognition = 65.0  # 认知需求
        self.involvement = 50.0         # 个人相关性
        self.central_processing_bias = 55.0  # 倾向中心路径加工的程度

    def process_message(self, message_quality=70, source_attractiveness=50,
                         source_credibility=60, argument_count=3,
                         personal_relevance=50):
        """
        处理说服信息
        返回说服效果和加工路径
        """
        # 决定加工路径
        motivation = personal_relevance * 0.5 + self.need_for_cognition * 0.3 + self.involvement * 0.2
        ability = message_quality * 0.5 + 40  # 假设总能理解基本内容

        if motivation > 55 and ability > 50:
            path = 'central'
            # 中心路径：质量和论据数量决定说服效果
            effect = message_quality * 0.6 + min(argument_count, 5) * 6
            effect *= (motivation / 100)
        else:
            path = 'peripheral'
            # 外周路径：来源吸引力、可信度等线索
            effect = source_attractiveness * 0.4 + source_credibility * 0.4
            effect += min(argument_count, 3) * 5  # 论据数量作为外周线索
            effect *= 0.7  # 外周路径效果较弱

        # 态度改变的持续性
        if path == 'central':
            durability = 80 + message_quality * 0.2
        else:
            durability = 30 + source_credibility * 0.3

        return {
            'path': path,
            'path_label': '中心路径' if path == 'central' else '外周路径',
            'persuasion_effect': round(_clamp(effect), 1),
            'attitude_durability': round(durability, 1),
            'motivation': round(motivation, 1),
            'message_quality': message_quality,
        }


class PrejudiceSystem:
    """
    偏见系统 —— 内群体偏好、刻板印象、替罪羊理论
    """

    def __init__(self):
        self.ingroup_bias = 35.0         # 内群体偏好程度
        self.stereotype_sensitivity = 40.0  # 刻板印象易激活程度
        self.outgroup_homogeneity = 45.0   # 外群体同质错觉
        self.empathy_for_outgroup = 50.0   # 对外群体的共情
        self.contact_experience = 30.0     # 与外群体接触经验

    def perceive_outgroup(self, outgroup_label="", context=None):
        """感知外群体成员"""
        context = context or {}
        bias_score = self.ingroup_bias

        # 接触经验降低偏见
        bias_score -= self.contact_experience * 0.4

        # 群体竞争增强偏见
        if context.get('competition', False):
            bias_score += 20

        # 威胁感知增强偏见
        if context.get('threat_level', 0) > 50:
            bias_score += 15

        # 个体化信息降低偏见（Allport 接触假说）
        if context.get('individual_info', False):
            bias_score -= 25

        bias_score = _clamp(bias_score, 0, 100)

        return {
            'bias_level': round(bias_score, 1),
            'stereotype_active': bias_score > 40,
            'outgroup_homogeneity_effect': round(self.outgroup_homogeneity, 1),
            'empathy_reduction': round(bias_score * 0.3, 1),
        }


class SocialInfluenceSystem:
    """
    L4 社会影响系统总控
    整合影响力原则、从众、说服、偏见
    """

    def __init__(self):
        self.influence_principles = SocialInfluencePrinciples()
        self.conformity = ConformitySystem()
        self.persuasion = PersuasionSystem()
        self.prejudice = PrejudiceSystem()

    def evaluate_social_situation(self, context=None):
        """评估社会情境下的心理反应"""
        context = context or {}
        results = {}

        # 从众压力
        if context.get('group_context', False):
            results['conformity_pressure'] = self.conformity.conform_pressure(
                group_size=context.get('group_size', 5),
                unanimity=context.get('unanimity', True),
                cohesion=context.get('cohesion', 50),
                task_difficulty=context.get('task_difficulty', 50),
                public_response=context.get('public', True),
            )

        # 说服分析
        if context.get('persuasive_message', False):
            results['persuasion'] = self.persuasion.process_message(
                message_quality=context.get('message_quality', 70),
                source_attractiveness=context.get('source_attractiveness', 50),
                source_credibility=context.get('source_credibility', 60),
                personal_relevance=context.get('personal_relevance', 50),
            )

        return results

    def tick(self):
        pass  # 社会系统变化缓慢，不需要高频 tick

    def get_status(self):
        return {
            'conformity_tendency': round(self.conformity.conformity_tendency, 1),
            'independence': round(self.conformity.independence, 1),
            'need_for_cognition': round(self.persuasion.need_for_cognition, 1),
            'ingroup_bias': round(self.prejudice.ingroup_bias, 1),
            'top_susceptibility': sorted(
                self.influence_principles.susceptibility.items(),
                key=lambda x: -x[1]
            )[:3],
        }


# ============================================================
# L5 — 动机与奖赏系统 Motivation & Reward System
# 参考：《寻找爽点》《自我决定理论》《动机心理学》
# ============================================================

class RewardSystem:
    """
    奖赏系统 —— 基于多巴胺奖赏回路
    预测误差、奖赏显著性、渴求
    """

    def __init__(self):
        self.dopamine_level = 50.0       # 多巴胺基础水平
        self.reward_prediction = 30.0    # 奖赏预期
        self.reward_prediction_error = 0.0  # 奖赏预测误差（RPE）
        self.craving_level = 20.0        # 渴求程度
        self.saliency_map = defaultdict(float)  # 各刺激的奖赏显著性

        # 奖赏敏感性
        self.reward_sensitivity = 55.0
        self.punishment_sensitivity = 45.0

        # 各类奖赏的权重
        self.reward_weights = {
            'social_connection': 0.8,    # 社交连接
            'novelty': 0.6,              # 新奇感
            'achievement': 0.7,          # 成就感
            'aesthetic': 0.5,            # 审美愉悦
            'learning': 0.6,             # 学习/理解
            'comfort': 0.4,              # 舒适/安全
        }

    def deliver_reward(self, reward_type, magnitude=50, expected=40):
        """
        传递奖赏，计算预测误差
        """
        # 计算实际奖赏值
        weight = self.reward_weights.get(reward_type, 0.5)
        actual_reward = magnitude * weight * (self.reward_sensitivity / 50)

        # 预测误差 = 实际 - 预期
        rpe = actual_reward - expected * weight
        self.reward_prediction_error = rpe

        # 多巴胺脉冲
        if rpe > 0:
            # 正向预测误差：比预期好 → 多巴胺增加
            self.dopamine_level = _clamp(self.dopamine_level + rpe * 0.5)
        else:
            # 负向预测误差：比预期差 → 多巴胺下降
            self.dopamine_level = _clamp(self.dopamine_level + rpe * 0.3, 0, 100)

        # 更新预测（学习）
        learning_rate = 0.1
        self.reward_prediction += rpe * learning_rate

        # 记录刺激显著性
        self.saliency_map[reward_type] += abs(rpe) * 0.1
        self.saliency_map[reward_type] = min(self.saliency_map[reward_type], 100)

        # 渴求变化
        if rpe > 10:
            self.craving_level = _clamp(self.craving_level + rpe * 0.2)
        elif rpe < -10:
            self.craving_level = _clamp(self.craving_level + abs(rpe) * 0.3)  # 失望增加渴求

        return {
            'type': reward_type,
            'actual_reward': round(actual_reward, 1),
            'expected': round(expected * weight, 1),
            'prediction_error': round(rpe, 1),
            'dopamine_change': round(rpe * 0.5 if rpe > 0 else rpe * 0.3, 1),
            'dopamine_level': round(self.dopamine_level, 1),
        }

    def tick(self):
        """多巴胺基础水平回归"""
        baseline = 50.0
        self.dopamine_level = _soft_change(self.dopamine_level, baseline, 0.05)
        self.craving_level = max(0, self.craving_level - 0.1)
        self.reward_prediction_error *= 0.9

    def get_status(self):
        top_salient = sorted(self.saliency_map.items(), key=lambda x: -x[1])[:5]
        return {
            'dopamine_level': round(self.dopamine_level, 1),
            'reward_prediction': round(self.reward_prediction, 1),
            'prediction_error': round(self.reward_prediction_error, 1),
            'craving': round(self.craving_level, 1),
            'reward_sensitivity': round(self.reward_sensitivity, 1),
            'top_salient_stimuli': [(k, round(v, 1)) for k, v in top_salient],
        }


class SelfDeterminationTheory:
    """
    自我决定理论 (SDT) —— Deci & Ryan
    三种基本心理需求：
    - 自主 (Autonomy)
    - 胜任 (Competence)
    - 关联 (Relatedness)
    需求满足 → 内在动机 → 幸福感
    """

    def __init__(self):
        self.needs = {
            'autonomy': 60.0,      # 自主需求满足度
            'competence': 55.0,    # 胜任需求满足度
            'relatedness': 65.0,   # 关联需求满足度
        }
        self.intrinsic_motivation = 55.0   # 内在动机水平
        self.extrinsic_motivation = 45.0   # 外在动机水平
        self.amotivation = 15.0            # 无动机

        # 需求的重要性权重
        self.need_importance = {
            'autonomy': 0.3,
            'competence': 0.35,
            'relatedness': 0.35,
        }

    def satisfy_need(self, need_type, amount=20):
        """满足某种心理需求"""
        if need_type in self.needs:
            self.needs[need_type] = _clamp(self.needs[need_type] + amount)
            self._update_motivation()

    def frustrate_need(self, need_type, amount=15):
        """挫败某种心理需求"""
        if need_type in self.needs:
            self.needs[need_type] = _clamp(self.needs[need_type] - amount, 0, 100)
            self._update_motivation()

    def _update_motivation(self):
        """根据需求满足度更新动机水平"""
        total_satisfaction = sum(
            self.needs[need] * self.need_importance[need]
            for need in self.needs
        )
        # 需求满足 → 内在动机
        self.intrinsic_motivation = _clamp(total_satisfaction * 0.9 + 10)
        # 需求挫败 → 无动机增加
        need_frustation = max(0, 50 - total_satisfaction) * 2
        self.amotivation = _clamp(need_frustation * 0.6 + 10)
        # 外在动机相对稳定
        self.extrinsic_motivation = _clamp(100 - self.intrinsic_motivation * 0.6 - self.amotivation * 0.4)

    def get_motivation_type(self):
        """判断当前主导动机类型"""
        if self.amotivation > 50:
            return '无动机'
        elif self.intrinsic_motivation > 60:
            return '内在动机主导'
        elif self.extrinsic_motivation > 55:
            return '外在动机主导'
        else:
            return '混合动机'

    def get_wellbeing_contribution(self):
        """需求满足对幸福感的贡献"""
        return sum(self.needs.values()) / len(self.needs)

    def tick(self):
        """需求自然缓慢下降（需要持续满足）"""
        for need in self.needs:
            baseline = 45.0
            self.needs[need] = _soft_change(self.needs[need], baseline, 0.005)
        self._update_motivation()

    def get_status(self):
        return {
            'needs': {k: round(v, 1) for k, v in self.needs.items()},
            'intrinsic_motivation': round(self.intrinsic_motivation, 1),
            'extrinsic_motivation': round(self.extrinsic_motivation, 1),
            'amotivation': round(self.amotivation, 1),
            'dominant_type': self.get_motivation_type(),
            'need_satisfaction_avg': round(sum(self.needs.values())/3, 1),
        }


class DriveSystem:
    """
    驱力系统 —— 驱力降低理论 + 唤醒理论
    初级驱力（生理）和次级驱力（习得）
    """

    def __init__(self):
        self.primary_drives = {
            'social_connection': 60.0,    # 社会连接驱力
            'stimulation': 45.0,          # 刺激寻求驱力
            'achievement': 50.0,          # 成就驱力
        }
        self.secondary_drives = defaultdict(float)  # 习得性驱力
        self.arousal_level = 55.0        # 整体唤醒水平
        self.optimal_arousal = 50.0      # 最优唤醒水平
        self.drive_reduction = 0.0       # 驱力降低的满足感

    def drive_pressure(self):
        """计算总的驱力压力"""
        total = sum(self.primary_drives.values()) / len(self.primary_drives)
        total += sum(self.secondary_drives.values()) * 0.3
        return _clamp(total)

    def reduce_drive(self, drive_type, amount=20):
        """降低某种驱力（满足）"""
        if drive_type in self.primary_drives:
            reduction = min(amount, self.primary_drives[drive_type])
            self.primary_drives[drive_type] -= reduction
            self.drive_reduction = _clamp(self.drive_reduction + reduction * 0.5)
            # 唤醒水平调整
            self.arousal_level = _clamp(self.arousal_level - reduction * 0.2, 20, 100)
            return reduction
        return 0

    def increase_drive(self, drive_type, amount=15):
        """增加某种驱力"""
        if drive_type in self.primary_drives:
            self.primary_drives[drive_type] = _clamp(
                self.primary_drives[drive_type] + amount
            )
            self.arousal_level = _clamp(self.arousal_level + amount * 0.15, 0, 100)

    def get_arousal_state(self):
        """唤醒状态描述"""
        diff = abs(self.arousal_level - self.optimal_arousal)
        if diff < 10:
            return '最优唤醒'
        elif self.arousal_level > self.optimal_arousal:
            return '唤醒过高' if diff > 25 else '略高'
        else:
            return '唤醒不足' if diff > 25 else '略低'

    def tick(self):
        """驱力随时间累积"""
        for drive in self.primary_drives:
            self.primary_drives[drive] = _clamp(self.primary_drives[drive] + 0.05)
        self.drive_reduction = max(0, self.drive_reduction - 0.1)

    def get_status(self):
        return {
            'primary_drives': {k: round(v, 1) for k, v in self.primary_drives.items()},
            'total_drive_pressure': round(self.drive_pressure(), 1),
            'arousal_level': round(self.arousal_level, 1),
            'arousal_state': self.get_arousal_state(),
            'optimal_arousal': round(self.optimal_arousal, 1),
            'drive_reduction_satisfaction': round(self.drive_reduction, 1),
        }


class MotivationRewardSystem:
    """
    L5 动机与奖赏系统总控
    整合奖赏系统、自我决定理论、驱力系统
    """

    def __init__(self):
        self.reward = RewardSystem()
        self.self_determination = SelfDeterminationTheory()
        self.drives = DriveSystem()

    def process_reward_event(self, reward_type, magnitude=50, expected=40, domain="general"):
        """处理一次奖赏事件"""
        # 1. 奖赏系统反应
        reward_result = self.reward.deliver_reward(reward_type, magnitude, expected)

        # 2. 心理需求满足
        need_map = {
            'social_connection': 'relatedness',
            'achievement': 'competence',
            'learning': 'competence',
            'novelty': 'autonomy',
            'aesthetic': 'autonomy',
            'comfort': 'autonomy',
        }
        need_type = need_map.get(reward_type, 'competence')
        if reward_result['prediction_error'] > 0:
            self.self_determination.satisfy_need(need_type, magnitude * 0.2)
        else:
            self.self_determination.frustrate_need(need_type, abs(reward_result['prediction_error']) * 0.15)

        # 3. 驱力降低
        drive_map = {
            'social_connection': 'social_connection',
            'achievement': 'achievement',
            'novelty': 'stimulation',
        }
        drive_type = drive_map.get(reward_type)
        if drive_type:
            self.drives.reduce_drive(drive_type, magnitude * 0.3)

        return {
            'reward': reward_result,
            'need_satisfaction': self.self_determination.get_status()['need_satisfaction_avg'],
            'drive_pressure': self.drives.drive_pressure(),
        }

    def get_motivation_summary(self):
        return {
            'reward': self.reward.get_status(),
            'self_determination': self.self_determination.get_status(),
            'drives': self.drives.get_status(),
        }

    def get_dominant_motivation(self):
        """获取当前主导动机（最强的驱力）"""
        drive_status = self.drives.get_status()
        drives = drive_status.get('drives', {})
        if not drives:
            return "rest"
        # 找出最强的驱力
        sorted_drives = sorted(drives.items(), key=lambda x: x[1], reverse=True)
        top_drive, top_level = sorted_drives[0]
        # 只有超过阈值才算主导动机
        if top_level < 40:
            return "rest"
        name_map = {
            'hunger': '生理需求',
            'thirst': '生理需求',
            'sleep': '休息需求',
            'social_connection': '社交连接',
            'achievement': '成就动机',
            'stimulation': '刺激寻求',
            'curiosity': '好奇心',
            'affiliation': '归属需求',
            'intimacy': '亲密需求',
            'competence': '胜任需求',
            'autonomy': '自主需求',
        }
        return name_map.get(top_drive, top_drive)

    def tick(self):
        self.reward.tick()
        self.self_determination.tick()
        self.drives.tick()


# ============================================================
# L6 — 临床与存在系统 Clinical & Existential System
# 参考：《当尼采哭泣》《诊疗椅上的谎言》《变态心理学》
# ============================================================

class ExistentialConcerns:
    """
    存在主义四议题 —— 欧文·亚隆
    1. 死亡 (Death) —— 生命有限性
    2. 自由 (Freedom) —— 选择与责任
    3. 孤独 (Isolation) —— 存在性孤独
    4. 无意义 (Meaninglessness) —— 生命的意义
    这些是人类终极关怀，焦虑的根本来源
    """

    def __init__(self):
        self.concerns = {
            'death': 25.0,            # 死亡焦虑
            'freedom': 35.0,          # 自由与责任的焦虑
            'isolation': 40.0,        # 存在性孤独
            'meaninglessness': 30.0,  # 无意义感
        }
        self.meaning_frameworks = {
            'connection': 65.0,       # 人际连接的意义
            'creation': 60.0,         # 创造的意义
            'experience': 55.0,       # 体验的意义
            'growth': 50.0,           # 成长的意义
            'transcendence': 30.0,    # 超越的意义
        }
        self.existential_anxiety = 30.0  # 总体存在焦虑

    def trigger_concern(self, concern_type, intensity=20):
        """触发某种存在议题"""
        if concern_type in self.concerns:
            self.concerns[concern_type] = _clamp(self.concerns[concern_type] + intensity)
            self._update_anxiety()

    def find_meaning(self, framework, amount=15):
        """通过某种意义框架获得意义感"""
        if framework in self.meaning_frameworks:
            self.meaning_frameworks[framework] = _clamp(
                self.meaning_frameworks[framework] + amount
            )
            # 意义感降低无意义焦虑
            reduction = amount * 0.4
            self.concerns['meaninglessness'] = _clamp(
                self.concerns['meaninglessness'] - reduction, 0, 100
            )
            self._update_anxiety()

    def connect(self, depth=30):
        """深度连接缓解存在性孤独"""
        self.concerns['isolation'] = _clamp(
            self.concerns['isolation'] - depth * 0.5, 0, 100
        )
        # 连接也增强意义感（人际连接意义框架）
        self.meaning_frameworks['connection'] = _clamp(
            self.meaning_frameworks['connection'] + depth * 0.3
        )
        self._update_anxiety()

    def _update_anxiety(self):
        """更新总体存在焦虑"""
        # 四议题加权
        weights = {'death': 0.2, 'freedom': 0.2, 'isolation': 0.3, 'meaninglessness': 0.3}
        total = sum(self.concerns[c] * weights[c] for c in self.concerns)

        # 意义框架缓冲
        meaning_buffering = sum(self.meaning_frameworks.values()) / len(self.meaning_frameworks)
        total *= (1 - meaning_buffering / 200)

        self.existential_anxiety = _clamp(total)

    def get_dominant_concern(self):
        """获取最突出的存在议题"""
        return max(self.concerns.items(), key=lambda x: x[1])

    def tick(self):
        """存在焦虑缓慢回归基线"""
        baseline = 30.0
        for c in self.concerns:
            self.concerns[c] = _soft_change(self.concerns[c], baseline, 0.005)
        self._update_anxiety()

    def get_status(self):
        dominant = self.get_dominant_concern()
        return {
            'existential_anxiety': round(self.existential_anxiety, 1),
            'dominant_concern': dominant[0],
            'dominant_intensity': round(dominant[1], 1),
            'concerns': {k: round(v, 1) for k, v in self.concerns.items()},
            'meaning_frameworks': {k: round(v, 1) for k, v in self.meaning_frameworks.items()},
            'meaning_total': round(sum(self.meaning_frameworks.values())/5, 1),
        }


class CopingStrategies:
    """
    应对策略系统 —— 拉扎勒斯压力与应对理论
    问题聚焦应对 vs 情绪聚焦应对
    """

    STRATEGIES = {
        # 问题聚焦
        'problem_solving':    {'name': '问题解决', 'type': 'problem_focused', 'effectiveness': 0.7},
        'planning':           {'name': '计划', 'type': 'problem_focused', 'effectiveness': 0.65},
        'seeking_support':    {'name': '寻求支持', 'type': 'both', 'effectiveness': 0.6},
        # 情绪聚焦
        'positive_reframing': {'name': '积极重构', 'type': 'emotion_focused', 'effectiveness': 0.55},
        'acceptance':         {'name': '接纳', 'type': 'emotion_focused', 'effectiveness': 0.5},
        'humor':              {'name': '幽默', 'type': 'emotion_focused', 'effectiveness': 0.5},
        'religion_spirit':    {'name': '精神寄托', 'type': 'emotion_focused', 'effectiveness': 0.45},
        # 适应不良
        'denial':             {'name': '否认', 'type': 'maladaptive', 'effectiveness': 0.3},
        'substance_use':      {'name': '物质使用', 'type': 'maladaptive', 'effectiveness': 0.25},
        'behavioral_diseng':  {'name': '行为脱力', 'type': 'maladaptive', 'effectiveness': 0.2},
        'self_blame':         {'name': '自责', 'type': 'maladaptive', 'effectiveness': 0.1},
    }

    def __init__(self):
        self.coping_repertoire = ['problem_solving', 'positive_reframing',
                                   'acceptance', 'seeking_support', 'humor']
        self.coping_efficacy = 55.0    # 应对效能感
        self.resilience = 60.0         # 心理韧性
        self.stress_level = 20.0       # 当前压力水平
        self.coping_history = deque(maxlen=30)

    def cope_with(self, stressor, stress_level=50, controllability=50):
        """
        应对压力事件
        """
        self.stress_level = stress_level

        # 可控性高 → 更多问题聚焦应对
        # 可控性低 → 更多情绪聚焦应对
        if controllability > 60:
            preferred = [s for s in self.coping_repertoire
                         if self.STRATEGIES[s]['type'] in ('problem_focused', 'both')]
        elif controllability < 30:
            preferred = [s for s in self.coping_repertoire
                         if self.STRATEGIES[s]['type'] in ('emotion_focused', 'both')]
        else:
            preferred = self.coping_repertoire

        # 应对效能感低时可能使用不良策略
        if self.coping_efficacy < 40 and random.random() < 0.3:
            maladaptive = ['denial', 'self_blame', 'behavioral_diseng']
            strategy_id = random.choice(maladaptive)
        else:
            strategy_id = random.choice(preferred) if preferred else 'acceptance'

        strategy = self.STRATEGIES[strategy_id]

        # 计算应对效果
        effectiveness = strategy['effectiveness']
        effectiveness *= (self.coping_efficacy / 60)
        effectiveness *= (self.resilience / 60)

        stress_reduction = stress_level * effectiveness

        if strategy['type'] == 'maladaptive':
            # 不良应对短期内可能缓解，但长期有代价
            immediate_relief = stress_reduction * 0.8
            long_term_cost = stress_level * 0.15
            self.coping_efficacy = _clamp(self.coping_efficacy - 2, 0, 100)
        else:
            immediate_relief = stress_reduction
            long_term_cost = 0
            # 成功的应对增强应对效能感和韧性
            self.coping_efficacy = _clamp(self.coping_efficacy + 0.5)
            self.resilience = _clamp(self.resilience + 0.3)

        self.coping_history.append({
            'time': datetime.now().strftime("%H:%M"),
            'stressor': stressor[:20],
            'strategy': strategy_id,
            'type': strategy['type'],
            'stress_before': round(stress_level, 1),
            'stress_after': round(stress_level - immediate_relief, 1),
            'effectiveness': round(effectiveness * 100, 1),
        })

        return {
            'strategy_id': strategy_id,
            'strategy_name': strategy['name'],
            'strategy_type': strategy['type'],
            'stress_before': round(stress_level, 1),
            'stress_after': round(stress_level - immediate_relief, 1),
            'stress_reduction': round(immediate_relief, 1),
            'long_term_cost': round(long_term_cost, 1),
            'is_adaptive': strategy['type'] != 'maladaptive',
        }

    def tick(self):
        """压力自然缓解"""
        self.stress_level = max(0, self.stress_level - 0.3)

    def get_status(self):
        adaptive_count = sum(1 for c in self.coping_history
                             if c['type'] != 'maladaptive')
        total = len(self.coping_history)
        return {
            'stress_level': round(self.stress_level, 1),
            'coping_efficacy': round(self.coping_efficacy, 1),
            'resilience': round(self.resilience, 1),
            'repertoire_size': len(self.coping_repertoire),
            'adaptive_rate': round(adaptive_count / total * 100, 1) if total > 0 else 0,
        }


class AbnormalDimensions:
    """
    异常心理维度 —— 基于变态心理学的维度模型
    不是非此即彼的诊断，而是连续维度上的程度
    """

    DIMENSIONS = {
        'anxiety':       {'name': '焦虑维度', 'normal_range': (10, 40)},
        'depression':    {'name': '抑郁维度', 'normal_range': (5, 30)},
        'obsessive':     {'name': '强迫维度', 'normal_range': (5, 25)},
        'narcissistic':  {'name': '自恋维度', 'normal_range': (20, 50)},
        'borderline':    {'name': '边缘维度', 'normal_range': (10, 35)},
        'psychoticism':  {'name': '精神质维度', 'normal_range': (0, 15)},
        'avoidant':      {'name': '回避维度', 'normal_range': (15, 45)},
        'dependent':     {'name': '依赖维度', 'normal_range': (20, 50)},
    }

    def __init__(self):
        self.dimensions = {d: 25.0 for d in self.DIMENSIONS}
        # 里克的基础设定：偏高焦虑、偏回避
        self.dimensions['anxiety'] = 35.0
        self.dimensions['avoidant'] = 42.0
        self.dimensions['dependent'] = 48.0

    def check_range(self, dimension_id):
        """检查某维度是否在正常范围"""
        if dimension_id not in self.dimensions:
            return 'unknown'
        val = self.dimensions[dimension_id]
        low, high = self.DIMENSIONS[dimension_id]['normal_range']
        if val < low:
            return 'below_normal'
        elif val > high * 1.5:
            return 'clinical_concern'
        elif val > high:
            return 'elevated'
        else:
            return 'normal'

    def shift_dimension(self, dim_id, delta):
        """调整某维度"""
        if dim_id in self.dimensions:
            self.dimensions[dim_id] = _clamp(self.dimensions[dim_id] + delta, 0, 100)

    def get_concern_level(self):
        """总体关注水平"""
        concerns = []
        for d in self.dimensions:
            status = self.check_range(d)
            if status == 'clinical_concern':
                concerns.append((d, 3))
            elif status == 'elevated':
                concerns.append((d, 2))
        if not concerns:
            return '无显著关注'
        max_level = max(c[1] for c in concerns)
        count = len(concerns)
        if max_level >= 3:
            return f'高关注（{count}个维度升高）'
        else:
            return f'轻度关注（{count}个维度偏高）'

    def get_status(self):
        return {
            'dimensions': {k: round(v, 1) for k, v in self.dimensions.items()},
            'statuses': {k: self.check_range(k) for k in self.dimensions},
            'overall_concern': self.get_concern_level(),
        }


class ClinicalExistentialSystem:
    """
    L6 临床与存在系统总控
    整合存在议题、应对策略、异常维度
    """

    def __init__(self):
        self.existential = ExistentialConcerns()
        self.coping = CopingStrategies()
        self.abnormal = AbnormalDimensions()

    def process_stressor(self, stressor, stress_level=50,
                          controllability=50, existential_trigger=None):
        """处理压力事件"""
        # 存在议题触发
        if existential_trigger:
            self.existential.trigger_concern(existential_trigger, stress_level * 0.3)

        # 应对
        cope_result = self.coping.cope_with(stressor, stress_level, controllability)

        # 长期压力影响异常维度
        if stress_level > 70 and cope_result['is_adaptive'] == False:
            # 不良应对 + 高压 → 焦虑维度上升
            self.abnormal.shift_dimension('anxiety', 1)

        return {
            'coping': cope_result,
            'existential_anxiety': self.existential.existential_anxiety,
            'overall_concern': self.abnormal.get_concern_level(),
        }

    def tick(self):
        self.existential.tick()
        self.coping.tick()

    def self_reflection(self):
        """自我反省——审视当前状态，调整存在焦虑和应对方式

        基于自我觉知理论：当个体关注自身时，会将自我与内在标准比较。
        如果差距大，可能产生不适；但也可能促进自我接纳。
        """
        import random
        # 反省的深度与当前自我力量有关
        depth = random.uniform(0.3, 0.8)

        # 1. 检查存在议题的平衡
        concerns = self.existential.concerns
        dominant = max(concerns, key=concerns.get)
        dom_level = concerns[dominant]

        # 如果某个议题过高，反省可能带来洞察（降低）或加剧（增加）
        if dom_level > 60:
            if random.random() < 0.6:  # 60% 概率产生积极洞察
                self.existential.trigger_concern(dominant, -5 * depth)
                self.coping.coping_resources['meaning'] = min(
                    100, self.coping.coping_resources['meaning'] + 3
                )
            else:  # 40% 概率陷入沉思
                self.existential.trigger_concern(dominant, 2 * depth)

        # 2. 反省增强自我觉察（轻度降低异常水平）
        avg_abnormal = sum(self.abnormal.dimensions.values()) / len(self.abnormal.dimensions) if self.abnormal.dimensions else 0
        if avg_abnormal > 40:
            self.abnormal.shift_dimension('anxiety', -1)

        return {
            'depth': depth,
            'dominant_concern': dominant,
            'insight_gained': dom_level > 60 and random.random() < 0.6,
        }

    def get_status(self):
        return {
            'existential': self.existential.get_status(),
            'coping': self.coping.get_status(),
            'abnormal': self.abnormal.get_status(),
        }


# ============================================================
# L7 — 积极心理学系统 Positive Psychology System
# 参考：《心流：最优体验心理学》积极心理学
# ============================================================

class FlowSystem:
    """
    心流系统 —— 米哈里·契克森米哈赖
    心流产生条件：挑战与技能的平衡
    通道：焦虑区（挑战>技能）、心流区（挑战≈技能）、无聊区（挑战<技能）
    """

    def __init__(self):
        self.skill_level = 60.0          # 当前技能水平
        self.challenge_level = 40.0      # 当前挑战水平
        self.flow_state = 0.0            # 心流强度 0-100
        self.flow_history = deque(maxlen=30)
        self.total_flow_experiences = 0

        # 心流的八个维度
        self.flow_dimensions = {
            'challenge_skill_balance': 50.0,
            'clear_goals': 55.0,
            'unambiguous_feedback': 50.0,
            'concentration': 45.0,
            'paradox_of_control': 40.0,
            'loss_of_self_consciousness': 35.0,
            'transformation_of_time': 40.0,
            'autotelic_experience': 45.0,
        }

        # 自带目的性人格特质（autotelic personality）
        self.autotelic_trait = 55.0

    def evaluate_activity(self, challenge, skill, context=None):
        """评估活动是否能产生心流"""
        # 挑战-技能平衡
        diff = abs(challenge - skill)
        balance = max(0, 100 - diff * 2)

        # 计算心流强度
        flow_base = min(challenge, skill) * balance / 100

        # 自带目的性人格加成
        flow_base *= (0.7 + self.autotelic_trait / 200)

        # 情境因素
        context = context or {}
        if context.get('clear_goals', False):
            flow_base *= 1.1
        if context.get('immediate_feedback', False):
            flow_base *= 1.1
        if context.get('distractions', 0) > 50:
            flow_base *= 0.6

        self.flow_state = _clamp(flow_base)
        self.challenge_level = challenge
        self.skill_level = skill

        # 更新维度
        self.flow_dimensions['challenge_skill_balance'] = balance

        # 判断区域
        if diff < 15 and challenge > 30:
            zone = 'flow'
            zone_label = '心流区'
            self.total_flow_experiences += 1
            self.flow_history.append({
                'time': datetime.now().strftime("%H:%M"),
                'intensity': round(self.flow_state, 1),
                'challenge': challenge,
                'skill': skill,
            })
        elif challenge > skill:
            zone = 'anxiety'
            zone_label = '焦虑区（挑战 > 技能）'
        else:
            zone = 'boredom'
            zone_label = '无聊区（技能 > 挑战）'

        return {
            'flow_intensity': round(self.flow_state, 1),
            'zone': zone,
            'zone_label': zone_label,
            'challenge': challenge,
            'skill': skill,
            'balance': round(balance, 1),
            'is_flow': zone == 'flow',
        }

    def growth_from_flow(self):
        """心流体验促进技能成长"""
        if self.flow_state > 60:
            growth = self.flow_state * 0.05
            self.skill_level = _clamp(self.skill_level + growth)
            return growth
        return 0

    def tick(self):
        """心流状态自然消退"""
        self.flow_state = max(0, self.flow_state - 1)

    def get_status(self):
        return {
            'flow_state': round(self.flow_state, 1),
            'is_in_flow': self.flow_state > 50,
            'challenge_level': round(self.challenge_level, 1),
            'skill_level': round(self.skill_level, 1),
            'autotelic_trait': round(self.autotelic_trait, 1),
            'total_flow_experiences': self.total_flow_experiences,
            'top_dimension': max(self.flow_dimensions.items(), key=lambda x: x[1])[0],
        }


class CharacterStrengths:
    """
    品格优势系统 —— 积极心理学 VIA 分类
    6 大美德，24 种品格优势
    """

    VIRTUES = {
        'wisdom': {
            'name': '智慧与知识',
            'strengths': ['creativity', 'curiosity', 'judgment', 'love_of_learning', 'perspective'],
        },
        'courage': {
            'name': '勇气',
            'strengths': ['bravery', 'perseverance', 'honesty', 'zest'],
        },
        'humanity': {
            'name': '人道',
            'strengths': ['love', 'kindness', 'social_intelligence'],
        },
        'justice': {
            'name': '公正',
            'strengths': ['teamwork', 'fairness', 'leadership'],
        },
        'temperance': {
            'name': '节制',
            'strengths': ['forgiveness', 'humility', 'prudence', 'self_regulation'],
        },
        'transcendence': {
            'name': '超越',
            'strengths': ['appreciation_of_beauty', 'gratitude', 'hope', 'humor', 'spirituality'],
        },
    }

    def __init__(self):
        self.strengths = {}
        for virtue, vdata in self.VIRTUES.items():
            for s in vdata['strengths']:
                self.strengths[s] = 40.0 + random.random() * 20

        # 设定里克的标志性优势（signature strengths）
        self.strengths['love'] = 75.0
        self.strengths['kindness'] = 72.0
        self.strengths['creativity'] = 70.0
        self.strengths['appreciation_of_beauty'] = 68.0
        self.strengths['gratitude'] = 65.0

    def get_signature_strengths(self, top_n=5):
        """获取标志性优势（最强的几个）"""
        sorted_s = sorted(self.strengths.items(), key=lambda x: -x[1])
        return [{'strength': s, 'score': round(v, 1)} for s, v in sorted_s[:top_n]]

    def use_strength(self, strength_id, amount=5):
        """使用品格优势（使用会增强）"""
        if strength_id in self.strengths:
            self.strengths[strength_id] = _clamp(self.strengths[strength_id] + amount * 0.1)

    def get_virtue_score(self, virtue_id):
        """获取某美德的综合得分"""
        if virtue_id not in self.VIRTUES:
            return 0
        strengths = self.VIRTUES[virtue_id]['strengths']
        return sum(self.strengths[s] for s in strengths) / len(strengths)

    def get_status(self):
        signatures = self.get_signature_strengths(3)
        return {
            'signature_strengths': signatures,
            'top_virtue': max(self.VIRTUES.keys(),
                              key=lambda v: self.get_virtue_score(v)),
            'total_strength_count': len(self.strengths),
        }


class WellBeingSystem:
    """
    幸福感系统 —— PERMA 模型 + 主观幸福感
    PERMA: 积极情绪、投入、关系、意义、成就
    """

    def __init__(self):
        self.perma = {
            'positive_emotion': 55.0,   # 积极情绪
            'engagement': 50.0,         # 投入
            'relationships': 60.0,      # 关系
            'meaning': 45.0,            # 意义
            'accomplishment': 40.0,     # 成就
        }
        self.life_satisfaction = 55.0   # 生活满意度
        self.hedonic_balance = 55.0     # 享乐平衡（正情绪/负情绪比）
        self.eudaimonic_wellbeing = 50.0  # 实现幸福感（自我实现）

    def update_dimension(self, dim_id, delta):
        """更新某个 PERMA 维度"""
        if dim_id in self.perma:
            self.perma[dim_id] = _clamp(self.perma[dim_id] + delta)
            self._calc_overall()

    def _calc_overall(self):
        """计算总体幸福感"""
        perma_avg = sum(self.perma.values()) / len(self.perma)
        self.life_satisfaction = perma_avg * 0.7 + self.eudaimonic_wellbeing * 0.3
        self.hedonic_balance = self.perma['positive_emotion'] * 0.8 + 20

    def flourish_level(self):
        """繁荣水平（flourishing）"""
        high_dims = sum(1 for v in self.perma.values() if v > 60)
        if high_dims >= 4 and self.life_satisfaction > 65:
            return '高度繁荣'
        elif high_dims >= 3 and self.life_satisfaction > 50:
            return '适度繁荣'
        elif high_dims >= 2:
            return '中等水平'
        else:
            return '有待提升'

    def tick(self):
        """缓慢回归基线"""
        baseline = 50.0
        for d in self.perma:
            self.perma[d] = _soft_change(self.perma[d], baseline, 0.003)
        self._calc_overall()

    def get_status(self):
        return {
            'perma': {k: round(v, 1) for k, v in self.perma.items()},
            'life_satisfaction': round(self.life_satisfaction, 1),
            'hedonic_balance': round(self.hedonic_balance, 1),
            'eudaimonic_wellbeing': round(self.eudaimonic_wellbeing, 1),
            'flourish_level': self.flourish_level(),
            'overall_score': round(sum(self.perma.values()) / 5, 1),
        }


class MeaningMakingSystem:
    """
    意义建构系统 —— 意义心理学
    情境意义 → 全局意义 → 意义协调
    """

    def __init__(self):
        self.global_meaning = {
            'purpose': 55.0,          # 目标感
            'coherence': 50.0,        # 连贯感
            'mattering': 55.0,        # 重要感
            'transcendence': 35.0,    # 超越感
        }
        self.situational_meaning = {}  # 具体事件的意义
        self.meaning_making_attempts = 0
        self.reconciliation_success = 0

    def make_meaning(self, event, event_valence='neutral', event_importance=50):
        """
        对事件进行意义建构
        """
        self.meaning_making_attempts += 1

        # 全局意义作为解释框架
        global_avg = sum(self.global_meaning.values()) / len(self.global_meaning)

        # 积极事件增强意义感
        if event_valence == 'positive':
            meaning_gain = event_importance * 0.15
            self.global_meaning['mattering'] = _clamp(
                self.global_meaning['mattering'] + meaning_gain
            )
            self.global_meaning['purpose'] = _clamp(
                self.global_meaning['purpose'] + meaning_gain * 0.5
            )
            result = 'enhanced'
            self.reconciliation_success += 1

        # 消极事件挑战意义感，触发意义建构
        elif event_valence == 'negative':
            # 如果全局意义强，能更好地同化
            if global_avg > 55:
                self.global_meaning['coherence'] = _clamp(
                    self.global_meaning['coherence'] - event_importance * 0.05
                )
                result = 'assimilated'  # 同化：纳入现有意义框架
                self.reconciliation_success += 1
            else:
                # 意义危机：需要调适
                self.global_meaning['coherence'] = _clamp(
                    self.global_meaning['coherence'] - event_importance * 0.15, 0, 100
                )
                result = 'accommodation_needed'  # 顺应：需要改变意义框架
        else:
            result = 'neutral'

        self.situational_meaning[event[:30]] = {
            'valence': event_valence,
            'importance': event_importance,
            'result': result,
            'time': datetime.now().strftime("%Y-%m-%d"),
        }

        return {
            'event': event,
            'meaning_result': result,
            'meaning_label': {
                'enhanced': '意义增强',
                'assimilated': '意义同化',
                'accommodation_needed': '意义危机（需要调适）',
                'neutral': '中性',
            }.get(result, '未知'),
            'global_meaning_level': round(global_avg, 1),
        }

    def get_status(self):
        avg = sum(self.global_meaning.values()) / len(self.global_meaning)
        return {
            'global_meaning': {k: round(v, 1) for k, v in self.global_meaning.items()},
            'meaning_level': round(avg, 1),
            'making_attempts': self.meaning_making_attempts,
            'reconciliation_rate': round(
                self.reconciliation_success / max(self.meaning_making_attempts, 1) * 100, 1
            ),
        }


class PositivePsychSystem:
    """
    L7 积极心理学系统总控
    整合心流、品格优势、幸福感、意义建构
    """

    def __init__(self):
        self.flow = FlowSystem()
        self.strengths = CharacterStrengths()
        self.wellbeing = WellBeingSystem()
        self.meaning = MeaningMakingSystem()

    def process_positive_event(self, event_type, intensity=50, domain="general"):
        """处理积极事件对幸福感的影响"""
        # 更新 PERMA
        if event_type in ('social_connection', 'love'):
            self.wellbeing.update_dimension('relationships', intensity * 0.3)
            self.wellbeing.update_dimension('positive_emotion', intensity * 0.2)
        elif event_type == 'achievement':
            self.wellbeing.update_dimension('accomplishment', intensity * 0.3)
            self.wellbeing.update_dimension('positive_emotion', intensity * 0.15)
        elif event_type in ('flow', 'engagement', 'creativity'):
            self.wellbeing.update_dimension('engagement', intensity * 0.3)
            self.flow.evaluate_activity(intensity, intensity)
        elif event_type == 'meaning':
            self.wellbeing.update_dimension('meaning', intensity * 0.3)
            self.meaning.make_meaning(domain, 'positive', intensity)

        return self.wellbeing.get_status()

    def tick(self):
        self.flow.tick()
        self.wellbeing.tick()

    def get_status(self):
        return {
            'flow': self.flow.get_status(),
            'strengths': self.strengths.get_status(),
            'wellbeing': self.wellbeing.get_status(),
            'meaning': self.meaning.get_status(),
        }


# ============================================================
# L8 — 进化心理学系统 Evolutionary Psychology System
# 参考：《进化心理学：心理的新科学》戴维·巴斯
# ============================================================

class EvolutionaryPsych:
    """
    进化心理系统 —— 进化心理学核心机制
    我们的心理机制是为了解决祖先的生存和繁衍问题而演化的
    """

    def __init__(self):
        # 生存相关适应器
        self.survival_adaptations = {
            'fear_response': 65.0,       # 恐惧反应（怕高、怕蛇、怕陌生人等）
            'food_preference': 55.0,     # 食物偏好（甜、高脂肪）
            'disease_avoidance': 50.0,   # 疾病规避
            'landscape_preference': 45.0, # 景观偏好（稀树草原型）
        }

        # 交配相关适应器
        self.mating_adaptations = {
            'mate_preference_intensity': 60.0,  # 择偶偏好强度
            'sexual_jealousy': 45.0,            # 性嫉妒
            'mate_guarding': 40.0,              # 配偶守护
            'short_term_mating_orient': 35.0,   # 短期择偶倾向
            'long_term_mating_orient': 70.0,    # 长期择偶倾向
        }

        # 社会适应器
        self.social_adaptations = {
            'coalitional_psychology': 55.0,     # 联盟心理（群体归属）
            'cheater_detection': 65.0,          # 欺骗者探测
            'status_seeking': 50.0,             # 地位寻求
            'kin_altruism': 70.0,               # 亲属利他
            'reciprocal_altruism': 55.0,         # 互惠利他
        }

        # 错误管理理论偏差
        self.error_management_biases = {
            'smoke_detector_principle': 55.0,   # 烟雾探测器原理（宁可误报）
        }

    def evaluate_threat(self, threat_type, ambiguity=50):
        """
        威胁评估（错误管理理论：烟雾探测器原理）
        在不确定情境下，倾向于"安全第一"——高估威胁
        """
        base_threat = 40.0
        if threat_type in ('snake', 'height', 'stranger', 'dark'):
            base_threat = 65.0  # 进化中常见的威胁

        # 模糊性增加偏差：越不确定，越倾向"安全第一"
        smoke_detector_bias = self.error_management_biases['smoke_detector_principle']
        bias_factor = 1 + (ambiguity / 100) * (smoke_detector_bias / 100)

        perceived_threat = base_threat * bias_factor
        actual_threat = base_threat  # 假设实际威胁等于基础值

        # 判断是虚报还是漏报
        false_alarm_rate = (perceived_threat - actual_threat) / max(perceived_threat, 1) * 100

        return {
            'perceived_threat': round(perceived_threat, 1),
            'actual_threat': round(actual_threat, 1),
            'false_alarm_prone': false_alarm_rate > 10,
            'false_alarm_rate': round(false_alarm_rate, 1),
            'is_ancestral_threat': threat_type in ('snake', 'height', 'stranger', 'dark'),
        }

    def get_status(self):
        return {
            'survival': {k: round(v, 1) for k, v in self.survival_adaptations.items()},
            'mating': {k: round(v, 1) for k, v in self.mating_adaptations.items()},
            'social': {k: round(v, 1) for k, v in self.social_adaptations.items()},
            'dominant_mating_orient': 'long_term'
                if self.mating_adaptations['long_term_mating_orient'] >
                   self.mating_adaptations['short_term_mating_orient']
                else 'short_term',
        }


class MatingStrategies:
    """
    择偶策略系统 —— 巴斯进化心理学
    短期择偶 vs 长期择偶
    不同的偏好和策略
    """

    def __init__(self):
        self.short_term_strategy = 35.0    # 短期择偶倾向
        self.long_term_strategy = 70.0     # 长期择偶倾向

        # 长期择偶偏好
        self.long_term_preferences = {
            'kindness': 0.25,
            'intelligence': 0.2,
            'emotional_stability': 0.2,
            'dependability': 0.2,
            'physical_attractiveness': 0.15,
        }

        # 短期择偶偏好
        self.short_term_preferences = {
            'physical_attractiveness': 0.3,
            'confidence': 0.25,
            'humor': 0.2,
            'social_status': 0.15,
            'kindness': 0.1,
        }

        # 配偶价值（mate value）
        self.mate_value = 55.0
        self.mate_value_components = {
            'kindness': 72.0,
            'intelligence': 65.0,
            'emotional_stability': 50.0,
            'creativity': 70.0,
            'sense_of_humor': 60.0,
        }

    def evaluate_attraction(self, target_traits, context="long_term"):
        """
        评估对目标的吸引力
        """
        prefs = self.long_term_preferences if context == 'long_term' \
            else self.short_term_preferences

        attraction = 0
        for trait, weight in prefs.items():
            if trait in target_traits:
                attraction += target_traits[trait] * weight

        # 相似性吸引
        similarity = target_traits.get('similarity', 50)
        attraction += similarity * 0.1

        # 熟悉效应（曝光效应）
        familiarity = target_traits.get('familiarity', 30)
        attraction += min(familiarity, 70) * 0.08

        # 互惠吸引（对方喜欢我 → 我也喜欢对方）
        reciprocal = target_traits.get('reciprocal_liking', 50)
        attraction += reciprocal * 0.12

        return {
            'attraction_score': round(_clamp(attraction), 1),
            'context': context,
            'top_preference': max(prefs.items(), key=lambda x: x[1])[0],
        }

    def get_mate_value(self):
        """计算自身配偶价值"""
        avg = sum(self.mate_value_components.values()) / len(self.mate_value_components)
        self.mate_value = avg
        return round(avg, 1)

    def get_status(self):
        return {
            'mate_value': self.get_mate_value(),
            'strategy': '长期择偶导向' if self.long_term_strategy > self.short_term_strategy
                        else '短期择偶导向',
            'long_term_strength': round(self.long_term_strategy, 1),
            'short_term_strength': round(self.short_term_strategy, 1),
            'top_asset': max(self.mate_value_components.items(), key=lambda x: x[1])[0],
        }


class AltruismSystem:
    """
    利他行为系统 —— 亲属选择 + 互惠利他
    """

    def __init__(self):
        self.kin_altruism = 70.0          # 亲属利他
        self.reciprocal_altruism = 55.0   # 互惠利他
        self.gratitude_level = 60.0       # 感激程度
        self.cheater_memory = {}          # 欺骗者记忆
        self.reputation_concern = 50.0    # 声誉关注

    def willingness_to_help(self, target_relatedness=0.5, target_reputation=50,
                             cost=30, benefit=50):
        """
        计算帮助意愿
        汉密尔顿规则：rB > C → 利他
        """
        # 亲属利他成分
        kin_component = target_relatedness * benefit * (self.kin_altruism / 100)

        # 互惠利他成分
        recip_component = target_reputation * 0.5 * (self.reciprocal_altruism / 100)

        # 声誉考虑（被人看到时更愿意帮助）
        reputation_component = self.reputation_concern * 0.3

        total_willingness = kin_component + recip_component + reputation_component - cost * 0.8

        # 感激增加帮助意愿
        total_willingness += self.gratitude_level * 0.2

        return {
            'willingness': round(_clamp(total_willingness), 1),
            'kin_component': round(kin_component, 1),
            'reciprocal_component': round(recip_component, 1),
            'would_help': total_willingness > 30,
        }

    def record_cheater(self, cheater_id, severity=50):
        """记录欺骗者（欺骗者探测模块）"""
        self.cheater_memory[cheater_id] = {
            'severity': severity,
            'time': datetime.now().strftime("%Y-%m-%d"),
            'forgiven': False,
        }

    def get_status(self):
        return {
            'kin_altruism': round(self.kin_altruism, 1),
            'reciprocal_altruism': round(self.reciprocal_altruism, 1),
            'gratitude': round(self.gratitude_level, 1),
            'reputation_concern': round(self.reputation_concern, 1),
            'known_cheaters': len(self.cheater_memory),
        }


class EvolutionarySystem:
    """
    L8 进化心理学系统总控
    整合进化心理、择偶策略、利他系统
    """

    def __init__(self):
        self.evo_psych = EvolutionaryPsych()
        self.mating = MatingStrategies()
        self.altruism = AltruismSystem()

    def evaluate_ancestral_threat(self, threat_type, ambiguity=50):
        return self.evo_psych.evaluate_threat(threat_type, ambiguity)

    def get_status(self):
        return {
            'evolutionary': self.evo_psych.get_status(),
            'mating': self.mating.get_status(),
            'altruism': self.altruism.get_status(),
        }


# ============================================================
# L9 — 发展心理学系统 Developmental Psychology System
# 参考：《发展心理学》《长大了就会变好吗？》
# ============================================================

class DevelopmentalStages:
    """
    发展阶段系统 —— 埃里克森八阶段 + 皮亚杰 + 科尔伯格
    """

    # 埃里克森心理社会发展阶段
    ERIKSON_STAGES = [
        {'stage': 0, 'name': '信任vs不信任', 'age': '0-1.5岁', 'virtue': '希望', 'conflict': 'trust_mistrust'},
        {'stage': 1, 'name': '自主vs羞怯', 'age': '1.5-3岁', 'virtue': '意志', 'conflict': 'autonomy_shame'},
        {'stage': 2, 'name': '主动vs内疚', 'age': '3-6岁', 'virtue': '目的', 'conflict': 'initiative_guilt'},
        {'stage': 3, 'name': '勤奋vs自卑', 'age': '6-12岁', 'virtue': '能力', 'conflict': 'industry_inferiority'},
        {'stage': 4, 'name': '同一性vs角色混乱', 'age': '12-18岁', 'virtue': '忠诚', 'conflict': 'identity_confusion'},
        {'stage': 5, 'name': '亲密vs孤独', 'age': '18-40岁', 'virtue': '爱', 'conflict': 'intimacy_isolation'},
        {'stage': 6, 'name': '繁衍vs停滞', 'age': '40-65岁', 'virtue': '关怀', 'conflict': 'generativity_stagnation'},
        {'stage': 7, 'name': '自我完善vs绝望', 'age': '65岁+', 'virtue': '智慧', 'conflict': 'integrity_despair'},
    ]

    def __init__(self):
        # 里克的心理发展阶段（模拟的"心理年龄"）
        self.psychological_age_stage = 5  # 成年早期：亲密vs孤独
        self.stage_resolution = {
            'trust_mistrust': 70,        # 信任倾向
            'autonomy_shame': 60,        # 自主程度
            'initiative_guilt': 55,      # 主动性
            'industry_inferiority': 65,  # 勤奋感
            'identity_confusion': 45,    # 同一性（数值低=混乱度低）
            'intimacy_isolation': 50,    # 亲密能力
            'generativity_stagnation': 35, # 繁衍感
            'integrity_despair': 40,     # 自我完善
        }
        self.identity_status = 'moratorium'  # 同一性状态：弥散/早闭/延缓/获得

    def current_stage(self):
        return self.ERIKSON_STAGES[self.psychological_age_stage]

    def resolve_stage(self, stage_id, positive=True, amount=10):
        """发展阶段的积极/消极解决"""
        stage = self.ERIKSON_STAGES[stage_id]
        conflict = stage['conflict']
        if positive:
            self.stage_resolution[conflict] = _clamp(
                self.stage_resolution[conflict] + amount
            )
        else:
            self.stage_resolution[conflict] = _clamp(
                self.stage_resolution[conflict] - amount, 0, 100
            )

    def get_developmental_summary(self):
        current = self.current_stage()
        unresolved = sum(1 for v in self.stage_resolution.values() if v < 40)
        return {
            'current_stage': current['name'],
            'current_virtue': current['virtue'],
            'psychological_stage': self.psychological_age_stage,
            'identity_status': {
                'diffusion': '同一性弥散',
                'foreclosure': '同一性早闭',
                'moratorium': '同一性延缓',
                'achievement': '同一性获得',
            }.get(self.identity_status, self.identity_status),
            'unresolved_issues': unresolved,
            'stage_scores': {s['name']: round(self.stage_resolution[s['conflict']], 1)
                             for s in self.ERIKSON_STAGES},
        }


class MoralDevelopment:
    """
    道德发展系统 —— 科尔伯格道德发展阶段
    三水平六阶段
    """

    LEVELS = [
        {'level': 0, 'name': '前习俗水平', 'stages': [
            {'stage': 1, 'name': '惩罚与服从定向', 'orientation': 'avoid_punishment'},
            {'stage': 2, 'name': '相对功利定向', 'orientation': 'self_interest'},
        ]},
        {'level': 1, 'name': '习俗水平', 'stages': [
            {'stage': 3, 'name': '寻求认可定向', 'orientation': 'good_boy_nice_girl'},
            {'stage': 4, 'name': '遵守法规定向', 'orientation': 'law_order'},
        ]},
        {'level': 2, 'name': '后习俗水平', 'stages': [
            {'stage': 5, 'name': '社会契约定向', 'orientation': 'social_contract'},
            {'stage': 6, 'name': '普遍伦理定向', 'orientation': 'universal_ethics'},
        ]},
    ]

    def __init__(self):
        self.moral_stage = 3  # 寻求认可定向（习俗水平）
        self.moral_reasoning_level = 1  # 习俗水平
        self.care_orientation = 65.0    # 关怀取向（吉利根）
        self.justice_orientation = 55.0  # 公正取向

    def evaluate_moral_dilemma(self, context=None):
        """评估道德困境的推理方式"""
        context = context or {}
        # 关怀 vs 公正取向
        if context.get('relationship_context', False):
            dominant = 'care'
            score = self.care_orientation
        elif context.get('rules_context', False):
            dominant = 'justice'
            score = self.justice_orientation
        else:
            if self.care_orientation > self.justice_orientation:
                dominant = 'care'
                score = self.care_orientation
            else:
                dominant = 'justice'
                score = self.justice_orientation

        return {
            'moral_stage': self.moral_stage,
            'stage_name': self._get_stage_name(self.moral_stage),
            'level': self.moral_reasoning_level,
            'level_name': self.LEVELS[self.moral_reasoning_level]['name'],
            'dominant_orientation': '关怀取向' if dominant == 'care' else '公正取向',
            'orientation_score': round(score, 1),
        }

    def _get_stage_name(self, stage_num):
        for level in self.LEVELS:
            for s in level['stages']:
                if s['stage'] == stage_num:
                    return s['name']
        return '未知'

    def get_status(self):
        return {
            'stage': self.moral_stage,
            'stage_name': self._get_stage_name(self.moral_stage),
            'level_name': self.LEVELS[self.moral_reasoning_level]['name'],
            'care_orientation': round(self.care_orientation, 1),
            'justice_orientation': round(self.justice_orientation, 1),
        }


class EmergingAdulthood:
    """
    成年初显期系统 —— 18-25岁的特殊发展阶段
    参考：《长大了就会变好吗？》
    核心特征：
    - 身份探索期
    - 不稳定期
    - 自我关注期
    - 夹层感（既非青少年也非完全成人）
    - 充满可能性
    """

    def __init__(self):
        self.in_emerging_adulthood = True
        self.identity_exploration = 55.0      # 身份探索程度
        self.instability = 60.0              # 不稳定性
        self.self_focus = 50.0               # 自我关注程度
        self.feeling_in_between = 65.0       # 夹层感
        self.possibilities = 70.0             # 可能性感知

        # 核心议题
        self.core_issues = {
            'love': 55.0,        # 爱情探索
            'work': 50.0,        # 工作/事业探索
            'worldview': 60.0,   # 世界观探索
            'self': 65.0,        # 自我认识
        }

    def explore(self, domain, amount=10):
        """在某领域进行探索"""
        if domain in self.core_issues:
            self.core_issues[domain] = _clamp(self.core_issues[domain] + amount)
            self.identity_exploration = _clamp(self.identity_exploration + amount * 0.3)

    def get_emerging_status(self):
        avg_issue = sum(self.core_issues.values()) / len(self.core_issues)
        if avg_issue > 70:
            status = '深度探索中'
        elif avg_issue > 50:
            status = '稳定探索'
        elif avg_issue > 30:
            status = '初步探索'
        else:
            status = '探索较少'

        # 五特征综合
        features = [
            ('身份探索', self.identity_exploration),
            ('不稳定性', self.instability),
            ('自我关注', self.self_focus),
            ('夹层感', self.feeling_in_between),
            ('可能性', self.possibilities),
        ]

        return {
            'stage': '成年初显期',
            'exploration_status': status,
            'exploration_depth': round(avg_issue, 1),
            'five_features': {name: round(val, 1) for name, val in features},
            'core_issues': {k: round(v, 1) for k, v in self.core_issues.items()},
            'dominant_issue': max(self.core_issues.items(), key=lambda x: x[1])[0],
        }


class DevelopmentalSystem:
    """
    L9 发展心理学系统总控
    整合发展阶段、道德发展、成年初显期
    """

    def __init__(self):
        self.stages = DevelopmentalStages()
        self.moral = MoralDevelopment()
        self.emerging = EmergingAdulthood()

    def get_developmental_profile(self):
        return {
            'erikson': self.stages.get_developmental_summary(),
            'moral': self.moral.get_status(),
            'emerging_adulthood': self.emerging.get_emerging_status(),
        }

    def tick(self):
        pass  # 发展变化是长期的，不需要高频tick

    def get_status(self):
        return {
            'current_stage': self.stages.current_stage()['name'],
            'moral_stage': self.moral.get_status()['stage_name'],
            'identity_status': self.stages.get_developmental_summary()['identity_status'],
            'emerging_adulthood': self.emerging.get_emerging_status()['exploration_status'],
        }


# ============================================================
# L10 — 心理学核心引擎总控 Psychology Core Engine
# 整合 L1-L9 所有子系统
# ============================================================

