#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""人际关系 + 世界状态（从 src_d_relations.py 迁移）。"""
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

class PermanentBlacklistModule:
    def __init__(self):
        self._data = dm.load("permanent_blacklist", {})
        self._dirty = False
        self._lock = _FakeLock()
        self._start_auto_flush()
        self._repair_data()

    def _repair_data(self):
        with self._lock:
            for uid, info in list(self._data.items()):
                if not isinstance(info, dict):
                    self._data[uid] = {"reason": "数据损坏已修复", "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                       "operator": "unknown", "severity": "medium", "attempts": 1}
                    self._mark_dirty()
                    continue
                if "time" not in info:
                    info["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self._mark_dirty()
                if "reason" not in info:
                    info["reason"] = "未知原因"
                    self._mark_dirty()
                if "attempts" not in info:
                    info["attempts"] = 1
                    self._mark_dirty()

    def _start_auto_flush(self):
        def flush_worker():
            while True:
                time.sleep(60)
                if self._dirty:
                    try:
                        dm.save("permanent_blacklist", self._data)
                        self._dirty = False
                    except Exception as e:
                        logger.error(f"[永久黑名单] 落盘失败: {e}")
        threading.Thread(target=flush_worker, daemon=True).start()

    def _mark_dirty(self):
        self._dirty = True

    def add(self, uid, reason, operator="guardian", severity="medium"):
        uid = str(uid)
        if uid == str(CONFIG["owner_qq"]):
            return False
        with self._lock:
            if uid in self._data:
                self._data[uid]["reason"] = reason
                self._data[uid]["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self._data[uid]["attempts"] = self._data[uid].get("attempts", 0) + 1
                self._data[uid]["severity"] = severity
                self._mark_dirty()
                return True
            self._data[uid] = {
                "reason": reason,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "operator": operator,
                "severity": severity,
                "attempts": 1
            }
            self._mark_dirty()
            return True

    def remove(self, uid):
        uid = str(uid)
        with self._lock:
            if uid in self._data:
                del self._data[uid]
                self._mark_dirty()
                return True
            return False

    def has(self, uid):
        uid = str(uid)
        with self._lock:
            return uid in self._data

    def list(self):
        with self._lock:
            items = []
            for uid, info in self._data.items():
                if isinstance(info, dict) and "time" in info:
                    items.append((uid, info))
                else:
                    info = {"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "reason": "数据损坏已修复", "operator": "unknown", "severity": "medium", "attempts": 1}
                    self._data[uid] = info
                    self._mark_dirty()
                    items.append((uid, info))
            return sorted(items, key=lambda x: x[1]["time"])

    def get_info(self, uid):
        uid = str(uid)
        with self._lock:
            info = self._data.get(uid)
            return info.copy() if info else None

    def is_malicious(self, uid):
        info = self.get_info(uid)
        if not info:
            return False
        return info.get("attempts", 0) >= 3 and info.get("severity") in ["high", "critical"]


perm_blacklist = PermanentBlacklistModule()

# ============================================================
# 十五、守卫系统
# ============================================================

class GuardianModule:
    def __init__(self):
        self._ops_log = defaultdict(lambda: deque(maxlen=50))  # 有界，防止内存泄漏
        self._risk_score = defaultdict(int)
        self._punish_cooldown = defaultdict(float)
        self._lock = _FakeLock()

    def record_operation(self, ws, uid, action, target_uid, gid):
        uid = str(uid)
        if is_owner(uid):
            return "safe"

        with self._lock:
            now = time.time()
            if now - self._punish_cooldown[uid] < 120:
                return "cooldown"

            self._ops_log[uid].append({
                "time": now,
                "action": action,
                "target": str(target_uid) if target_uid else "",
                "gid": str(gid)
            })

            window = CONFIG["guardian"]["window_seconds"]
            cutoff = now - window
            # 保持 deque 类型，过滤过期操作
            self._ops_log[uid] = deque((op for op in self._ops_log[uid] if op["time"] > cutoff), maxlen=50)

            return self._evaluate_risk(ws, uid, action, target_uid, gid)

    def _evaluate_risk(self, ws, uid, action, target_uid, gid):
        ops = self._ops_log[uid]
        op_count = len(ops)

        if op_count < CONFIG["guardian"]["max_ops_per_window"]:
            self._risk_score[uid] = op_count * 5
            return "safe"

        trust_value = trust.get(uid)
        is_low = trust_value < CONFIG["guardian"]["trust_low_threshold"]
        is_high = trust_value > CONFIG["guardian"]["trust_high_threshold"]

        threshold = CONFIG["guardian"]["max_ops_per_window"]
        if is_low:
            threshold = CONFIG["guardian"]["risk_threshold_low_trust"]
        elif is_high:
            threshold = CONFIG["guardian"]["risk_threshold_high_trust"]

        if target_uid and action == "ban" and str(target_uid) == str(CONFIG["owner_qq"]):
            self._execute_punishment(ws, uid, "试图拉黑主人", gid, "critical")
            return "critical"

        if op_count >= threshold:
            severity = "high" if (is_low or op_count >= threshold * 2) else "medium"
            reason = f"{op_count}次操作/{CONFIG['guardian']['window_seconds']}秒内"
            if is_low:
                reason += "，且好感度低"
            self._execute_punishment(ws, uid, reason, gid, severity)
            return "punished"

        self._risk_score[uid] = op_count * 10 + (0 if is_high else 20 if is_low else 10)
        return "safe"

    def _execute_punishment(self, ws, uid, reason, gid, severity="medium"):
        with self._lock:
            self._punish_cooldown[uid] = time.time()
            self._ops_log[uid] = []
            self._risk_score[uid] = 0

        logger.warning(f"[守卫] 处罚 {uid}: {reason} ({severity})")

        if not CONFIG["guardian"]["auto_revoke_admin"] and not CONFIG["guardian"]["auto_blacklist"]:
            _append_audit_log({
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "guardian_warning",
                "operator": uid,
                "reason": reason,
                "severity": severity,
                "group": str(gid)
            })
            return

        revoke_msg = ""
        if CONFIG["guardian"]["auto_revoke_admin"]:
            try:
                if permission.level(uid) >= PERM_ADMIN:
                    permission.set(uid, PERM_USER)
                    revoke_msg = "✅ 已撤销管理员权限"
                else:
                    revoke_msg = "（该用户已不是管理员）"
            except Exception as e:
                logger.error(f"[守卫] 撤销权限失败: {e}")
                revoke_msg = "❌ 撤销权限失败"

        blacklist_msg = ""
        if CONFIG["guardian"]["auto_blacklist"]:
            try:
                blacklist.add(uid)
                trust.update(uid, -10)
                blacklist_msg = "✅ 已加入黑名单"
                if CONFIG["permanent_blacklist"]["auto_add_guardian_bans"]:
                    perm_blacklist.add(uid, reason, "guardian", severity)
                    blacklist_msg += "\n🔒 已加入永久黑名单"
            except Exception as e:
                logger.error(f"[守卫] 拉黑失败: {e}")
                blacklist_msg = "❌ 拉黑失败"

        _append_audit_log({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "type": "guardian_auto_punish",
            "operator": uid,
            "reason": reason,
            "severity": severity,
            "revoke_admin": revoke_msg,
            "blacklist": blacklist_msg,
            "group": str(gid)
        })

        if CONFIG["guardian"]["notify_owner"] and ws:
            notify = (
                f"{'🔴' if severity in ['high','critical'] else '🟠'} 【守卫】自动处罚通知\n"
                f"━━━━━━━━━━━━━━\n"
                f"📋 管理员：{uid}\n"
                f"⚠️ 原因：{reason}\n"
                f"📊 严重程度：{severity.upper()}\n"
                f"\n🛠️ 执行操作：\n{revoke_msg}\n{blacklist_msg}\n"
                f"\n📌 群号：{gid}\n"
                f"（如有异议，私聊「恢复管理员 {uid}」恢复）"
            )
            try:
                send_private(ws, CONFIG["owner_qq"], notify)
            except Exception:
                pass

    def reset_record(self, uid):
        uid = str(uid)
        with self._lock:
            self._ops_log[uid] = []
            self._risk_score[uid] = 0
            self._punish_cooldown[uid] = 0

    def get_status(self, uid):
        uid = str(uid)
        ops = self._ops_log[uid]
        now = time.time()
        window = CONFIG["guardian"]["window_seconds"]
        recent = [op for op in ops if now - op["time"] < window]
        in_cooldown = now - self._punish_cooldown[uid] < 120
        return {
            "recent_ops": len(recent),
            "risk_score": self._risk_score[uid],
            "in_cooldown": in_cooldown,
            "cooldown_remain": max(0, int(120 - (now - self._punish_cooldown[uid]))) if in_cooldown else 0,
            "total_ops": len(ops)
        }


guardian = GuardianModule()

# ============================================================
# 十六、彩蛋系统
# ============================================================

class EggModule:
    def __init__(self):
        d = dm.load("egg", None)
        self._eggs = d if d else [
            {"keywords": ["巧克力"], "reply": "......你怎么知道我喜欢巧克力？（耳朵动了动，尾巴有点开心地晃了晃）"},
            {"keywords": ["打雷", "雷雨"], "reply": "（尾巴紧紧夹在腿间，声音有点抖）......雷、雷声好吵。"},
            {"keywords": ["图书馆", "借书"], "reply": "......你也喜欢去图书馆吗？（眼睛亮了一下，又很快暗下去）我、我最近常去借。"},
            {"keywords": ["小塔"], "reply": "（尾巴微微翘了一下）......小塔是很重要的人。"}
        ]

    def check(self, msg):
        for e in self._eggs:
            for kw in e["keywords"]:
                if kw in msg:
                    return e["reply"]
        return None


egg = EggModule()




# ============================================================
# 十七、技能系统
# ============================================================

class SkillModule:
    def __init__(self):
        d = dm.load("skill", None)
        self._skills = d if d else [
            {"name": "聊天", "desc": "可以陪你聊天，虽然话不多", "permission": 0},
            {"name": "尾巴状态查询", "desc": "问我尾巴怎么样，我会告诉你", "permission": 0},
            {"name": "听你说", "desc": "你想说什么都可以，我会听着", "permission": 0},
            {"name": "拉黑/解封", "desc": "管理黑名单", "permission": 1},
            {"name": "管理员管理", "desc": "添加/删除管理员", "permission": 2},
            {"name": "退群", "desc": "让我从某个群退出", "permission": 2}
        ]

    def list_text(self, perm=0):
        t = "【我会的事情】\n"
        i = 1
        for s in self._skills:
            if s.get("permission", 0) <= perm:
                t += f"{i}. {s['name']}：{s['desc']}\n"
                i += 1
        t += "\n......其他的我不太会。（声音小小的）"
        return t


skill = SkillModule()

# ============================================================
# 十八、关系阶段管理器
# ============================================================

class RelationshipManager:
    def __init__(self):
        self._data = dm.load("relationship_data", {})
        self._dirty = False
        self._lock = _FakeLock()
        self._stages = CONFIG["relationship_stages"]

    def get_stage(self, uid):
        uid = str(uid)
        trust_value = trust.get(uid)
        current_stage = "stranger"
        current_threshold = 0
        for stage_key, stage_info in self._stages.items():
            threshold = stage_info["threshold"]
            if trust_value >= threshold and threshold >= current_threshold:
                current_stage = stage_key
                current_threshold = threshold
        return current_stage

    def get_stage_info(self, uid):
        uid = str(uid)
        stage_key = self.get_stage(uid)
        stage_info = self._stages.get(stage_key, self._stages["stranger"])
        trust_value = trust.get(uid)
        return {
            "key": stage_key,
            "name": stage_info["name"],
            "desc": stage_info["desc"],
            "threshold": stage_info["threshold"],
            "current_trust": trust_value,
            "next_stage": self._get_next_stage(stage_key),
            "progress": self._get_progress(uid, stage_key),
        }

    def _get_next_stage(self, current_key):
        keys = list(self._stages.keys())
        for i, key in enumerate(keys):
            if key == current_key and i + 1 < len(keys):
                return keys[i + 1]
        return None

    def _get_progress(self, uid, current_key):
        uid = str(uid)
        trust_value = trust.get(uid)
        stage_info = self._stages.get(current_key)
        if not stage_info:
            return 0
        next_stage_key = self._get_next_stage(current_key)
        if not next_stage_key:
            return 100
        next_threshold = self._stages[next_stage_key]["threshold"]
        current_threshold = stage_info["threshold"]
        if next_threshold <= current_threshold:
            return 100
        progress = (trust_value - current_threshold) / (next_threshold - current_threshold)
        return max(0, min(100, int(progress * 100)))

    def get_behavior_style(self, uid):
        stage_key = self.get_stage(uid)
        styles = {
            "stranger": {"warmth": 0.1, "length": "short", "initiate": 0.01, "tags": ["保持距离"]},
            "acquaintance": {"warmth": 0.25, "length": "short", "initiate": 0.03, "tags": ["客气"]},
            "familiar": {"warmth": 0.4, "length": "medium", "initiate": 0.05, "tags": ["日常"]},
            "friend": {"warmth": 0.6, "length": "medium", "initiate": 0.08, "tags": ["信任"]},
            "close_friend": {"warmth": 0.75, "length": "long", "initiate": 0.12, "tags": ["很信任"]},
            "trusted": {"warmth": 0.9, "length": "long", "initiate": 0.15, "tags": ["几乎不设防"]},
            "soulmate": {"warmth": 1.0, "length": "long", "initiate": 0.2, "tags": ["灵魂伴侣"]},
        }
        return styles.get(stage_key, styles["stranger"])

    def get_stage_prompt(self, uid):
        info = self.get_stage_info(uid)
        style = self.get_behavior_style(uid)
        prompt = f"\n【你们的关系】\n- 阶段：{info['name']}\n- 描述：{info['desc']}\n- 目前好感度：{info['current_trust']}/1000\n"
        if info["next_stage"]:
            prompt += f"- 下一阶段：{self._stages[info['next_stage']]['name']}（进度 {info['progress']}%）\n"
        prompt += f"- 建议互动方式：{'、'.join(style['tags'])}\n"
        return prompt


relationship_manager = RelationshipManager()

# ============================================================
# 十九、记忆权重系统
# ============================================================

class MemoryWeightSystem:
    def __init__(self):
        self._data = dm.load("memory_weight", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _apply_forgetting(self, uid=None):
        with self._lock:
            uids = [uid] if uid else list(self._data.keys())
            for uid_str in uids:
                if uid_str not in self._data:
                    continue
                memories = self._data[uid_str]
                new_memories = []
                for mem in memories:
                    weight = mem.get("weight", 0.5)
                    decay = CONFIG["memory_weight_decay"]
                    days_since = (datetime.now() - datetime.strptime(
                        mem.get("time", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                        "%Y-%m-%d %H:%M:%S"
                    )).total_seconds() / 86400
                    new_weight = weight * (decay ** days_since)
                    if mem.get("important", False):
                        new_weight *= 1.5
                    mem["weight"] = new_weight
                    if new_weight > CONFIG["memory_forgetting_threshold"]:
                        new_memories.append(mem)
                if len(new_memories) != len(memories):
                    self._data[uid_str] = new_memories
                    self._mark_dirty()

    def _mark_dirty(self):
        self._dirty = True

    def add_memory(self, uid, content, weight=0.5, important=False, tags=None):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = []
            self._data[uid].append({
                "content": content,
                "weight": weight,
                "important": important,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "tags": tags or []
            })
            if len(self._data[uid]) > 200:
                self._data[uid] = sorted(self._data[uid], key=lambda x: x["weight"], reverse=True)[:200]
            self._mark_dirty()
            return True

    def get_top_memories(self, uid, limit=5, min_weight=0.2):
        uid = str(uid)
        self._apply_forgetting(uid)
        if uid not in self._data:
            return []
        with self._lock:
            memories = [m for m in self._data.get(uid, []) if m.get("weight", 0) > min_weight]
            return sorted(memories, key=lambda x: x.get("weight", 0), reverse=True)[:limit]

    def get_memories_by_tag(self, uid, tag, limit=5):
        uid = str(uid)
        self._apply_forgetting(uid)
        if uid not in self._data:
            return []
        with self._lock:
            memories = [m for m in self._data.get(uid, []) if tag in m.get("tags", [])]
            return sorted(memories, key=lambda x: x.get("weight", 0), reverse=True)[:limit]

    def recall(self, uid, query, limit=3):
        uid = str(uid)
        self._apply_forgetting(uid)
        if uid not in self._data:
            return []
        with self._lock:
            memories = self._data.get(uid, [])
            scored = []
            for mem in memories:
                content = mem.get("content", "").lower()
                query_lower = query.lower()
                score = sum(1 for word in query_lower.split() if word in content)
                final_score = score * mem.get("weight", 0.5)
                if final_score > 0:
                    scored.append((final_score, mem))
            scored.sort(reverse=True, key=lambda x: x[0])
            return [mem for _, mem in scored[:limit]]


memory_weight = MemoryWeightSystem()

# ============================================================
# 二十、标签自动提取器
# ============================================================

class TagExtractor:
    def __init__(self):
        self._data = dm.load("tag_data", {})
        self._dirty = False
        self._lock = _FakeLock()
        self._stopwords = {"的", "了", "是", "我", "你", "他", "她", "它", "们", "这", "那", "啊", "呢", "吧", "吗",
                           "也", "都", "和", "与", "或", "而", "但", "就", "又", "还", "在", "有", "把", "被", "给",
                           "让", "使", "去", "来", "上", "下", "中", "内", "外", "前", "后", "左", "右"}

    def _mark_dirty(self):
        self._dirty = True

    def extract_from_text(self, uid, text):
        uid = str(uid)
        if uid not in self._data:
            self._data[uid] = {"words": {}, "tags": []}
        words = []
        for match in re.findall(r'[\u4e00-\u9fa5]{2,4}', text):
            if match not in self._stopwords and len(match) >= 2:
                words.append(match)
        with self._lock:
            for word in words:
                self._data[uid]["words"][word] = self._data[uid]["words"].get(word, 0) + 1
            for word, count in list(self._data[uid]["words"].items()):
                if count >= 5 and word not in self._data[uid]["tags"]:
                    self._data[uid]["tags"].append(word)
                    self._mark_dirty()
            self._mark_dirty()

    def get_tags(self, uid):
        uid = str(uid)
        if uid not in self._data:
            return []
        return self._data[uid].get("tags", [])

    def get_tag_summary(self, uid):
        tags = self.get_tags(uid)
        if not tags:
            return ""
        uid = str(uid)
        words = self._data[uid].get("words", {})
        sorted_tags = sorted(tags, key=lambda x: words.get(x, 0), reverse=True)
        return f"\n【用户标签】\n- 关注话题：{'、'.join(sorted_tags[:5])}\n"


tag_extractor = TagExtractor()

# ============================================================
# 二十一、世界状态系统
# ============================================================

class WorldState:
    def __init__(self):
        self._data = dm.load("world_state_data", {})
        self._dirty = False
        self._lock = _FakeLock()
        self._start_daily_update()
        self._activities = [
            "在窗边看书", "在沙发上发呆", "听歌", "整理书架",
            "泡了一杯茶", "在阳台看云", "翻旧照片", "写日记",
            "在图书馆借书", "在公园散步", "窝在椅子里听雨声",
            "闭着眼睛晒太阳", "在整理收藏盒里的东西", "发呆看窗外",
        ]
        self._status_templates = {
            "morning": ["刚醒，还有点迷糊", "在慢慢吃早餐", "在窗边看晨光"],
            "afternoon": ["在看书", "在做杂事", "在发呆"],
            "dusk": ["在看晚霞", "在窗边看天色暗下来", "在听音乐"],
            "evening": ["在听音乐", "在回想一天的事", "在台灯下翻书"],
            "night": ["该休息了", "睡不着，在听雨声", "在回忆今天"],
        }

    def _start_daily_update(self):
        def update_worker():
            while True:
                time.sleep(3600)
                self._update_today()
        threading.Thread(target=update_worker, daemon=True).start()

    def _update_today(self):
        today = datetime.now().strftime("%Y-%m-%d")
        with self._lock:
            if "last_update" not in self._data or self._data["last_update"] != today:
                self._data["last_update"] = today
                self._data["activity"] = random.choice(self._activities)
                self._data["mood"] = "平静"
                self._mark_dirty()

    def _mark_dirty(self):
        self._dirty = True

    def _get_time_state(self):
        hour = datetime.now().hour
        if 5 <= hour < 12:
            return "morning"
        elif 12 <= hour < 17:
            return "afternoon"
        elif 17 <= hour < 19:
            return "dusk"
        elif 19 <= hour < 22:
            return "evening"
        else:
            return "night"

    def get_current_state(self):
        self._update_today()
        time_state = self._get_time_state()
        status_templates = self._status_templates.get(time_state, self._status_templates["afternoon"])
        status = random.choice(status_templates)
        return {
            "activity": self._data.get("activity", "在发呆"),
            "status": status,
            "time_state": time_state,
            "mood": self._data.get("mood", "平静"),
            "last_update": self._data.get("last_update", ""),
        }

    def get_state_prompt(self):
        state = self.get_current_state()
        return f"\n【里克当前的状态】\n- 正在做的事：{state['activity']}\n- 目前状态：{state['status']}\n"

    def get_activity_message(self):
        state = self.get_current_state()
        return f"（刚才在{state['activity']}……）"


# 阶段11修复：WorldState() 已被 memory_time.WorldStateHub() 取代，避免重复赋值
# context.world_state = WorldState()

# ============================================================
# 二十二、话题追踪器
# ============================================================

class TopicTracker:
    def __init__(self):
        self._data = dm.load("topic_tracker_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def update_topic(self, uid, topic, confidence=0.7):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"current": None, "history": [], "context": "", "topic_counts": {}}
            self._data[uid]["current"] = topic
            self._data[uid]["history"].append({
                "topic": topic,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "confidence": confidence
            })
            if len(self._data[uid]["history"]) > 20:
                self._data[uid]["history"] = self._data[uid]["history"][-20:]
            # ---- 话题疲劳度计数 ----
            counts = self._data[uid].setdefault("topic_counts", {})
            counts[topic] = counts.get(topic, 0) + 1
            # 保留最近 50 个话题的计数
            if len(counts) > 50:
                sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
                self._data[uid]["topic_counts"] = dict(sorted_counts[:50])
            self._mark_dirty()

    def get_current_topic(self, uid):
        uid = str(uid)
        if uid not in self._data:
            return None
        return self._data[uid].get("current")

    def get_fatigue_context(self, uid):
        """话题疲劳度——如果同一话题聊太多次，注入'有点腻了'的感觉，让对话更有真人感"""
        uid = str(uid)
        if uid not in self._data:
            return ""
        counts = self._data[uid].get("topic_counts", {})
        current = self._data[uid].get("current")
        if not counts or not current:
            return ""
        current_count = counts.get(current, 0)
        if current_count >= 8:
            return f"\n（这个话题——{current}——你们已经聊了好多次了，你有点腻了，想换个方向聊。）\n"
        elif current_count >= 5:
            return f"\n（{current}这个话题聊了好几遍了，你有点提不起劲。）\n"
        elif current_count >= 3:
            return f"\n（{current}这个话题最近聊过几次了。）\n"
        return ""

    def get_topic_context(self, uid):
        uid = str(uid)
        if uid not in self._data or not self._data[uid].get("current"):
            return ""
        current = self._data[uid]["current"]
        history = self._data[uid]["history"][-3:]
        history_str = " → ".join([h["topic"] for h in history]) if len(history) > 1 else current
        result = f"\n【话题追踪】\n- 当前话题：{current}\n- 话题脉络：{history_str}\n"
        # ---- 追加话题疲劳度叙事 ----
        result += self.get_fatigue_context(uid)
        return result


topic_tracker = TopicTracker()

# ============================================================
# 二十三、自我总结系统
# ============================================================

class SelfSummary:
    def __init__(self):
        self._data = dm.load("self_summary_data", {})
        self._dirty = False
        self._lock = _FakeLock()
        self._start_periodic_summary()

    def _start_periodic_summary(self):
        def summary_worker():
            while True:
                time.sleep(86400)
                self._generate_summary()
        threading.Thread(target=summary_worker, daemon=True).start()

    def _mark_dirty(self):
        self._dirty = True

    def _generate_summary(self):
        today = datetime.now().strftime("%Y-%m-%d")
        with self._lock:
            if "last_summary" not in self._data or self._data["last_summary"] != today:
                summary = self._compile_summary()
                self._data[today] = {
                    "summary": summary,
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                self._data["last_summary"] = today
                self._mark_dirty()
                logger.info(f"[自我总结] 生成每日总结: {summary[:30]}...")

    def _compile_summary(self):
        # ---- 三模型协同：用 summary 槽位生成自然每日总结 ----
        context_parts = []

        # 今日日记——优先用内存数据，避免读到未 flush 的旧数据
        try:
            diary_data = daily_diary._data if daily_diary._data else dm.load("daily_diary", {})
            if diary_data:
                latest = None
                for uid, entries in diary_data.items():
                    if entries:
                        for date, entry in entries.items():
                            if not latest or date > latest[0]:
                                latest = (date, entry)
                if latest:
                    context_parts.append(f"今日日记：{latest[1].get('content', '平淡地度过')}")
        except Exception:
            pass

        # 高权重记忆
        try:
            if CONFIG["modules"]["memory"]:
                for uid_str in list(user_profiles._data.keys())[:3]:
                    tops = memory_weight.get_top_memories(int(uid_str), limit=2, min_weight=0.2)
                    for m in tops:
                        context_parts.append(f"- 记忆：{m['content'][:40]}")
        except Exception:
            pass

        # 人格状态
        try:
            if CONFIG["modules"].get("personality"):
                for uid_str in list(user_profiles._data.keys())[:1]:
                    context_parts.append(f"状态：{personality.get_summary(int(uid_str))}")
        except Exception:
            pass

        # 近期情绪
        try:
            if CONFIG["modules"]["emotion"]:
                for uid_str in list(user_profiles._data.keys())[:2]:
                    s = context.get("emotion_isolated").get_mood_summary(int(uid_str), "private")
                    context_parts.append(f"对某人的情绪：{s['dominant']}")
        except Exception:
            pass

        if not context_parts:
            context_parts.append("今天比较安静，没有特别的事。")

        prompt = SYSTEM_PROMPT + "\n\n【今日素材】\n" + "\n".join(context_parts) + \
            "\n\n请用第一人称写一段今天的自我总结。你的口吻，简短真实，80字以内。" \
            "像是在心里默默回顾今天。只写总结内容。"

        summary = _v6_call("summary", prompt, [])
        if summary and len(summary) > 5:
            return summary.strip()

        # 回退到模板
        templates = [
            "今天是平静的一天。",
            "今天没什么特别的事。",
            "今天在发呆中度过。",
            "今天又是在看书的一天。",
            "今天天气很好，心情也不错。",
            "今天见了几个人，说了几句话，就这样。",
            "今天有点累，但也不算坏。",
        ]
        return random.choice(templates)

    def get_summary(self, date=None):
        if date:
            return self._data.get(date, {}).get("summary", "没有那天的总结")
        return self._data.get("last_summary", "还没有总结")

    def get_recent_summaries(self, limit=3):
        items = [(k, v) for k, v in self._data.items() if k not in ["last_summary"]]
        items.sort(reverse=True)
        return items[:limit]


self_summary = SelfSummary()

# ============================================================
# 二十四、里程碑/关系事件记录
# ============================================================

class MilestoneTracker:
    def __init__(self):
        self._data = dm.load("milestone_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def check_and_record(self, uid, event_type, context=""):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {}
            if event_type in self._data[uid]:
                return False, self._data[uid][event_type]

            # ---- 三模型协同：用 milestone 槽位生成自然的关系事件描述 ----
            rich_context = context
            try:
                # 收集上下文
                ctx_parts = [f"事件类型：{event_type}"]
                if context:
                    ctx_parts.append(f"原始描述：{context}")
                try:
                    rel = user_profiles._data.get(uid, {})
                    good = rel.get("good", 0)
                    ctx_parts.append(f"好感度：{good}")
                except Exception:
                    pass
                try:
                    tops = memory_weight.get_top_memories(uid, limit=2, min_weight=0.2)
                    if tops:
                        ctx_parts.append("相关记忆：" + "; ".join(m['content'][:30] for m in tops))
                except Exception:
                    pass

                prompt = SYSTEM_PROMPT + "\n\n【事件信息】\n" + "\n".join(ctx_parts) + \
                    "\n\n这是一个关系里程碑事件。请用你的口吻（第一人称），" \
                    "用一句话描述你对这件事的感受。简短真实，符合你的性格。只说这一句。"

                llm_context = _v6_call("milestone", prompt, [])
                if llm_context and len(llm_context) > 3:
                    rich_context = llm_context.strip()
            except Exception:
                pass

            event_data = {
                "type": event_type,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "context": rich_context
            }
            self._data[uid][event_type] = event_data
            self._mark_dirty()
            user_profiles.add_milestone(uid, event_type)
            logger.info(f"[里程碑] {uid} - {event_type}（三模型协同）")
            return True, event_data

    def get_milestones(self, uid):
        uid = str(uid)
        if uid not in self._data:
            return []
        return list(self._data[uid].values())

    def get_milestone_prompt(self, uid):
        milestones = self.get_milestones(uid)
        if not milestones:
            return ""
        prompt = "\n【关系记录】\n"
        for m in milestones[-5:]:
            prompt += f"- {m['time'][:10]} {m['type']}：{m.get('context', '')}\n"
        return prompt


milestone_tracker = MilestoneTracker()

# ============================================================
# 二十五、心情计算器
# ============================================================

class EmotionGiftLink:
    def __init__(self):
        self._cache = {}

    def get_daily_mood(self, uid, hour=None):
        if hour is None:
            hour = datetime.now().hour
        today = datetime.now().strftime("%Y-%m-%d")
        uid = str(uid)
        seed_str = f"{today}_{uid}_rikka_mood"
        seed_hash = hashlib.sha256(seed_str.encode()).hexdigest()
        seed = int(seed_hash[:8], 16) / 0xFFFFFFFF
        hour_factor = self._get_hour_factor(hour)
        day_offset = (seed - 0.5) * 0.3
        mood = max(0.6, min(1.4, 1.0 + hour_factor + day_offset + random.uniform(-0.03, 0.03)))
        mood = round(mood, 2)
        return mood

    def _get_hour_factor(self, hour):
        if 6 <= hour < 9:
            return -0.1 + (hour - 6) * 0.033
        elif 9 <= hour < 12:
            return 0.05 + (hour - 9) * 0.033
        elif 12 <= hour < 14:
            return 0.2
        elif 14 <= hour < 17:
            return 0.1
        elif 17 <= hour < 19:
            return -0.05
        elif 19 <= hour < 22:
            return 0.05
        else:
            return -0.2

    def get_mood_description(self, mood):
        if mood >= 1.3:
            return "心情很好，尾巴轻快地晃着"
        elif mood >= 1.1:
            return "心情不错，耳朵微微竖起"
        elif mood >= 0.9:
            return "心情普通，安静地待着"
        elif mood >= 0.7:
            return "心情有点低落，尾巴耷拉着"
        else:
            return "心情很差，不想说话"

    def get_mood_emoji(self, mood):
        if mood >= 1.3:
            return "☀️"
        elif mood >= 1.1:
            return "🌤️"
        elif mood >= 0.9:
            return "⛅"
        elif mood >= 0.7:
            return "🌥️"
        else:
            return "🌧️"


mood_link = EmotionGiftLink()

# ============================================================
# 二十六、礼物系统 V5
# ============================================================

class GiftSystemV5:
    GIFTS = {
        "巧克力": {"emoji": "🍫", "effect": {"joy": 8, "calm": 3}, "good_base": 3, "cooldown": 3600, "daily_limit": 2},
        "牛奶": {"emoji": "🥛", "effect": {"calm": 6, "nervous": -4}, "good_base": 2, "cooldown": 1800, "daily_limit": 3},
        "花": {"emoji": "🌷", "effect": {"joy": 10, "shy": 8}, "good_base": 5, "cooldown": 7200, "daily_limit": 1},
        "书签": {"emoji": "📑", "effect": {"curious": 10, "calm": 5}, "good_base": 4, "cooldown": 7200, "daily_limit": 1},
        "信": {"emoji": "💌", "effect": {"shy": 15, "joy": 8}, "good_base": 8, "cooldown": 86400, "daily_limit": 1},
    }

    GIFT_ALIASES = {
        "巧克力": ["巧克力", "choco", "🍫"],
        "牛奶": ["牛奶", "milk", "🥛"],
        "花": ["花", "鲜花", "🌷", "flower"],
        "书签": ["书签", "bookmark", "📑"],
        "信": ["信", "letter", "💌"],
    }

    def __init__(self):
        self._data = dm.load("gift_v5_data", {})
        self._cooldowns = {}
        self._daily_counts = dm.load("gift_daily_counts", {})
        self._last_reset = dm.load("gift_last_reset", {})
        self._lock = _FakeLock()
        self._dirty = False
        self._start_auto_flush()
        self._check_reset()

    def _start_auto_flush(self):
        def flush_worker():
            while True:
                time.sleep(60)
                if self._dirty:
                    try:
                        dm.save("gift_v5_data", self._data)
                        dm.save("gift_daily_counts", self._daily_counts)
                        dm.save("gift_last_reset", self._last_reset)
                        self._dirty = False
                    except Exception as e:
                        logger.error(f"[礼物V5] 落盘失败: {e}")
        threading.Thread(target=flush_worker, daemon=True).start()

    def _mark_dirty(self):
        self._dirty = True

    def _check_reset(self):
        today = datetime.now().strftime("%Y-%m-%d")
        for uid in list(self._daily_counts.keys()):
            if self._last_reset.get(uid) != today:
                self._daily_counts[uid] = {}
                self._last_reset[uid] = today
                self._mark_dirty()

    def _ensure_user(self, uid):
        uid = str(uid)
        if uid not in self._data:
            self._data[uid] = {"total_gifts": 0, "gift_count": {}, "consecutive_days": 0, "last_gift_day": ""}
        return self._data[uid]

    def _match_gift(self, text):
        for name, aliases in self.GIFT_ALIASES.items():
            if text in aliases:
                return name
        return None

    def _calc_effectiveness(self, current_good, gift_name, uid):
        if current_good <= 200:
            factor = 1.0
        elif current_good <= 500:
            factor = 0.8
        elif current_good <= 800:
            factor = 0.5
        elif current_good <= 950:
            factor = 0.3
        else:
            factor = 0.1
        data = self._ensure_user(uid)
        count = data["gift_count"].get(gift_name, 0)
        repeat_factor = max(0.2, 1 - count * 0.15)
        return max(CONFIG["gift_system"]["min_effectiveness"], factor * repeat_factor)

    def give_gift(self, uid, gift_name, current_good):
        uid = str(uid)
        matched = self._match_gift(gift_name)
        if not matched:
            return False, "（歪了歪头）……这个礼物我不认识耶。", 0, None

        gift = self.GIFTS[matched]

        global_cooldown_key = f"global_{uid}"
        now = time.time()
        if global_cooldown_key in self._cooldowns:
            elapsed = now - self._cooldowns[global_cooldown_key]
            if elapsed < CONFIG["gift_system"]["global_cooldown"]:
                remain = int(CONFIG["gift_system"]["global_cooldown"] - elapsed)
                return False, f"（尾巴轻轻摇了摇）……你今天已经送过东西了，等{remain//3600}小时再说吧。", 0, None

        today = datetime.now().strftime("%Y-%m-%d")
        daily_total_key = f"total_{today}"
        daily_total = self._daily_counts.get(daily_total_key, 0)
        daily_limit = CONFIG["gift_system"]["daily_gift_limit"]
        if daily_total >= daily_limit:
            return False, "（轻轻摇头）……今天收到的礼物已经够多了，明天再说吧。", 0, None

        mood = mood_link.get_daily_mood(uid)
        mood_modifier = 1 + (mood - 1.0) * 0.5
        effective_good = current_good * mood_modifier

        if effective_good < 50:
            return False, self._get_rejection(mood, "very_low"), 0, None
        if effective_good < 120:
            if random.random() < 0.2 + (mood - 0.6) * 0.5:
                return self._accept_gift(uid, matched, current_good, mood, True)
            return False, self._get_rejection(mood, "low"), 0, None
        if effective_good < 250:
            if random.random() < 0.4 + (mood - 0.6) * 0.4:
                return self._accept_gift(uid, matched, current_good, mood, True)
            return False, self._get_rejection(mood, "medium"), 0, None

        data = self._ensure_user(uid)
        last_day = data.get("last_gift_day", "")
        consecutive = data.get("consecutive_days", 0)

        if last_day == today:
            pass
        elif last_day == (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"):
            consecutive += 1
        else:
            consecutive = 1

        data["consecutive_days"] = consecutive
        data["last_gift_day"] = today

        if consecutive >= 4:
            if random.random() < 0.5:
                return False, "（尾巴轻轻摇了摇）……你最近送我太多了，我有点不知道该说什么……休息几天吧。", 0, None
            else:
                _, reply, _, emotion_delta = self._accept_gift(uid, matched, current_good, mood, False)
                return True, reply + "（虽然不会加好感……但谢谢）", 0, emotion_delta

        return self._accept_gift(uid, matched, current_good, mood, False)

    def _accept_gift(self, uid, gift_name, current_good, mood, unease):
        gift = self.GIFTS[gift_name]
        effectiveness = self._calc_effectiveness(current_good, gift_name, uid)
        if unease:
            effectiveness *= 0.6

        mood_effect = 0.8 + (mood - 0.6) * 0.5
        good_delta = max(1, int(gift["good_base"] * effectiveness * mood_effect))

        emotion_delta = {}
        for emo, val in gift["effect"].items():
            emotion_delta[emo] = max(0, int(val * effectiveness * mood_effect))

        with self._lock:
            data = self._ensure_user(uid)
            data["total_gifts"] += 1
            data["gift_count"][gift_name] = data["gift_count"].get(gift_name, 0) + 1

            today = datetime.now().strftime("%Y-%m-%d")
            daily_total_key = f"total_{today}"
            self._daily_counts[daily_total_key] = self._daily_counts.get(daily_total_key, 0) + 1
            self._cooldowns[f"global_{uid}"] = time.time()

            self._mark_dirty()

        milestone_tracker.check_and_record(uid, "first_gift", f"收到 {gift_name}")

        reply = self._generate_reply(uid, gift_name, current_good, good_delta, mood, unease)
        return True, reply, good_delta, emotion_delta

    def _generate_reply(self, uid, gift_name, current_good, good_delta, mood, unease):
        gift = self.GIFTS[gift_name]

        # 三模型协同——用 gift 槽位生成自然的收礼回复
        mood_desc = "很好" if mood >= 1.3 else "不错" if mood >= 1.1 else "平静" if mood >= 0.9 else "低落" if mood >= 0.7 else "很差"
        relation_desc = "不太熟" if current_good < 300 else "一般" if current_good < 600 else "亲近"

        prompt = SYSTEM_PROMPT + f"""
你收到了{gift['emoji']}{gift_name}。
你当前心情：{mood_desc}
你们的关系：{relation_desc}
{"你有些不安和紧张。" if unease else ""}

请用你的口吻说一句话，表达收到礼物的反应。要自然、简短、符合你的性格。只说一句话，不要解释。"""

        reply = _v6_call("gift", prompt, [])
        if reply and len(reply) > 2:
            return reply

        # 回退模板
        if mood >= 1.3:
            prefix = "（心情很好，尾巴愉快地晃着）"
        elif mood >= 1.1:
            prefix = "（心情不错，耳朵微微竖起）"
        elif mood >= 0.9:
            prefix = ""
        elif mood >= 0.7:
            prefix = "（有点低落，但还是）"
        else:
            prefix = "（心情很差，但勉强）"

        if unease:
            replies = [f"{prefix}……谢、谢谢{gift['emoji']}……", f"{prefix}（小心翼翼地收下）……你、你不用这样的……"]
        elif current_good < 300:
            replies = [f"{prefix}（接过{gift['emoji']}{gift_name}）……谢谢。", f"{prefix}（耳朵动了动）……你总是带东西来……"]
        elif current_good < 600:
            replies = [f"{prefix}（接过{gift['emoji']}{gift_name}，尾巴轻轻摇了摇）……谢谢！", f"{prefix}（眼睛亮了一下）……是{gift_name}……"]
        else:
            replies = [f"{prefix}（接过{gift['emoji']}{gift_name}，尾巴愉快地晃着）……你又送东西了！", f"{prefix}（笑着收好）……每次你送东西我都好开心。"]

        return random.choice(replies)

    def _get_rejection(self, mood, level):
        if mood < 0.7:
            pool = {"very_low": ["（往后退了两步，声音发抖）……走、走开……", "（紧紧抱着尾巴，不看任何人）……不要……"],
                    "low": ["（别过脸去，声音闷闷的）……不想收。", "（摇头）……走开。"],
                    "medium": ["（疲惫地摇头）……今天不想说话。", "（尾巴耷拉着）……不要。"]}
        elif mood < 0.9:
            pool = {"very_low": ["（尾巴夹紧，声音很小）……不、不用了……", "（犹豫地摇头）……算了吧。"],
                    "low": ["（轻轻摇头）……谢谢，但还是算了。", "（叹了口气）……你收回去吧。"],
                    "medium": ["（别过脸去）……我今天不想收礼物。", "（尾巴轻轻摇了摇）……不用了。"]}
        else:
            pool = {"very_low": ["（不好意思地摇头）……太贵重了，我不能收。", "（笑了笑）……你留着自己用吧。"],
                    "low": ["（轻轻摇头）……今天不想收礼物，但还是谢谢你。", "（耳朵动了动）……改天吧。"],
                    "medium": ["（尾巴轻轻晃了晃）……今天已经收了不少啦。", "（笑了笑）……你总是这么客气。"]}
        return random.choice(pool.get(level, pool["medium"]))

    def get_gift_status(self, uid):
        uid = str(uid)
        if uid not in self._data or not self._data[uid].get("gift_count"):
            return "（还没有收到过礼物呢……）"
        data = self._data[uid]
        result = "【收到的礼物】\n"
        for name, count in data["gift_count"].items():
            emoji = self.GIFTS.get(name, {}).get("emoji", "🎁")
            result += f"{emoji} {name} ×{count}\n"
        result += f"\n合计收到 {data['total_gifts']} 件礼物"
        return result

    def get_today_mood(self, uid):
        mood_name = mood_cycle._data.get("current_mood", "平静")
        intensity = mood_cycle._data.get("current_intensity", 0)
        uid_hash = int(hashlib.sha256(str(uid).encode()).hexdigest()[:4], 16) / 0xFFFF
        uid_offset = (uid_hash - 0.5) * 0.06

        MOOD_RANGE = {
            "开心": (1.15, 1.35), "兴奋": (1.2, 1.4),
            "平静": (0.95, 1.05), "放松": (0.90, 1.00),
            "平淡": (0.85, 0.92), "发呆": (0.82, 0.90),
            "低落": (0.72, 0.82), "疲惫": (0.70, 0.80),
            "难过": (0.62, 0.70), "沉默": (0.60, 0.68),
        }
        low, high = MOOD_RANGE.get(mood_name, (0.90, 1.00))
        base = low + (high - low) * intensity
        coeff = max(0.6, min(1.4, base + uid_offset + random.uniform(-0.02, 0.02)))
        coeff = round(coeff, 2)

        if coeff >= 1.3:
            desc, emoji = "心情很好，尾巴轻快地晃着", "☀️"
        elif coeff >= 1.1:
            desc, emoji = "心情不错，耳朵微微竖起", "🌤️"
        elif coeff >= 0.9:
            desc, emoji = "心情普通，安静地待着", "⛅"
        elif coeff >= 0.7:
            desc, emoji = "心情有点低落，尾巴耷拉着", "🌥️"
        else:
            desc, emoji = "心情很差，不想说话", "🌧️"
        return f"{emoji} {desc}（系数：{coeff}）"


gift_system = GiftSystemV5()

# ============================================================
# 二十七、每日日记
# ============================================================

class DailyDiary:
    def __init__(self):
        self._data = dm.load("daily_diary", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _get_today_key(self):
        return datetime.now().strftime("%Y-%m-%d")

    def record_today(self, uid, emotion_summary, interaction):
        uid = str(uid)
        today = self._get_today_key()
        key = f"diary_{uid}"
        with self._lock:
            if key not in self._data:
                self._data[key] = {}
            if today not in self._data[key]:
                mood = emotion_summary.get("dominant", "平静") if emotion_summary else "平静"
                diary = f"今天心情{mood}，{interaction}。"
                self._data[key][today] = {"content": diary, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "mood": mood}
                self._dirty = True
                dm.save("daily_diary", self._data)
                return diary
        return None

    def get_diary(self, uid, date=None):
        uid = str(uid)
        key = f"diary_{uid}"
        if key not in self._data:
            return "（还没有写日记呢……）"
        if date:
            entry = self._data[key].get(date)
            return entry["content"] if entry else "（那天没有写日记……）"
        entries = sorted(self._data[key].items(), reverse=True)[:7]
        if not entries:
            return "（还没有写过日记呢……）"
        result = "【日记】\n"
        for date, entry in entries:
            result += f"{date}: {entry['content']}\n"
        return result


daily_diary = DailyDiary()

# ============================================================
# 二十八、收藏盒
# ============================================================

class CollectionBox:
    ITEMS = [
        {"name": "一片枫叶", "emoji": "🍁", "desc": "秋天从公园捡的，颜色很漂亮。"},
        {"name": "一枚书签", "emoji": "📑", "desc": "图书馆里找到的，上面写着一句诗。"},
        {"name": "一张糖纸", "emoji": "🍬", "desc": "巧克力糖的包装纸，还有点香。"},
        {"name": "一颗小石头", "emoji": "🪨", "desc": "河边捡的，被水冲得很光滑。"},
        {"name": "一朵干花", "emoji": "🌸", "desc": "压平了夹在书里，还保留着一点颜色。"},
        {"name": "一根羽毛", "emoji": "🪶", "desc": "在公园树下捡到的，应该是鸽子掉落的。"},
        {"name": "一枚贝壳", "emoji": "🐚", "desc": "很小一个，耳廓里能听到风声。"},
        {"name": "一张电影票", "emoji": "🎫", "desc": "某天独自去看的电影，已经忘了剧情。"},
    ]

    def __init__(self):
        self._data = dm.load("collection_box", {})
        self._dirty = False

    def add_item(self, uid):
        uid = str(uid)
        if uid not in self._data:
            self._data[uid] = []
        available = [i for i in self.ITEMS if i["name"] not in [x["name"] for x in self._data[uid]]]
        if not available:
            return None
        item = random.choice(available)
        self._data[uid].append({
            "name": item["name"],
            "emoji": item["emoji"],
            "desc": item["desc"],
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        dm.save("collection_box", self._data)
        return item

    def get_collection(self, uid):
        uid = str(uid)
        if uid not in self._data or not self._data[uid]:
            return "（收藏盒是空的……）"
        result = "【收藏盒】\n"
        for item in self._data[uid]:
            result += f"{item['emoji']} {item['name']}: {item['desc']}\n"
        return result


collection_box = CollectionBox()

# ============================================================
# 二十九、碎碎念
# ============================================================

class RandomMutter:
    MUTTERS = [
        "今天整理书架了……发现好多书都没看完。",
        "刚才在发呆，看着窗外云飘过去……",
        "昨天晚上又没睡好，做了个奇怪的梦。",
        "下午去便利店买了瓶牛奶，回来路上遇到一只猫。",
        "今天有点冷……把厚外套翻出来了。",
        "刚才在听一首老歌，旋律在脑子里转了好久。",
        "今天走路时踩到一片落叶，咔嚓响了一声。",
        "窗外的路灯亮得好早……天又黑得早了。",
        "今天又忘记买巧克力了……唉。",
        "晚上风好大，把窗帘吹得鼓起来。",
        "今天在路边看到一朵很好看的花，不知道叫什么名字。",
        "刚才翻到旧照片了……有点怀念。",
    ]

    def __init__(self):
        self._data = dm.load("mutter_data", {})

    def should_mutter(self, uid):
        uid = str(uid)
        now = time.time()
        last_chat = self._data.get(uid, {}).get("last_chat", 0)
        if last_chat and (now - last_chat) > 7200:
            if random.random() < 0.2:
                self._data[uid]["last_chat"] = now
                dm.save("mutter_data", self._data)
                return True
            else:
                self._data[uid]["last_chat"] = now
                dm.save("mutter_data", self._data)
        return False

    def get_mutter(self):
        return random.choice(self.MUTTERS)

    def record_chat(self, uid):
        uid = str(uid)
        if uid not in self._data:
            self._data[uid] = {}
        self._data[uid]["last_chat"] = time.time()
        dm.save("mutter_data", self._data)


mutter = RandomMutter()

# ============================================================
# 三十、人格引擎
# ============================================================

