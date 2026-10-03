#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""行为学习（从 src_h_behavior.py 迁移）。"""
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

class MemoryModule:
    IMPORTANT_KW = ["喜欢", "讨厌", "害怕", "生日", "记住", "重要", "秘密",
                    "约定", "答应", "承诺", "永远", "第一次", "最", "梦想",
                    "想做", "愿望", "目标", "计划", "决定", "不想", "不能"]

    def __init__(self):
        self._short = dm.load("memory_short", {})
        self._profile = dm.load("memory_profile", {})
        self._event = dm.load("memory_event", {})
        self._cleanup_old()

    def _cleanup_old(self):
        cutoff = datetime.now() - timedelta(days=CONFIG["memory_short_days"])
        cutoff_imp = datetime.now() - timedelta(days=CONFIG["memory_important_days"])
        changed = False
        for uid in list(self._short.keys()):
            msgs = self._short[uid]
            new = []
            for m in msgs:
                try:
                    t = datetime.strptime(m["time"], "%Y-%m-%d %H:%M:%S")
                    imp = m.get("important", False)
                    if imp and t > cutoff_imp:
                        new.append(m)
                    elif not imp and t > cutoff:
                        new.append(m)
                except Exception:
                    new.append(m)
            if len(new) != len(msgs):
                self._short[uid] = new
                changed = True
        if changed:
            dm.save("memory_short", self._short)

    def _judge_importance(self, content, uid=None):
        score = sum(1 for kw in self.IMPORTANT_KW if kw in content)
        if len(content) > 20:
            score += 1
        if "！" in content or "!" in content:
            score += 1
        if "我" in content and ("你" in content or "小塔" in content):
            score += 1
        if uid and CONFIG["modules"]["personality"]:
            state = personality.get_state(uid)
            if state["social_desire"] > 60:
                score += 1
            if state["focus"] > 60:
                score += 1
        return score >= 2

    def add_short(self, uid, role, content, source="unknown"):
        if role == "user" and defense.check_memory_pollution(content):
            return
        if role == "user" and CONFIG["modules"].get("jailbreak_defense") and not jailbreak_defense.is_memory_safe(content):
            return
        u = str(uid)
        if u not in self._short:
            self._short[u] = []
        important = self._judge_importance(content, uid) if role == "user" else False
        self._short[u].append({"role": role, "content": content,
                               "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                               "important": important, "source": source})
        if len(self._short[u]) > CONFIG["memory_short_max"] * 2:
            imp_msgs = [m for m in self._short[u] if m.get("important")]
            normal = [m for m in self._short[u] if not m.get("important")]
            self._short[u] = imp_msgs + normal[-CONFIG["memory_short_max"]:]
        dm.save("memory_short", self._short)

    def get_short(self, uid):
        u = str(uid)
        if u not in self._short:
            return []
        return [{"role": m["role"], "content": m["content"]} for m in self._short[u][-CONFIG["memory_short_max"]:]]

    def get_profile(self, uid):
        return self._profile.get(str(uid), {})

    def update_profile(self, uid, key, value):
        u = str(uid)
        if u not in self._profile:
            self._profile[u] = {}
        self._profile[u][key] = value
        dm.save("memory_profile", self._profile)

    def add_event(self, uid, etype, content):
        u = str(uid)
        if u not in self._event:
            self._event[u] = []
        self._event[u].append({"type": etype, "content": content,
                               "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        if len(self._event[u]) > 50:
            self._event[u] = self._event[u][-50:]
        dm.save("memory_event", self._event)

    def get_events_text(self, uid):
        u = str(uid)
        if u not in self._event or not self._event[u]:
            return ""
        t = "\n【重要的事】\n"
        for ev in self._event[u][-10:]:
            t += f"- {ev.get('content', '')}\n"
        return t

    def get_profile_text(self, uid):
        p = self.get_profile(uid)
        if not p:
            return ""
        t = "\n【关于对方】\n"
        for k, v in p.items():
            t += f"- {k}：{v}\n"
        return t

    def clear(self, uid):
        u = str(uid)
        changed = False
        if u in self._short:
            del self._short[u]
            dm.save("memory_short", self._short)
            changed = True
        if u in self._profile:
            del self._profile[u]
            dm.save("memory_profile", self._profile)
            changed = True
        return changed

    def auto_extract_profile(self, uid, msg):
        if CONFIG["modules"].get("jailbreak_defense") and not jailbreak_defense.is_memory_safe(msg):
            return
        u = str(uid)
        deny_words = ["不喜欢", "才不", "不是", "没有", "假的", "开玩笑", "骗你的", "才没有", "怎么可能", "才怪"]
        if any(w in msg for w in deny_words):
            return
        if msg.endswith("？") or msg.endswith("?") or "吗" in msg[-3:]:
            return
        if "哈哈" in msg or "狗头" in msg or "doge" in msg.lower():
            return
        m = re.search(r'我喜欢([^。！？，\s]+)', msg)
        if m:
            like = m.group(1).strip()
            if 1 < len(like) < 10 and not any(w in like for w in deny_words):
                old = self.get_profile(u).get("喜欢", "")
                if old and like not in old:
                    like = old + "、" + like
                self.update_profile(u, "喜欢", like)
        m = re.search(r'我叫([^。！？，\s]+)', msg)
        if m:
            name = m.group(1).strip()
            if 1 < len(name) < 10:
                self.update_profile(u, "称呼", name)


memory = MemoryModule()

# ============================================================
# 三十四、群记忆系统
# ============================================================

class GroupMemoryModule:
    def __init__(self):
        self._data = dm.load("group_memory", {})

    def _ensure(self, gid):
        g = str(gid)
        if g not in self._data:
            self._data[g] = {"summary": "", "events": [], "members": {}, "last_active": ""}
        return self._data[g]

    def add_chat(self, gid, uid, nickname, content):
        if not CONFIG["modules"]["group_memory"]:
            return
        g = self._ensure(gid)
        u = str(uid)
        if u not in g["members"]:
            g["members"][u] = {"nickname": nickname, "msg_count": 0}
        g["members"][u]["msg_count"] = g["members"][u].get("msg_count", 0) + 1
        g["members"][u]["last_seen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        g["last_active"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dm.save("group_memory", self._data)

    def add_event(self, gid, etype, content):
        g = self._ensure(gid)
        g["events"].append({"type": etype, "content": content, "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        if len(g["events"]) > 30:
            g["events"] = g["events"][-30:]
        dm.save("group_memory", self._data)

    def add_summary(self, gid, summary):
        self.add_event(gid, "summary", summary)

    def get_context(self, gid):
        if not CONFIG["modules"]["group_memory"]:
            return ""
        g = str(gid)
        if g not in self._data:
            return ""
        d = self._data[g]
        t = ""
        if d.get("events"):
            t += "\n【群里最近发生的事】\n"
            for ev in d["events"][-5:]:
                t += f"- {ev.get('content', '')}\n"
        return t


group_memory = GroupMemoryModule()

# ============================================================
# 三十五、生日系统
# ============================================================

class BirthdayModule:
    def __init__(self):
        d = dm.load("birthday", None)
        self._data = d if d else [{"name": "里克的生日", "date": "06-21", "type": "生日"}]

    def today(self):
        t = datetime.now().strftime("%m-%d")
        return [b for b in self._data if b.get("date") == t]


birthday = BirthdayModule()

# ============================================================
# 三十六、消息队列
# ============================================================

class MessageQueue:
    def __init__(self):
        self._q = queue.Queue()
        self._workers = []
        self._ws = None
        self._start()

    def set_ws(self, ws):
        self._ws = ws

    def clear(self):
        try:
            while not self._q.empty():
                self._q.get_nowait()
                self._q.task_done()
        except queue.Empty:
            pass
        logger.info("[队列] 已清空")

    def _start(self):
        for i in range(CONFIG["ai_workers"]):
            t = threading.Thread(target=self._worker, args=(i,), daemon=True)
            t.start()
            self._workers.append(t)
        logger.info(f"[队列] 启动 {CONFIG['ai_workers']} 个AI线程")

    def _worker(self, wid):
        while True:
            try:
                task = self._q.get()
                self._process(task)
                self._q.task_done()
            except Exception as e:
                logger.error(f"[队列] 线程{wid}异常: {e}")
                time.sleep(1)

    def _process(self, task):
        t = task["type"]
        try:
            if t == "private":
                # V6.0: 使用多模型管道
                r = context.pipeline.process(
                    task["uid"], task["msg"],
                    is_owner=task["owner"],
                    extra=task.get("extra", ""),
                )
                if r:
                    send_private(self._ws, task["uid"], r)
                    logger.info(f"[私聊] AI回复: {r[:200]}")
                else:
                    logger.info(f"[私聊] Brain决定不回复 uid={task['uid']}")
            elif t == "group":
                # V6.0: 使用多模型管道（群聊）
                extra = task.get("extra", "") + "\n现在是在群聊里，人很多，你更社恐一点，话更少一点，不要太引人注目。"
                r = context.pipeline.process(
                    task["uid"], task["msg"],
                    is_owner=task["owner"],
                    extra=extra,
                    gid=task["gid"],
                )
                if r:
                    send_group(self._ws, task["gid"], r)
                    logger.info(f"[群聊] AI回复: {r[:30]}...")
                else:
                    logger.info(f"[群聊] Brain决定不回复 uid={task['uid']} gid={task['gid']}")
        except Exception as e:
            logger.exception(f"[队列] 任务失败: {e}")

    def submit_private(self, ws, uid, msg, is_owner, extra=""):
        self._ws = ws
        self._q.put({"type": "private", "ws": ws, "uid": uid, "msg": msg, "owner": is_owner, "extra": extra})

    def submit_group(self, ws, gid, uid, msg, is_owner, extra=""):
        self._ws = ws
        self._q.put({"type": "group", "ws": ws, "gid": gid, "uid": uid, "msg": msg, "owner": is_owner, "extra": extra})


msg_queue = MessageQueue()

# ============================================================
# 三十七、发送函数（含撤回系统）
# ============================================================

def mute_group_member(ws, gid, user_id, duration):
    if not ws or not ws.sock or not ws.sock.connected:
        return False
    try:
        ws.send(json.dumps({"action": "set_group_ban", "params": {"group_id": int(gid), "user_id": int(user_id), "duration": int(duration)}}))
        logger.info(f"[禁言] 群{gid} 用户{user_id} 禁言{duration}秒")
        return True
    except Exception as e:
        logger.error(f"禁言失败: {e}")
        return False


# ============================================================
# 消息撤回系统
# ============================================================

_pending_api_responses = {}
_recent_sent_msgs = []
_recent_group_msg_ids = {}

def delete_msg(ws, message_id):
    if not ws or not ws.sock or not ws.sock.connected:
        return False
    try:
        ws.send(json.dumps({"action": "delete_msg", "params": {"message_id": int(message_id)}}))
        logger.info(f"[撤回] message_id={message_id}")
        return True
    except Exception as e:
        logger.error(f"撤回失败: {e}")
        return False

def _send_with_echo(ws, action, params, timeout=5):
    import uuid
    echo = str(uuid.uuid4())[:8]
    payload = {"action": action, "params": params, "echo": echo}
    try:
        ws.send(json.dumps(payload))
    except Exception as e:
        logger.error(f"发送失败: {e}")
        return None
    deadline = time.time() + timeout
    while time.time() < deadline:
        if echo in _pending_api_responses:
            return _pending_api_responses.pop(echo)
        time.sleep(0.05)
    _pending_api_responses.pop(echo, None)
    return None

def send_private(ws, uid, msg, track=True):
    if not ws or not ws.sock or not ws.sock.connected:
        return
    time.sleep(1.5)
    try:
        if track:
            resp = _send_with_echo(ws, "send_private_msg", {"user_id": int(uid), "message": msg})
            mid = None
            if resp and resp.get("status") == "ok":
                mid = resp.get("data", {}).get("message_id")
            if mid:
                _recent_sent_msgs.append({"mid": mid, "ts": time.time(), "target": f"私聊{uid}", "content": msg[:50]})
                if len(_recent_sent_msgs) > 50:
                    _recent_sent_msgs.pop(0)
                threading.Thread(target=_recall_self_if_needed, args=(mid, msg), daemon=True).start()
            logger.info(f"[私聊] → {uid}: {msg[:30]}... (mid={mid})")
        else:
            ws.send(json.dumps({"action": "send_private_msg", "params": {"user_id": int(uid), "message": msg}}))
            logger.info(f"[私聊] → {uid}: {msg[:30]}...")
    except Exception as e:
        logger.error(f"发送私聊失败: {e}")

def send_group(ws, gid, msg, track=True):
    if not ws or not ws.sock or not ws.sock.connected:
        return
    time.sleep(1.5)
    try:
        if track:
            resp = _send_with_echo(ws, "send_group_msg", {"group_id": int(gid), "message": msg})
            mid = None
            if resp and resp.get("status") == "ok":
                mid = resp.get("data", {}).get("message_id")
            if mid:
                _recent_sent_msgs.append({"mid": mid, "ts": time.time(), "target": f"群{gid}", "content": msg[:50]})
                if len(_recent_sent_msgs) > 50:
                    _recent_sent_msgs.pop(0)
                threading.Thread(target=_recall_self_if_needed, args=(mid, msg), daemon=True).start()
            logger.info(f"[群聊] → {gid}: {msg[:30]}... (mid={mid})")
        else:
            ws.send(json.dumps({"action": "send_group_msg", "params": {"group_id": int(gid), "message": msg}}))
            logger.info(f"[群聊] → {gid}: {msg[:30]}...")
    except Exception as e:
        logger.error(f"发送群聊失败: {e}")

def _should_self_recall(msg_content):
    if not msg_content or len(msg_content) < 2:
        return True
    garbled_patterns = ["â", "Ã", "Â", "ï¿½", "\ufffd", "\\u00", ""]
    for p in garbled_patterns:
        if p in msg_content:
            return True
    truncated_patterns = ["...", "。。。。。", "------"]
    for p in truncated_patterns:
        if msg_content.endswith(p) and len(msg_content) < 10:
            return True
    fallback_markers = ["（我不知道该说什么", "[系统提示]", "ERROR:", "出错了", "请稍后再试"]
    for m in fallback_markers:
        if m in msg_content:
            return True
    return False

def _recall_self_if_needed(mid, content, delay=3):
    time.sleep(delay)
    if _should_self_recall(content):
        wsm = WSM()
        if wsm and wsm._ws:
            delete_msg(wsm._ws, mid)
            logger.info(f"[自撤回] 检测到异常消息已撤回 mid={mid} content={content[:30]}")

def _check_group_violation(ws, gid, uid, text, raw_msg, message_id):
    if not message_id or is_owner(uid):
        return False
    clean = re.sub(r'\[CQ:[^\]]+\]', '', text).strip()
    if not clean:
        return False
    violation_keywords = [
        "加群", "进群", "引流", "兼职", "刷单", "代练", "代打",
        "赌", "博彩", "彩票", "六合彩", "澳门", "赌博",
        "色情", "av", "黄片", "裸聊", "约炮", "一夜情",
        "贩毒", "毒品", "大麻", "冰毒",
        "枪支", "弹药", "管制刀具",
        "贷款", "网贷", "高利贷", "套现",
        "vpn", "翻墙", "科学上网",
        "代购", "走私", " fake", "伪造",
    ]
    clean_lower = clean.lower()
    for kw in violation_keywords:
        if kw in clean_lower:
            if delete_msg(ws, message_id):
                logger.warning(f"[违规撤回] 群{gid} 用户{uid} 触发关键词'{kw}' 已撤回")
                send_group(ws, gid, f"（尾巴警惕地竖起）……群里不允许发这类内容。", track=False)
                return True
            break
    return False

# ============================================================
# 三十八、菜单函数
# ============================================================

def _tail_status():
    return context.get("ReplyBank").get("tail_status")


def _menu(perm=0):
    m = "【功能菜单】\n• 「菜单」/「帮助」：查看功能列表\n• 「尾巴」：看看我的尾巴状态\n• 「清空记忆」：清空我们的聊天记忆\n• 「技能」：看看我会什么\n"
    m += "• 「礼物记录」：查看收到的礼物\n• 「查看日记」：查看日记\n• 「收藏盒」：查看收藏\n• 「今天心情」：查看我今天的心情\n"
    m += "• 「关系」：查看我们之间的关系阶段\n• 「群总结」：总结最近群聊内容\n• 「更新日志」：查看当前版本更新内容\n• 「旧版本更新日志」：查看历史更新记录\n• 「随机梗」：随机输出一个网络梗\n"
    if perm >= 1:
        m += "\n【管理员功能】\n• 「拉黑 + QQ号」：把某人加入黑名单\n• 「解封 + QQ号」：把某人从黑名单移除\n• 「黑名单」：查看黑名单列表\n• 「禁言 + @某人 + 分钟数」：禁言群成员（默认1分钟）\n• 「解除禁言 + @某人」：解除禁言\n• 「撤回 + @某人」：撤回该成员最近一条消息\n• 「撤回」：撤回我最近一条消息\n"
    if perm >= 2:
        m += "\n【主人专属】\n• 「加管理员 + QQ号」：添加管理员\n• 「删管理员 + QQ号」：移除管理员\n• 「管理员列表」：查看所有管理员\n• 「退群 + 群号」：让我从某个群退出\n"
        m += "• 「永久黑名单」：查看永久黑名单\n• 「加入永久黑名单 + QQ号」：永久拉黑\n• 「移出永久黑名单 + QQ号 确认」：移除永久黑名单\n"
        m += "• 「紧急退出」：强制退出测试模式\n• 「绝对重置」：重置所有情绪数据\n• 「进入测试模式」：测试模式\n"
        m += "• 「查看里程碑」：查看关系事件记录\n"
        m += "• 「内心戏」：查看内心想法\n• 「秘密收藏」：查看秘密收藏\n• 「里克日记」：查看里克日记\n"
        m += "• 「真人状态」：查看心理状态\n• 「重置真人状态」：重置心理状态\n"
        m += "\n【V8.0 多模型架构】\n• 「V8帮助」：查看多模型架构说明\n• 「模式 + auto/single/dual/triple」：切换调度模式\n"
        m += "• 「思维日志」：查看Brain思考记录\n• 「路由日志」：查看Router调度决策\n"
        m += "• 「执行日志」：查看Agent后台任务\n• 「管道统计」：查看V8运行数据\n"
        m += "• 「模型配置」：查看三模型槽位和分工\n"
        m += "• 「查看梦境」：查看梦境记录\n• 「主动问候」：触发主动行为\n"
        m += "• 「手动日记」：生成日记\n• 「记忆整理」：清理低价值记忆\n"
    m += "\n......我话不多，有什么事直接说就好。"
    return m


def _generate_menu_image(perm=0):
    import io, base64, math
    from PIL import Image, ImageDraw, ImageFont

    W = 600
    pad = 28
    line_h = 30
    sec_gap = 18
    sec_header_h = 36
    title_area_h = 90
    footer_h = 55

    overlay_bg = (15, 15, 25)
    accent = (233, 69, 96)
    accent2 = (99, 102, 241)
    title_color = (245, 235, 220)
    subtitle_color = (190, 180, 210)
    section_bg = (25, 25, 40)
    section_color = (168, 200, 240)
    item_color = (225, 225, 235)
    desc_color = (140, 140, 160)
    dot_color = (233, 69, 96)
    footer_color = (130, 130, 150)

    font_title = ImageFont.truetype("C:/Windows/Fonts/msyhbd.ttc", 26)
    font_sub = ImageFont.truetype("C:/Windows/Fonts/msyh.ttc", 14)
    font_section = ImageFont.truetype("C:/Windows/Fonts/msyhbd.ttc", 16)
    font_item = ImageFont.truetype("C:/Windows/Fonts/msyh.ttc", 14)
    font_desc = ImageFont.truetype("C:/Windows/Fonts/msyh.ttc", 12)
    font_footer = ImageFont.truetype("C:/Windows/Fonts/msyh.ttc", 13)

    sections = []
    sections.append(("基础功能", accent, [
        ("菜单 / 帮助", "查看功能列表"),
        ("尾巴", "看看我的尾巴状态"),
        ("清空记忆", "清空我们的聊天记忆"),
        ("技能", "看看我会什么"),
        ("礼物记录", "查看收到的礼物"),
        ("查看日记", "查看日记"),
        ("收藏盒", "查看收藏"),
        ("今天心情", "查看我今天的心情"),
        ("关系", "查看我们之间的关系阶段"),
        ("群总结", "总结最近群聊内容"),
        ("更新日志", "查看当前版本更新内容"),
        ("旧版本更新日志", "查看历史更新记录"),
        ("随机梗", "随机输出一个网络梗"),
    ]))
    if perm >= 1:
        sections.append(("管理员功能", accent2, [
            ("拉黑 + QQ号", "把某人加入黑名单"),
            ("解封 + QQ号", "把某人从黑名单移除"),
            ("黑名单", "查看黑名单列表"),
            ("禁言 + @某人 + 分钟数", "禁言群成员"),
            ("解除禁言 + @某人", "解除禁言"),
            ("撤回 + @某人", "撤回该成员消息"),
            ("撤回", "撤回我最近消息"),
        ]))
    if perm >= 2:
        sections.append(("主人专属", (180, 130, 220), [
            ("加/删管理员 + QQ号", "管理管理员"),
            ("管理员列表", "查看所有管理员"),
            ("退群 + 群号", "退出指定群"),
            ("永久黑名单", "永久拉黑管理"),
            ("绝对重置", "重置所有情绪数据"),
            ("查看里程碑", "查看关系事件"),
            ("内心戏 / 秘密收藏", "查看内心想法"),
            ("真人状态", "查看心理状态"),
        ]))
        sections.append(("V8.0 架构", (80, 200, 160), [
            ("V8帮助", "多模型架构说明"),
            ("模式 + auto/single", "切换调度模式"),
            ("思维日志 / 路由日志", "查看思考记录"),
            ("模型配置", "查看三模型槽位"),
            ("查看梦境", "查看梦境记录"),
        ]))

    total_h = title_area_h
    for _, _, items in sections:
        total_h += sec_header_h + len(items) * line_h + sec_gap
    total_h += footer_h

    bg_path = os.path.join(CONFIG["data_dir"], "menu_bg.jpg")
    if os.path.exists(bg_path):
        bg_img = Image.open(bg_path).convert("RGB")
        from PIL import ImageOps
        bg_img = ImageOps.fit(bg_img, (W, total_h), method=Image.LANCZOS, centering=(0.5, 0.3))
    else:
        bg_img = Image.new("RGB", (W, total_h), (25, 25, 40))

    img = bg_img.copy()
    overlay = Image.new("RGBA", (W, total_h), (12, 12, 20, 175))
    img.paste(overlay, (0, 0), overlay)
    draw = ImageDraw.Draw(img)

    draw.rectangle([(0, 0), (W, 5)], fill=accent)
    draw.rectangle([(0, total_h - 3), (W, total_h)], fill=(50, 50, 70))

    y = 22
    draw.text((pad, y), "塔洛斯·里克", font=font_title, fill=title_color)
    y += 34
    draw.text((pad, y), "Tallous Rick V8.0  |  功能菜单", font=font_sub, fill=subtitle_color)
    y = title_area_h

    for sec_name, sec_color, items in sections:
        draw.rounded_rectangle([(pad - 6, y), (W - pad + 6, y + sec_header_h - 2)], radius=8, fill=(*section_bg, 200) if len(section_bg) == 4 else section_bg)
        draw.rounded_rectangle([(pad - 6, y), (pad, y + sec_header_h - 2)], radius=3, fill=sec_color)
        draw.text((pad + 14, y + 7), sec_name, font=font_section, fill=sec_color)
        y += sec_header_h
        for cmd, desc in items:
            cx = pad + 18
            cy = y + 9
            draw.ellipse([(cx, cy), (cx + 5, cy + 5)], fill=dot_color)
            draw.text((pad + 30, y), cmd, font=font_item, fill=item_color)
            desc_x = pad + 270
            if desc_x < W - pad:
                draw.text((desc_x, y + 1), desc, font=font_desc, fill=desc_color)
            y += line_h
        y += sec_gap

    fy = total_h - footer_h + 18
    draw.line([(pad, fy - 6), (W - pad, fy - 6)], fill=(60, 60, 80))
    draw.text((pad, fy), "有什么事直接说就好。", font=font_footer, fill=footer_color)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    b64 = base64.b64encode(buf.getvalue()).decode()
    return f"[CQ:image,file=base64://{b64}]"

# ============================================================
# 三十八点五、V5.5 新功能模块类定义
# ============================================================

class PoutingModule:
    """赌气状态"""
    def __init__(self):
        self._data = dm.load("pouting_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {
                    "is_pouting": False,
                    "pout_reason": "",
                    "pout_since": "",
                    "unmentioned_count": 0,
                    "last_check": "",
                }
                self._mark_dirty()
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def record_mention(self, uid):
        data = self._ensure_user(uid)
        data["unmentioned_count"] = 0
        if data["is_pouting"]:
            data["is_pouting"] = False
            data["pout_reason"] = ""
            data["pout_since"] = ""
            self._mark_dirty()
            return True
        return False

    def record_not_mentioned(self, uid):
        data = self._ensure_user(uid)
        data["unmentioned_count"] += 1
        if data["unmentioned_count"] >= CONFIG["pouting"]["unmentioned_threshold"] and not data["is_pouting"]:
            data["is_pouting"] = True
            data["pout_reason"] = "没人理我"
            data["pout_since"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._mark_dirty()
            return True
        self._mark_dirty()
        return False

    def is_pouting(self, uid):
        data = self._ensure_user(uid)
        if data.get("is_pouting"):
            try:
                since = datetime.strptime(data["pout_since"], "%Y-%m-%d %H:%M:%S")
                if (datetime.now() - since).total_seconds() > CONFIG["pouting"]["duration_minutes"] * 60:
                    data["is_pouting"] = False
                    self._mark_dirty()
                    return False
            except Exception:
                pass
        return data.get("is_pouting", False)

    def get_pout_reply(self, uid):
        return context.get("ReplyBank").get("pout_reply")

    def get_pout_status(self, uid):
        data = self._ensure_user(uid)
        return {
            "is_pouting": data.get("is_pouting", False),
            "reason": data.get("pout_reason", ""),
            "since": data.get("pout_since", ""),
            "count": data.get("unmentioned_count", 0),
        }


class DarkDiaryModule:
    """暗黑日记"""
    def __init__(self):
        self._data = dm.load("dark_diary_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"mode": "normal", "last_entry": ""}
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def get_diary_mode(self, uid, mood_sum):
        data = self._ensure_user(uid)
        if mood_sum < 200:
            data["mode"] = "dark"
            self._mark_dirty()
            return "dark"
        else:
            if data["mode"] == "dark":
                data["mode"] = "normal"
                self._mark_dirty()
            return "normal"

    def get_dark_entry(self, uid, base_content):
        dark_templates = [
            "今天很难过，但没人看出来。",
            "今天不想说话，可是有人一直在说话。",
            "今天很累，累到不想想明天。",
            "今天又一次觉得……自己很多余。",
            "今天想躲起来，但没地方躲。",
            "今天的风很冷，心也是。",
        ]
        return random.choice(dark_templates)

    def get_normal_entry(self, uid, base_content):
        normal_templates = [
            "今天过得还行。",
            "今天没什么特别的事。",
            "今天心情一般，但也没那么差。",
            "今天天气不错，虽然没出门。",
            "今天听到了一首好听的歌。",
        ]
        return random.choice(normal_templates)


class SilentModeModule:
    """不想说话的日子"""
    def __init__(self):
        self._data = dm.load("silent_mode_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"is_silent": False, "silent_until": "", "triggered_today": False, "last_check": ""}
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def check_and_trigger(self, uid):
        today = datetime.now().strftime("%Y-%m-%d")
        data = self._ensure_user(uid)
        if data.get("last_check") == today:
            return data.get("is_silent", False)
        data["last_check"] = today
        if data.get("triggered_today"):
            return data.get("is_silent", False)
        if random.random() < CONFIG["silent_mode"]["probability"]:
            data["is_silent"] = True
            data["silent_until"] = (datetime.now() + timedelta(hours=CONFIG["silent_mode"]["duration_hours"])).strftime("%Y-%m-%d %H:%M:%S")
            data["triggered_today"] = True
            self._mark_dirty()
            return True
        data["triggered_today"] = True
        self._mark_dirty()
        return False

    def is_silent(self, uid):
        data = self._ensure_user(uid)
        if data.get("is_silent"):
            try:
                until = datetime.strptime(data["silent_until"], "%Y-%m-%d %H:%M:%S")
                if datetime.now() > until:
                    data["is_silent"] = False
                    data["silent_until"] = ""
                    self._mark_dirty()
                    return False
            except Exception:
                pass
        return data.get("is_silent", False)

    def get_silent_reply(self, uid):
        return context.get("ReplyBank").get("silent_reply")


class InnerMonologueModule:
    """内心戏"""
    def __init__(self):
        self._data = dm.load("inner_monologue_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"inner_thoughts": [], "last_inner": "", "conflict_count": 0}
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def add_inner_thought(self, uid, thought, context=""):
        data = self._ensure_user(uid)
        data["inner_thoughts"].append({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "thought": thought,
            "context": context,
        })
        if len(data["inner_thoughts"]) > 50:
            data["inner_thoughts"] = data["inner_thoughts"][-50:]
        data["last_inner"] = thought
        self._mark_dirty()

    def get_inner_thoughts(self, uid, limit=10):
        data = self._ensure_user(uid)
        return data["inner_thoughts"][-limit:]

    def get_contradiction(self, uid, spoken, thought):
        data = self._ensure_user(uid)
        data["conflict_count"] += 1
        self._mark_dirty()
        return f"{spoken}（但其实{thought}）"


class RegretModule:
    """冲动发言后后悔"""
    def __init__(self):
        self._data = dm.load("regret_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"last_spontaneous": "", "last_time": "", "regret_count": 0}
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def record_spontaneous(self, uid, message):
        data = self._ensure_user(uid)
        data["last_spontaneous"] = message
        data["last_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._mark_dirty()

    def should_regret(self, uid):
        data = self._ensure_user(uid)
        if not data.get("last_spontaneous"):
            return False
        return random.random() < 0.2

    def get_regret_reply(self, uid):
        data = self._ensure_user(uid)
        data["regret_count"] += 1
        self._mark_dirty()
        return context.get("ReplyBank").get("regret_reply")


class SocialEnergyModule:
    """社交能量值"""
    def __init__(self):
        self._data = dm.load("social_energy_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"energy": 100, "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def _recover(self, uid):
        data = self._ensure_user(uid)
        now = datetime.now()
        try:
            last = datetime.strptime(data["last_update"], "%Y-%m-%d %H:%M:%S")
            minutes = (now - last).total_seconds() / 60
            if minutes > 0:
                recovery = int(minutes * CONFIG["social_energy"]["recover_per_minute"])
                data["energy"] = min(CONFIG["social_energy"]["max_energy"], data["energy"] + recovery)
        except Exception:
            pass
        data["last_update"] = now.strftime("%Y-%m-%d %H:%M:%S")
        self._mark_dirty()
        return data["energy"]

    def consume_energy(self, uid, amount=None):
        if amount is None:
            amount = CONFIG["social_energy"]["consume_per_mention"]
        # ---- 信任值越高，单次消耗越低 ----
        # 陌生人(0-50)：全额消耗
        # 认识(50-150)：95%
        # 熟悉(150-350)：80%
        # 朋友(350-600)：65%
        # 亲密朋友(600-850)：45%
        # 信任的人(850+)：25%
        if CONFIG["modules"].get("trust") and not is_owner(uid):
            t_val = trust.get(uid)
            if t_val >= 850:
                amount = int(amount * 0.25)
            elif t_val >= 600:
                amount = int(amount * 0.45)
            elif t_val >= 350:
                amount = int(amount * 0.65)
            elif t_val >= 150:
                amount = int(amount * 0.80)
            elif t_val >= 50:
                amount = int(amount * 0.95)
            amount = max(1, amount)
        # ---- 人格核心：外向性影响社交能量消耗速度 ----
        try:
            if CONFIG["modules"].get("personality_core", True):
                multiplier = personality_core.get_social_energy_multiplier()
                amount = max(1, int(amount * multiplier))
        except Exception:
            pass
        data = self._ensure_user(uid)
        self._recover(uid)
        data["energy"] = max(0, data["energy"] - amount)
        self._mark_dirty()
        return data["energy"]

    def get_low_threshold(self, uid):
        """信任值越高，能量耗尽门槛越低——高信任用户聊得正热不会被拒之门外"""
        base = 20
        if CONFIG["modules"].get("trust") and not is_owner(uid):
            t_val = trust.get(uid)
            if t_val >= 850:
                return 5
            elif t_val >= 600:
                return 8
            elif t_val >= 350:
                return 12
            elif t_val >= 150:
                return 15
        return base

    def get_energy(self, uid):
        data = self._ensure_user(uid)
        self._recover(uid)
        return data["energy"]

    def get_energy_level(self, uid):
        energy = self.get_energy(uid)
        if energy >= 80:
            return "high"
        elif energy >= 40:
            return "medium"
        else:
            return "low"


class SafeDistanceModule:
    """安全距离"""
    def __init__(self):
        self._data = dm.load("safe_distance_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"distance_level": 0}
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def get_distance(self, uid):
        trust_value = trust.get(uid)
        distance = max(0, min(10, 10 - int(trust_value / 100)))
        self._ensure_user(uid)["distance_level"] = distance
        self._mark_dirty()
        return distance

    def get_reply_length(self, uid):
        distance = self.get_distance(uid)
        if distance <= 2:
            return "long"
        elif distance <= 5:
            return "medium"
        elif distance <= 8:
            return "short"
        else:
            return "very_short"

    def get_warmth(self, uid):
        distance = self.get_distance(uid)
        return max(0, 1 - distance / 10)


class ObserveModule:
    """偷偷观察"""
    def __init__(self):
        self._data = dm.load("observe_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_group(self, gid):
        gid = str(gid)
        with self._lock:
            if gid not in self._data:
                self._data[gid] = {"observations": [], "topics": {}, "last_check": ""}
            return self._data[gid]

    def _mark_dirty(self):
        self._dirty = True

    def observe(self, gid, uid, message):
        data = self._ensure_group(gid)
        data["observations"].append({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "uid": str(uid),
            "content": message[:50],
        })
        if len(data["observations"]) > 200:
            data["observations"] = data["observations"][-200:]
        for word in re.findall(r'[\u4e00-\u9fa5]{2,4}', message):
            if word not in ["的", "是", "了", "我", "你", "他", "她", "们", "这", "那"]:
                data["topics"][word] = data["topics"].get(word, 0) + 1
        self._mark_dirty()

    def get_observation_summary(self, gid, days=7):
        data = self._ensure_group(gid)
        cutoff = datetime.now() - timedelta(days=days)
        cutoff_str = cutoff.strftime("%Y-%m-%d")
        recent = [o for o in data["observations"] if o["time"][:10] >= cutoff_str]
        return recent

    def get_hot_topics(self, gid, limit=5):
        data = self._ensure_group(gid)
        topics = data.get("topics", {})
        sorted_topics = sorted(topics.items(), key=lambda x: -x[1])
        return sorted_topics[:limit]


class GroupBystanderModule:
    """群聊旁观插嘴模块——观察群聊，感兴趣时自然插嘴，聊投入了会忘我"""
    def __init__(self):
        self._data = dm.load("bystander_data", {})
        self._dirty = False
        self._lock = _FakeLock()
        self._context_max = 20

    def _ensure_group(self, gid):
        gid = str(gid)
        with self._lock:
            if gid not in self._data:
                self._data[gid] = {
                    "recent_messages": [],
                    "interject_cooldown": 0,
                    "engagement_level": 0,
                    "total_interjections": 0,
                }
            return self._data[gid]

    def _mark_dirty(self):
        self._dirty = True

    def _default_keywords(self):
        return ["漫画", "小说", "游戏", "料理", "美甲", "猫", "睡觉", "学校", "学习",
                "动漫", "无职转生", "洛琪希", "尾巴", "咖啡", "食物", "电影", "音乐",
                "梦", "回忆", "朋友", "生日", "蛋糕", "布丁", "三明治", "下雨", "雨天",
                "图书馆", "衣服", "卫衣", "睡衣", "发烧", "感冒", "好看", "好吃"]

    def observe_message(self, gid, uid, text, nickname=""):
        """记录群聊消息，返回是否应该插嘴"""
        gid = str(gid)
        data = self._ensure_group(gid)
        logger.debug(f"[旁观者] 群{gid} 收到{nickname or uid}: {text[:30]} 忘我={data['engagement_level']} 冷却={data['interject_cooldown']}")

        data["recent_messages"].append({
            "uid": str(uid),
            "nickname": nickname,
            "text": text[:200],
            "time": datetime.now().strftime("%H:%M"),
        })
        if len(data["recent_messages"]) > self._context_max:
            data["recent_messages"] = data["recent_messages"][-self._context_max:]

        if data["interject_cooldown"] > 0:
            data["interject_cooldown"] -= 1

        data["engagement_level"] = max(0, data["engagement_level"] - 3)
        self._mark_dirty()

        return self._should_interject(gid, uid, text, data)

    def _should_interject(self, gid, uid, text, data):
        if str(uid) == str(CONFIG.get("bot_qq", "")):
            return False
        if data["interject_cooldown"] > 0:
            return False

        # 忘我模式：很投入时高概率回复
        eng = data["engagement_level"]
        if eng >= 70:
            prob = 0.75
        elif eng >= 40:
            prob = 0.35
        else:
            interest = self._evaluate_interest(text)
            base = 0.015
            keyword_bonus = interest * 0.12
            eng_bonus = eng / 300
            prob = base + keyword_bonus + eng_bonus

        hour = datetime.now().hour
        if 0 <= hour < 7:
            prob *= 0.2

        if CONFIG["modules"].get("personality"):
            try:
                ps = personality.get_state(CONFIG.get("owner_qq", uid))
                mood_factor = 0.85 + (ps["social_desire"] / 500) - (ps["fatigue"] / 400)
                prob *= max(0.3, min(1.5, mood_factor))
            except Exception:
                pass

        prob = min(0.85, prob)
        should = random.random() < prob
        if should:
            logger.info(f"[旁观者] 群{gid} 兴趣={self._evaluate_interest(text)} 忘我={eng} 概率={prob:.3f} → 插嘴")
        return should

    def _evaluate_interest(self, text):
        score = 0
        for kw in self._default_keywords():
            if kw in text:
                score += 2
        if any(q in text for q in ["？", "?", "怎么", "什么", "为什么", "是不是", "对不对"]):
            score += 1
        if "里克" in text or "塔洛斯" in text:
            score += 3
        if len(text) > 50:
            score += 1
        return min(score, 10)

    def on_replied(self, gid):
        gid = str(gid)
        data = self._ensure_group(gid)
        data["engagement_level"] = min(100, data["engagement_level"] + 30)
        data["total_interjections"] += 1
        if data["engagement_level"] >= 70:
            data["interject_cooldown"] = 2
        else:
            data["interject_cooldown"] = random.randint(4, 8)
        self._mark_dirty()
        logger.info(f"[旁观者] 群{gid} 回复后忘我度={data['engagement_level']} 冷却={data['interject_cooldown']}")

    def get_context(self, gid, limit=8):
        gid = str(gid)
        data = self._ensure_group(gid)
        return data["recent_messages"][-limit:]

    def get_engagement(self, gid):
        gid = str(gid)
        data = self._ensure_group(gid)
        return data["engagement_level"]

    def save(self):
        with self._lock:
            dm.save("bystander_data", self._data)
            self._dirty = False


class NewbieAdaptationModule:
    """新人适应期"""
    def __init__(self):
        self._data = dm.load("newbie_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_group(self, gid):
        gid = str(gid)
        with self._lock:
            if gid not in self._data:
                self._data[gid] = {"new_users": {}, "newbie_check": ""}
            return self._data[gid]

    def _mark_dirty(self):
        self._dirty = True

    def add_new_user(self, gid, uid):
        data = self._ensure_group(gid)
        if str(uid) not in data["new_users"]:
            data["new_users"][str(uid)] = {
                "join_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "is_newbie": True
            }
            self._mark_dirty()
            return True
        return False

    def is_newbie(self, gid, uid, days=None):
        if days is None:
            days = CONFIG["newbie"]["adaptation_days"]
        data = self._ensure_group(gid)
        uid = str(uid)
        if uid not in data["new_users"]:
            return False
        user_data = data["new_users"][uid]
        try:
            join_time = datetime.strptime(user_data["join_time"], "%Y-%m-%d %H:%M:%S")
            if (datetime.now() - join_time).days < days:
                return True
        except Exception:
            pass
        return False

    def get_adaptation_factor(self, gid, uid):
        if not self.is_newbie(gid, uid):
            return 1.0
        data = self._ensure_group(gid)
        uid = str(uid)
        try:
            join_time = datetime.strptime(data["new_users"][uid]["join_time"], "%Y-%m-%d %H:%M:%S")
            days = (datetime.now() - join_time).days
            return min(1.0, days / CONFIG["newbie"]["adaptation_days"])
        except Exception:
            return 0.5


class LateNightModule:
    """深夜自由时间"""
    def __init__(self):
        self._data = dm.load("late_night_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def is_late_night(self):
        hour = datetime.now().hour
        return hour in CONFIG["late_night"]["hours"]

    def is_early_morning(self):
        hour = datetime.now().hour
        return 5 <= hour < 8

    def get_mode(self):
        if self.is_late_night():
            return "late_night"
        elif self.is_early_morning():
            return "early_morning"
        else:
            return "normal"

    def get_reply_style(self):
        mode = self.get_mode()
        if mode == "late_night":
            return {"length": "long", "warmth": 0.3, "openness": 0.7}
        elif mode == "early_morning":
            return {"length": "short", "warmth": 0.2, "openness": 0.3}
        else:
            return {"length": "medium", "warmth": 0.5, "openness": 0.5}


class SecretCollectionModule:
    """囤东西不给看"""
    def __init__(self):
        self._data = dm.load("secret_collection_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_group(self, gid):
        gid = str(gid)
        with self._lock:
            if gid not in self._data:
                self._data[gid] = {"funny_phrases": [], "typos": [], "secrets": []}
            return self._data[gid]

    def _mark_dirty(self):
        self._dirty = True

    def add_funny_phrase(self, gid, phrase):
        data = self._ensure_group(gid)
        data["funny_phrases"].append({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "content": phrase
        })
        if len(data["funny_phrases"]) > 50:
            data["funny_phrases"] = data["funny_phrases"][-50:]
        self._mark_dirty()

    def add_typo(self, gid, typo, correct=None):
        data = self._ensure_group(gid)
        data["typos"].append({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "typo": typo,
            "correct": correct or "?"
        })
        if len(data["typos"]) > 50:
            data["typos"] = data["typos"][-50:]
        self._mark_dirty()

    def get_secrets(self, gid, type="all", limit=10):
        data = self._ensure_group(gid)
        if type == "funny":
            return data["funny_phrases"][-limit:]
        elif type == "typo":
            return data["typos"][-limit:]
        else:
            return {
                "funny": data["funny_phrases"][-limit:],
                "typos": data["typos"][-limit:],
            }


class NicknameSystem:
    """偷偷起外号"""
    def __init__(self):
        self._data = dm.load("nickname_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_group(self, gid):
        gid = str(gid)
        with self._lock:
            if gid not in self._data:
                self._data[gid] = {"nicknames": {}}
            return self._data[gid]

    def _mark_dirty(self):
        self._dirty = True

    def add_nickname(self, gid, uid, nickname):
        data = self._ensure_group(gid)
        data["nicknames"][str(uid)] = nickname
        self._mark_dirty()

    def get_nickname(self, gid, uid):
        data = self._ensure_group(gid)
        return data["nicknames"].get(str(uid), "")

    def get_all_nicknames(self, gid):
        data = self._ensure_group(gid)
        return data["nicknames"]


class OldAccountModule:
    """翻旧账"""
    def __init__(self):
        self._data = dm.load("old_account_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"past_contradictions": [], "last_old_account": ""}
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def record_contradiction(self, uid, old_msg, new_msg):
        data = self._ensure_user(uid)
        data["past_contradictions"].append({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "old": old_msg,
            "new": new_msg,
        })
        if len(data["past_contradictions"]) > 20:
            data["past_contradictions"] = data["past_contradictions"][-20:]
        self._mark_dirty()

    def should_old_account(self, uid, current_msg, threshold=70):
        trust_value = trust.get(uid)
        if trust_value < threshold:
            return False, None
        data = self._ensure_user(uid)
        if not data["past_contradictions"]:
            return False, None
        if random.random() < 0.15:
            return True, random.choice(data["past_contradictions"])
        return False, None


class TriggerRecallModule:
    """触发式回忆"""
    def __init__(self):
        self._data = dm.load("trigger_recall_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"recall_history": []}
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def try_recall(self, uid, message):
        memories = memory_weight.get_top_memories(uid, limit=20, min_weight=0.1)
        if not memories:
            return None
        words = set(re.findall(r'[\u4e00-\u9fa5]{2,4}', message))
        for mem in memories:
            content = mem.get("content", "")
            for word in words:
                if word in content and mem.get("weight", 0) < 0.3:
                    return f"（好像……有过这么回事？{content[:20]}……不太确定）"
        return None


class ConflictDetectionModule:
    """群友吵架时消失"""
    def __init__(self):
        self._data = dm.load("conflict_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_group(self, gid):
        gid = str(gid)
        with self._lock:
            if gid not in self._data:
                self._data[gid] = {"conflict_level": 0, "last_check": "", "silence_until": ""}
            return self._data[gid]

    def _mark_dirty(self):
        self._dirty = True

    def detect_conflict(self, gid, message):
        conflict_keywords = ["你", "我", "他说", "可是", "但", "明明", "明明就是", "你才", "才不是", "你凭什么"]
        intensity = sum(0.5 for kw in conflict_keywords if kw in message)
        intensity += message.count("！") * 0.3
        intensity += message.count("?") * 0.2

        data = self._ensure_group(gid)
        data["conflict_level"] = min(10, data["conflict_level"] + intensity)

        if data["conflict_level"] > CONFIG["conflict"]["threshold"]:
            data["silence_until"] = (datetime.now() + timedelta(minutes=CONFIG["conflict"]["silence_minutes"])).strftime("%Y-%m-%d %H:%M:%S")
            self._mark_dirty()
            return True

        data["conflict_level"] = max(0, data["conflict_level"] - 0.5)
        self._mark_dirty()
        return False

    def should_be_silent(self, gid):
        data = self._ensure_group(gid)
        if data.get("silence_until"):
            try:
                until = datetime.strptime(data["silence_until"], "%Y-%m-%d %H:%M:%S")
                if datetime.now() < until:
                    return True
            except Exception:
                pass
        return False


class JealousyModule:
    """吃醋机制"""
    def __init__(self):
        self._data = dm.load("jealousy_data", {})
        self._dirty = False
        self._lock = _FakeLock()
        self._owner_id = str(CONFIG["owner_qq"])

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"jealousy_level": 0, "target": "", "last_update": ""}
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def record_interaction(self, owner_id, other_id, gid):
        if str(owner_id) != self._owner_id:
            return
        other_str = str(other_id)
        data = self._ensure_user(other_str)
        data["jealousy_level"] = min(10, data["jealousy_level"] + 1)
        data["target"] = str(other_id)
        data["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._mark_dirty()

    def get_jealousy_reply(self, other_id):
        data = self._ensure_user(str(other_id))
        level = data["jealousy_level"]
        if level <= 0:
            return None

        # ---- 三模型协同：用 relationship 槽位生成自然的吃醋回应 ----
        intensity = "有点" if level <= 3 else "明显" if level <= 6 else "很"

        owner_prompt = SYSTEM_PROMPT + f"\n\n你正在吃醋，程度：{intensity}。" \
            "小塔（你重视的人）最近跟别人聊得很多，你有点不舒服。" \
            "请用你的口吻对他说一句话，表达你的醋意。简短自然，带动作描写。只说这一句。"
        owner_reply = _v6_call("relationship", owner_prompt, [])

        target_prompt = SYSTEM_PROMPT + f"\n\n你正在吃醋，程度：{intensity}。" \
            "有个不认识的人跟你搭话，你现在不想理对方。" \
            "请用你的口吻冷淡地回应一句。简短，带动作描写。只说这一句。"
        target_reply = _v6_call("relationship", target_prompt, [])

        # 回退到模板
        if not owner_reply or len(owner_reply) < 3:
            owner_reply = random.choice([
                "（小声）……小塔你最近跟他聊得好多。",
                "（尾巴轻轻摇了摇）……小塔，你都不找我了。",
                "（低头玩尾巴尖）……你今天跟他说话比跟我多。",
            ])
        if not target_reply or len(target_reply) < 3:
            target_reply = random.choice([
                "（看了你一眼，没说话）",
                "（别过脸去）……哦。",
                "（冷淡地）……嗯。",
            ])

        return {"to_owner": owner_reply.strip(), "to_target": target_reply.strip()}


class PraiseModule:
    """被夸更慌"""
    def __init__(self):
        self._data = dm.load("praise_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"praise_count": 0, "last_praise": ""}
                self._mark_dirty()
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def on_praise(self, uid, is_owner=False):
        data = self._ensure_user(uid)
        data["praise_count"] += 1
        self._mark_dirty()

        # ---- 三模型协同：用 talk 槽位生成自然的被夸反应 ----
        if is_owner:
            trust.update(uid, CONFIG["praise"]["owner_bonus"])
            relation = "你很信任的人"
        else:
            trust.penalize(uid, "被群友夸不知所措")
            relation = "不太熟的人"

        prompt = SYSTEM_PROMPT + f"\n\n{relation}夸了你。" \
            "你不太习惯被夸，会害羞、不知所措。" \
            "请用你的口吻回应一句，带动作描写。简短自然。只说这一句。"

        reply = _v6_call("talk", prompt, [], temperature_override=_v6_temp("talk"))
        if reply and len(reply) > 3:
            return reply.strip()

        # 回退到模板
        if is_owner:
            return random.choice([
                "（尾巴愉快地晃了晃，耳朵微微竖起）……你、你这么说我会不好意思的……",
                "（低头，声音变小）……小塔你这样夸我……我会当真的。",
                "（嘴角微微扬起）……真的吗？",
            ])
        else:
            return random.choice([
                "（耳朵往后压，往后退了半步）……别、别这么说……",
                "（声音变小，有点慌乱）……你、你这样我反而不知道该怎么回……",
                "（低头玩手指）……我、我没那么好……",
            ])


class DraftModule:
    """打了又删"""
    def __init__(self):
        self._data = dm.load("draft_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"drafts": []}
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def add_draft(self, uid, draft):
        data = self._ensure_user(uid)
        data["drafts"].append({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "content": draft
        })
        if len(data["drafts"]) > 20:
            data["drafts"] = data["drafts"][-20:]
        self._mark_dirty()

    def get_drafts(self, uid, limit=5):
        data = self._ensure_user(uid)
        return data["drafts"][-limit:]


class SelfContradictionModule:
    """被自己绊倒"""
    def __init__(self):
        self._data = dm.load("self_contradiction_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {"recent_messages": [], "contradictions": 0}
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def add_message(self, uid, message):
        data = self._ensure_user(uid)
        data["recent_messages"].append({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "content": message
        })
        if len(data["recent_messages"]) > 10:
            data["recent_messages"] = data["recent_messages"][-10:]
        if len(data["recent_messages"]) >= 2 and random.random() < 0.1:
            data["contradictions"] += 1
            self._mark_dirty()
            return True
        self._mark_dirty()
        return False

    def get_contradiction_reply(self):
        return context.get("ReplyBank").get("contradiction_reply")


class HumanLikeStateModule:
    """真人感核心状态（细化版）

    改进点：
    - 区分 directly addressed（私聊/群聊艾特）和 observed（群里只是被看见）
    - 使用 time.time() 做衰减，避免高频 datetime 解析
    - 情绪惯性增加冷战 -> 试探 -> 缓和 -> 柔软的过渡
    - 主动欲望加入场合与时段判断，深夜更容易主动，白天群聊更克制
    """
    POSITIVE_KW = ["喜欢", "谢谢", "辛苦", "抱抱", "陪我", "真好", "可爱", "温柔", "想你", "晚安", "早安"]
    NEGATIVE_KW = ["烦", "讨厌", "闭嘴", "滚", "没用", "傻", "笨", "恶心", "吵死", "别说话"]
    OWNER_SOFT_NEGATIVE_KW = ["生气", "气死", "烦死", "不理你", "讨厌你", "你坏", "哼"]
    CARE_KW = ["累", "困", "难受", "头疼", "崩了", "不开心", "害怕", "焦虑", "睡不着", "生病"]
    PROMISE_KW = ["记住", "约定", "答应", "以后", "不要忘", "一定", "永远"]
    TOPIC_STOP = ["这个", "那个", "一下", "怎么", "什么", "就是", "还是", "没有", "不是", "可以"]

    INERTIA_LABELS = {
        "normal": "平常",
        "soft": "有点柔软",
        "hurt": "被刺到",
        "cold": "冷淡",
        "cold_war": "冷战",
        "testing": "试探",
        "softening": "缓和中",
        "reserved": "保留一点距离",
    }

    def __init__(self):
        self._data = dm.load("human_like_state_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _default_user(self):
        now_ts = time.time()
        return {
            "safety": 45,
            "fatigue": 25,
            "vigilance": 35,
            "grievance": 0,
            "attachment": 15,
            "expression": 35,
            "care": 25,
            "last_interaction": "",
            "last_interaction_ts": now_ts,
            "last_topic": "",
            "last_interaction_mode": "none",
            "thread": [],
            "active_desire": 0,
            "mood_inertia": "normal",
            "surface_style": "guarded",
            "inner_note": "",
        }

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = self._default_user()
                self._mark_dirty()
            else:
                data = self._data[uid]
                # 兼容旧数据：旧版只保存 last_interaction 字符串，新版优先使用时间戳。
                if "last_interaction_ts" not in data:
                    data["last_interaction_ts"] = time.time()
                if "last_interaction_mode" not in data:
                    data["last_interaction_mode"] = "none"
            return self._data[uid]

    def _mark_dirty(self):
        self._dirty = True

    def _clamp(self, val, low=0, high=100):
        return max(low, min(high, int(val)))

    def _change(self, data, key, delta):
        data[key] = self._clamp(data.get(key, 0) + delta)

    def _now_text(self, ts=None):
        return datetime.fromtimestamp(ts or time.time()).strftime("%Y-%m-%d %H:%M:%S")

    def _advance_inertia_by_state(self, data, hours):
        inertia = data.get("mood_inertia", "normal")
        grievance = data.get("grievance", 0)
        safety = data.get("safety", 45)
        attachment = data.get("attachment", 15)

        # 冷战不会瞬间软化：冷战 -> 试探 -> 缓和中 -> 保留距离/正常
        if inertia == "cold_war":
            if grievance < 45 or hours >= 2:
                data["mood_inertia"] = "testing"
        elif inertia == "cold":
            if grievance < 50 or hours >= 1:
                data["mood_inertia"] = "testing"
        elif inertia == "hurt":
            if grievance < 35 or hours >= 0.5:
                data["mood_inertia"] = "testing"
        elif inertia == "testing":
            if grievance < 22 and safety >= 45:
                data["mood_inertia"] = "softening"
        elif inertia == "softening":
            if grievance < 10 and safety >= 50:
                data["mood_inertia"] = "reserved" if attachment < 45 else "soft"
        elif inertia in ["reserved", "soft"]:
            if data["fatigue"] < 45 and data["vigilance"] < 45 and grievance < 8:
                data["mood_inertia"] = "normal"

    def _decay_state(self, data):
        now_ts = time.time()
        last_ts = data.get("last_interaction_ts")
        if not isinstance(last_ts, (int, float)):
            last_ts = now_ts
        hours = max(0, (now_ts - last_ts) / 3600)
        if hours <= 0:
            return

        decay = CONFIG["human_like"]["state_decay_per_hour"]
        data["fatigue"] = self._clamp(data["fatigue"] - hours * 3 * decay)
        data["vigilance"] = self._clamp(data["vigilance"] - hours * 2 * decay)
        data["expression"] = self._clamp(data["expression"] + hours * 2 * decay)
        data["active_desire"] = self._clamp(data["active_desire"] + min(10, hours * 0.8))
        data["grievance"] = self._clamp(data["grievance"] - hours * CONFIG["human_like"]["grievance_recover_per_hour"])
        self._advance_inertia_by_state(data, hours)
        data["last_interaction_ts"] = now_ts
        data["last_interaction"] = self._now_text(now_ts)

    def _extract_topic(self, message):
        words = re.findall(r'[\u4e00-\u9fa5]{2,6}', str(message))
        words = [w for w in words if w not in self.TOPIC_STOP and len(w) >= 2]
        if not words:
            return ""
        return words[0][:12]

    def _remember_thread(self, data, message, source, interaction_mode):
        topic = self._extract_topic(message)
        if topic and interaction_mode != "observed":
            data["last_topic"] = topic
        data["thread"].append({
            "time": self._now_text(),
            "source": source,
            "interaction_mode": interaction_mode,
            "topic": topic,
            "content": str(message)[:80],
        })
        max_len = CONFIG["human_like"]["max_thread_messages"]
        if len(data["thread"]) > max_len:
            data["thread"] = data["thread"][-max_len:]

    def _classify_message(self, message, is_owner=False):
        msg = str(message)
        if is_owner and any(k in msg for k in self.OWNER_SOFT_NEGATIVE_KW):
            return "owner_pout"
        if any(k in msg for k in self.NEGATIVE_KW):
            return "negative"
        if any(k in msg for k in self.CARE_KW):
            return "care"
        if any(k in msg for k in self.PROMISE_KW):
            return "promise"
        if any(k in msg for k in self.POSITIVE_KW):
            return "positive"
        if len(msg) > 80:
            return "long"
        return "neutral"

    def _interaction_weight(self, source, interaction_mode):
        if source == "private":
            return 1.0
        if interaction_mode == "direct":
            return 0.8
        if interaction_mode == "observed":
            return 0.2
        return 0.5

    def _set_surface_style(self, data):
        if data["grievance"] >= 75:
            data["surface_style"] = "cold"
            data["mood_inertia"] = "cold_war"
        elif data["grievance"] >= 55:
            data["surface_style"] = "cold"
            if data.get("mood_inertia") not in ["cold_war", "testing"]:
                data["mood_inertia"] = "cold"
        elif data.get("mood_inertia") == "testing":
            data["surface_style"] = "testing"
        elif data["fatigue"] >= 75:
            data["surface_style"] = "tired"
        elif data["vigilance"] >= 65:
            data["surface_style"] = "guarded"
        elif data["safety"] >= 70 and data["attachment"] >= 45 and data["grievance"] < 20:
            data["surface_style"] = "soft"
        else:
            data["surface_style"] = "reserved"

    def on_user_message(self, uid, message, source="private", gid=None, is_owner=False, interaction_mode="direct"):
        """用户直接和里克说话。

        interaction_mode:
        - direct: 私聊或群聊 @ 里克，状态权重较高
        - observed: 群聊里不是对里克说，只是被看见，应该调用 on_observe_message
        """
        data = self._ensure_user(uid)
        with self._lock:
            self._decay_state(data)
            kind = self._classify_message(message, is_owner=is_owner)
            weight = self._interaction_weight(source, interaction_mode)
            self._remember_thread(data, message, source, interaction_mode)
            data["last_interaction_mode"] = interaction_mode

            if source == "group":
                self._change(data, "fatigue", 3 * weight)
                self._change(data, "vigilance", 2 * weight)
                self._change(data, "expression", -2 * weight)
            else:
                self._change(data, "expression", 2 * weight)
                self._change(data, "active_desire", -2)

            if is_owner:
                self._change(data, "attachment", 2 * weight)
                self._change(data, "safety", 2 * weight)
                self._change(data, "vigilance", -2 * weight)

            if kind == "positive":
                self._change(data, "safety", (6 + random.randint(0, 3)) * weight)
                self._change(data, "attachment", (4 + random.randint(0, 2)) * weight)
                self._change(data, "vigilance", -(4 + random.randint(0, 2)) * weight)
                self._change(data, "grievance", -(5 + random.randint(0, 3)) * weight)
                self._change(data, "expression", random.randint(2, 5))
                if data.get("mood_inertia") in ["cold_war", "cold", "hurt"]:
                    data["mood_inertia"] = "testing"
                    data["inner_note"] = random.choice([
                        "心里有点松动，但还想试探一下对方是不是真的在意。",
                        "……其实有点开心，但不能马上表现出来。",
                        "对方的善意收到了，嘴上先别太软。",
                    ])
                else:
                    data["mood_inertia"] = "soft"
                    data["inner_note"] = random.choice([
                        "其实有点高兴，但不太想表现得太明显。",
                        "嘴角想翘但忍住了，语气可以稍微松一点。",
                        "被夸了有点不好意思，别让对方看到心里在笑。",
                    ])
            elif kind == "care":
                self._change(data, "care", (8 + random.randint(0, 3)) * weight)
                self._change(data, "attachment", (3 + random.randint(0, 2)) * weight)
                self._change(data, "expression", (3 + random.randint(0, 2)) * weight)
                data["inner_note"] = random.choice([
                    "有点担心对方，嘴上可以别太直白。",
                    "对方状态不太好，想关心但别太刻意。",
                    "……有点心疼，但说出来会很奇怪吧。",
                    "在意对方的状况，可以稍微多问一句。",
                ])
            elif kind == "promise":
                self._change(data, "safety", (4 + random.randint(0, 2)) * weight)
                self._change(data, "attachment", (3 + random.randint(0, 2)) * weight)
                data["inner_note"] = random.choice([
                    "这句话可能值得记住，不要随便敷衍。",
                    "对方在认真说一件事，认真听。",
                    "……记在心里了，但别表现得太郑重。",
                ])
            elif kind == "owner_pout":
                # 小塔是最信任的人：她不会真的很介意，但会有一点小脾气。
                self._change(data, "grievance", 3)
                self._change(data, "vigilance", 1)
                self._change(data, "attachment", 1)
                data["mood_inertia"] = "testing"
                data["inner_note"] = "对方是小塔，就算她有点生气，也不是要远离；可以有小脾气，但别太冷。"
            elif kind == "negative":
                if is_owner:
                    self._change(data, "vigilance", 4 * weight)
                    self._change(data, "grievance", 4 * weight)
                    self._change(data, "expression", -4 * weight)
                    self._change(data, "safety", -3 * weight)
                    data["mood_inertia"] = "testing"
                    data["inner_note"] = "被小塔的话刺到了一点，但还是信任她；嘴硬一下就好，不要真正疏远。"
                else:
                    self._change(data, "vigilance", 10 * weight)
                    self._change(data, "grievance", 8 * weight)
                    self._change(data, "expression", -8 * weight)
                    self._change(data, "safety", -8 * weight)
                    data["mood_inertia"] = "hurt"
                    data["inner_note"] = "被刺到了一下，先保持距离。"
            elif kind == "long":
                self._change(data, "fatigue", 2 * weight)
                self._change(data, "expression", 1 * weight)
            elif kind == "neutral":
                self._change(data, "fatigue", 1 * weight)
                self._change(data, "expression", random.randint(-1, 2))
                self._change(data, "active_desire", random.randint(0, 2))
                if is_owner:
                    self._change(data, "safety", random.randint(0, 1))
                    self._change(data, "attachment", random.randint(0, 1))
                    roll = random.random()
                    if roll < 0.15:
                        data["inner_note"] = random.choice([
                            "其实没那么想说话，但对方在，就应一声吧。",
                            "有点走神，但还是听着。",
                            "不讨厌这个话题，但也不知道接什么。",
                            "嗯……在想怎么回比较自然。",
                            "对方好像只是随便聊聊，不用太紧张。",
                        ])
                else:
                    roll = random.random()
                    if roll < 0.08:
                        data["inner_note"] = random.choice([
                            "不太熟，先看看再说。",
                            "不知道该接什么，含糊一下。",
                            "对方好像没有特别的意思，正常回就好。",
                        ])

            self._set_surface_style(data)
            self._mark_dirty()
            return kind

    def on_observe_message(self, gid, uid, message):
        """群聊里没 @ 里克：只是旁观/被看见，权重很低，不应等同直接对话。"""
        data = self._ensure_user(uid)
        with self._lock:
            self._decay_state(data)
            self._remember_thread(data, message, "observe", "observed")
            data["last_interaction_mode"] = "observed"
            if len(str(message)) > 30:
                self._change(data, "fatigue", 0.5)
            # 观察到群聊内容，只影响群环境压力，不主动贴近用户。
            self._change(data, "active_desire", 0.2)
            self._mark_dirty()

    def on_bot_reply(self, uid, reply, source="private", gid=None):
        data = self._ensure_user(uid)
        with self._lock:
            if len(str(reply)) > 120:
                self._change(data, "fatigue", 2)
            else:
                self._change(data, "fatigue", 1)
            self._change(data, "active_desire", -8 if source == "private" else -5)
            self._change(data, "expression", -1)
            self._mark_dirty()

    def _active_threshold(self, source, gid=None):
        threshold = CONFIG["human_like"]["active_desire_threshold"]
        hour = datetime.now().hour
        if hour in CONFIG["late_night"]["hours"]:
            threshold -= CONFIG["human_like"].get("active_desire_late_night_bonus", 18)
        if source == "private":
            threshold -= CONFIG["human_like"].get("active_desire_private_bonus", 8)
        elif source == "group":
            threshold += CONFIG["human_like"].get("active_desire_group_penalty", 18)
        return max(35, min(90, threshold))

    def _active_context_note(self, active, source, threshold):
        hour = datetime.now().hour
        if active < threshold:
            return ""
        if source == "group":
            return "- 主动欲望：现在虽然有一点想说话，但群聊场合要克制；除非被明确递话或气氛很安静，否则不要主动抢话。"
        if hour in CONFIG["late_night"]["hours"]:
            return "- 主动欲望：现在是深夜，阈值降低；可以更诚实一点，偶尔主动接话，但仍然保持短句。"
        return "- 主动欲望：你有一点想主动说些什么，但要克制，除非对方给了话口。"

    def get_prompt(self, uid, gid=None):
        data = self._ensure_user(uid)
        with self._lock:
            self._decay_state(data)
            style = data.get("surface_style", "reserved")
            inertia = data.get("mood_inertia", "normal")
            topic = data.get("last_topic", "")
            active = data.get("active_desire", 0)
            source = "group" if gid else "private"
            threshold = self._active_threshold(source, gid)
            lines = [
                "\n【真人感状态】",
                f"- 安全感:{data['safety']}/100，疲惫:{data['fatigue']}/100，警惕:{data['vigilance']}/100，委屈:{data['grievance']}/100，依赖:{data['attachment']}/100，表达欲:{data['expression']}/100。",
            ]
            if style == "cold":
                lines.append("- 表层状态：有点冷淡，不想立刻软下来；可以短句、停顿、回避热情。")
            elif style == "testing":
                lines.append("- 表层状态：处在试探期，想确认对方是不是认真；可以嘴硬、轻轻刺一下，但不要彻底拒绝。")
            elif style == "soft":
                lines.append("- 表层状态：比平时柔软一点，但仍然别太外放。")
            elif style == "tired":
                lines.append("- 表层状态：累，回复可以更短，更慢，像没什么力气。")
            elif style == "guarded":
                lines.append("- 表层状态：保持警惕，有边界，不要过度亲近。")
            else:
                lines.append("- 表层状态：克制、平淡、略别扭。")

            if inertia == "cold_war":
                lines.append("- 情绪惯性：还在冷战，不会立刻恢复；但如果对方是小塔，底层仍然信任她。")
            elif inertia == "cold":
                lines.append("- 情绪惯性：刚才的不舒服还在，语气可以偏冷。")
            elif inertia == "hurt":
                lines.append("- 情绪惯性：被刺到，还没完全消化。")
            elif inertia == "testing":
                lines.append("- 情绪惯性：从冷淡进入试探期，想靠近但还要确认安全。")
            elif inertia == "softening":
                lines.append("- 情绪惯性：正在缓和，但还会有一点嘴硬。")
            elif inertia == "reserved":
                lines.append("- 情绪惯性：已经不生气了，但还保留一点距离。")
            elif inertia == "soft":
                lines.append("- 情绪惯性：心里有点软，但不要说得太甜。")

            if topic:
                lines.append(f"- 话题延续：你隐约记得刚才在聊「{topic}」，可以自然接上，不要像第一次听见。")

            if data.get("inner_note"):
                lines.append(f"- 内心倾向：{data['inner_note']}")

            active_note = self._active_context_note(active, source, threshold)
            if active_note:
                lines.append(active_note)

            lines.append("- 口是心非：可以嘴上冷淡一点，但行为上露出关心；不要客服腔，不要过度解释。")
            self._mark_dirty()
            return "\n".join(lines) + "\n"

    def get_status_text(self, uid):
        data = self._ensure_user(uid)
        with self._lock:
            topic = data.get("last_topic") or "无"
            inertia = data.get("mood_inertia", "normal")
            inertia_name = self.INERTIA_LABELS.get(inertia, inertia)
            return (
                "【真人感状态】\n"
                f"安全感：{data['safety']}/100\n"
                f"疲惫：{data['fatigue']}/100\n"
                f"警惕：{data['vigilance']}/100\n"
                f"委屈：{data['grievance']}/100\n"
                f"依赖：{data['attachment']}/100\n"
                f"表达欲：{data['expression']}/100\n"
                f"表层状态：{data.get('surface_style', 'reserved')}\n"
                f"情绪惯性：{inertia_name}（{inertia}）\n"
                f"最近话题：{topic}\n"
                f"最近互动：{data.get('last_interaction_mode', 'none')}\n"
                f"主动欲望：{data.get('active_desire', 0)}/100\n"
                f"私聊主动阈值：{self._active_threshold('private')}\n"
                f"群聊主动阈值：{self._active_threshold('group')}"
            )

    def reset(self, uid):
        uid = str(uid)
        with self._lock:
            self._data[uid] = self._default_user()
            self._mark_dirty()

    def fluctuate_all(self):
        import random as _r
        with self._lock:
            count = 0
            for uid, data in self._data.items():
                changed = False
                for key in ("safety", "fatigue", "vigilance", "grievance", "attachment", "expression"):
                    if key not in data:
                        continue
                    delta = _r.randint(-5, 8)
                    if key == "fatigue":
                        delta = _r.randint(-5, 8)
                    elif key == "safety":
                        delta = _r.randint(-4, 3)
                    elif key == "attachment":
                        delta = _r.randint(-3, 4)
                    elif key == "vigilance":
                        delta = _r.randint(-3, 6)
                    elif key == "grievance":
                        delta = _r.randint(-4, 5)
                    elif key == "expression":
                        delta = _r.randint(-4, 6)
                    data[key] = max(0, min(100, data[key] + delta))
                    changed = True
                if "active_desire" in data:
                    data["active_desire"] = max(0, min(100, data["active_desire"] + _r.randint(3, 15)))
                    changed = True
                if _r.random() < 0.6:
                    notes = [
                        "（发呆中）", "（好像听到了什么）", "（尾巴晃了晃）",
                        "（肚子有点饿）", "（想出去走走）", "（困了）",
                        "（心情一般般）", "（在胡思乱想）"
                    ]
                    data["inner_note"] = _r.choice(notes)
                    changed = True
                if changed:
                    self._set_surface_style(data)
                    count += 1
                    logger.info(f"[真人状态] 波动 uid={uid} safety={data['safety']} fatigue={data['fatigue']} vigilance={data['vigilance']} grievance={data['grievance']} attachment={data['attachment']} expression={data['expression']}")
            if self._data:
                self._mark_dirty()
                dm.save("human_like_state_data", self._data)
                dm.flush_all()
                logger.info(f"[真人状态] 随机波动完成并已保存，覆盖 {count}/{len(self._data)} 个用户")


