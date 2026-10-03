#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""WebSocket 管理器（从 src_m_reality.py 迁移）。"""
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

from qqagent.core import CONFIG, logger
from qqagent.core import context
from qqagent.behavior.behavior_learn import send_private, send_group

class WSManager:
    def __init__(self):
        self._ws = None
        self._retry = 0
        self._running = False
        self._last_hb = 0
        self._last_msg = time.time()
        self._bg_started = False

    def _start_hb(self, ws):
        def w():
            while self._running:
                time.sleep(CONFIG["heartbeat_interval"])
                try:
                    if ws.sock and ws.sock.connected:
                        ws.send(json.dumps({"action": "get_login_info", "params": {}}))
                        self._last_hb = time.time()
                        silent = time.time() - self._last_msg
                        if silent > CONFIG["heartbeat_interval"] * 4:
                            logger.warning(f"[心跳] 已 {silent:.0f}秒 未收到任何响应，主动断开重连")
                            try:
                                ws.close()
                            except Exception:
                                pass
                    else:
                        logger.warning("[心跳] WebSocket未连接，等待重连")
                except Exception as e:
                    logger.warning(f"[心跳] 失败: {e}")
        threading.Thread(target=w, daemon=True).start()

    def _on_message(self, ws, message):
        self._last_msg = time.time()
        try:
            data = json.loads(message)
            echo = data.get("echo")
            if echo and "status" in data:
                _pending_api_responses[echo] = data
                return
            pt = data.get("post_type")
            mt = data.get("message_type")
            if pt == "message":
                raw_preview = str(data.get('raw_message', ''))[:80]
                logger.info(f"[WS DEBUG] 收到消息 post_type={pt} message_type={mt} raw={raw_preview}")
            if pt == "message" and mt == "private":
                handle_private(ws, data["user_id"], data["raw_message"])
            elif pt == "message" and mt == "group":
                text = ""
                nickname = data.get("sender", {}).get("nickname", "")
                if isinstance(data.get("message"), list):
                    for seg in data["message"]:
                        if seg.get("type") == "text":
                            text += seg.get("data", {}).get("text", "")
                else:
                    text = data["raw_message"]
                msg_id = data.get("message_id")
                if msg_id:
                    _recent_group_msg_ids[(int(data["group_id"]), int(data["user_id"]))] = {"mid": msg_id, "ts": time.time(), "content": text[:50]}
                    logger.info(f"[消息追踪] 群{data['group_id']} 用户{data['user_id']} mid={msg_id} 内容={text[:20]}")
                    if len(_recent_group_msg_ids) > 200:
                        cutoff = time.time() - 300
                        stale = [k for k, v in _recent_group_msg_ids.items() if v["ts"] < cutoff]
                        for k in stale:
                            del _recent_group_msg_ids[k]
                handle_group(ws, data["group_id"], data["user_id"], text, data["raw_message"], nickname, msg_id)
            elif pt == "notice" and data.get("notice_type") == "group_increase":
                handle_group_increase(ws, data.get("group_id"), data.get("user_id"))
            elif pt == "notice" and data.get("notice_type") == "group_decrease":
                if str(data.get("user_id")) == CONFIG["bot_qq"]:
                    notify = "......小塔，我被踢出群了。" if data.get("operator_id") and str(data["operator_id"]) != CONFIG["bot_qq"] else "......我退群了。"
                    send_private(ws, CONFIG["owner_qq"], notify)
            elif pt == "notice" and data.get("notice_type") == "group_admin":
                handle_group_admin(ws, data.get("group_id"), data.get("user_id"), data.get("sub_type"))
            elif pt == "request" and data.get("request_type") == "friend":
                u = str(data.get("user_id"))
                pending_friends[u] = data.get("flag")
                notify = f"......有人加我。（尾巴有点紧张地绷着）\nQQ号：{u}\n验证信息：{data.get('comment', '')}"
                send_private(ws, CONFIG["owner_qq"], notify)
        except Exception as e:
            logger.exception(f"消息处理异常: {e}")

    def _on_error(self, ws, error):
        logger.error(f"WebSocket错误: {error}")

    def _on_close(self, ws, code, msg):
        logger.warning(f"连接断开: code={code}, msg={msg}")
        if not self._running:
            return
        self._retry += 1
        if self._retry <= CONFIG["retry_max"]:
            delay = min(CONFIG["retry_base_delay"] * (2 ** (self._retry - 1)), CONFIG["retry_max_delay"])
            logger.info(f"第 {self._retry} 次重连，{delay} 秒后...")
            time.sleep(delay)
            self._connect()
        else:
            logger.warning(f"重连 {self._retry} 次失败，休息 {CONFIG['retry_max_delay']} 秒后继续")
            self._retry = 0
            time.sleep(CONFIG["retry_max_delay"])
            self._connect()

    def _on_open(self, ws):
        self._retry = 0
        self._last_hb = time.time()
        self._last_msg = time.time()
        msg_queue.set_ws(ws)
        msg_queue.clear()

        # V6.0: 设置全局引用并启动后台调度器（重连时不重复启动）
        context.wsm = self
        if not self._bg_started:
            context.bg_scheduler.start()
            self._bg_started = True

        logger.info("\n" + "=" * 60)
        logger.info("✅ 塔洛斯·里克 V10.0 - Life Engine + 人格核心 + 心理学核心引擎")
        logger.info(f"🤖 机器人: {CONFIG['bot_qq']}")
        logger.info(f"👑 主人: {CONFIG['owner_qq']}")
        logger.info("🧠 架构: Router + Brain + Talk + Agent")
        logger.info(f"⚙️ 模式: {CONFIG.get('v6', {}).get('default_mode', 'auto')}")
        logger.info("=" * 60)
        self._start_hb(ws)

        if CONFIG["send_greeting_on_start"]:
            try:
                today = datetime.now().strftime("%Y-%m-%d")
                gf = os.path.join(CONFIG["data_dir"], CONFIG["files"]["greeting"])
                os.makedirs(os.path.dirname(gf), exist_ok=True)
                need = True
                if os.path.exists(gf):
                    with open(gf, "r", encoding="utf-8") as f:
                        if f.read().strip() == today:
                            need = False
                if need and CONFIG["modules"]["personality"]:
                    if CONFIG["modules"].get("time_perception", True) and 'time_perception' in globals() and time_perception is not None:
                        time_msg = time_perception.get_time_awareness(CONFIG["owner_qq"])
                    else:
                        time_msg = personality.get_time_awareness()
                    send_private(ws, CONFIG["owner_qq"], f"......小塔，{time_msg}")
                    with open(gf, "w", encoding="utf-8") as f:
                        f.write(today)
            except Exception as e:
                logger.error(f"问候发送失败: {e}")

        if CONFIG["modules"]["birthday"]:
            try:
                for b in birthday.today():
                    if b["name"] == "里克的生日":
                        msg = "......今天好像是我的生日。（尾巴有点不安地晃了晃）我、我自己都快忘了。"
                    else:
                        msg = f"......小塔，今天是 {b['name']} 哦。（尾巴轻轻点了点）"
                    send_private(ws, CONFIG["owner_qq"], msg)
            except Exception as e:
                logger.error(f"生日提醒失败: {e}")

    def _connect(self):
        ws_url = CONFIG["napcat_ws_url"]
        access_token = CONFIG.get("napcat_access_token", "")
        if access_token and "access_token=" not in ws_url:
            separator = "&" if "?" in ws_url else "?"
            ws_url = f"{ws_url}{separator}access_token={quote(access_token, safe='')}"
        logger.info(
            "正在连接 %s...",
            ws_url.split("access_token=", 1)[0] + (
                "access_token=<已配置>" if "access_token=" in ws_url else ""
            ),
        )
        self._ws = websocket.WebSocketApp(
            ws_url,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open
        )
        msg_queue.set_ws(self._ws)
        self._ws.run_forever(ping_interval=30, ping_timeout=15)

    def start(self):
        self._running = True
        self._connect()

    def stop(self):
        self._running = False
        if self._ws:
            self._ws.close()

