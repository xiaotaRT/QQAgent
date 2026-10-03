#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""通用工具（从 src_a_utils.py 迁移）：_FakeLock、is_owner、文本处理、正则。"""
import re
import time
from datetime import datetime
from qqagent.core.config import CONFIG

class _FakeLock:
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def acquire(self, *a, **kw): return True
    def release(self): pass


_PSYCH_CORE_AVAILABLE = True


def is_owner(uid):
    if str(uid) != str(CONFIG["owner_qq"]):
        return False
    try:
        if emotion_isolated and emotion_isolated.get_test_mode():
            return False
    except Exception:
        pass
    return True


# ---- 高频正则预编译（每条消息都会调用）----
_CQ_CODE_RE = re.compile(r'\[CQ:[^\]]+\]')
_WHITESPACE_RE = re.compile(r'[\s\u200b-\u200f\ufeff]')
_SHORT_SYMBOL_RE = re.compile(r'^[\W_]+$')
_NORMALIZE_RE = re.compile(r'[\s_\u200b-\u200f\ufeff\-\.]')


def is_empty_content(text):
    cleaned = _CQ_CODE_RE.sub('', str(text))
    cleaned = _WHITESPACE_RE.sub('', cleaned)
    if len(cleaned) <= 2 and _SHORT_SYMBOL_RE.match(cleaned):
        return True
    return len(cleaned) == 0


def normalize_text(text):
    text = _NORMALIZE_RE.sub('', str(text))
    return text.lower()


def _time_str():
    now = datetime.now()
    wd = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
    return f"{now.year}年{now.month}月{now.day}日 {now.hour}:{now.minute:02d} {wd[now.weekday()]}"


def _extract_qq(text):
    return re.findall(r'[1-9]\d{4,10}', str(text))


def _now_str():
    """当前时间字符串（YYYY-MM-DD HH:MM:SS）。"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

