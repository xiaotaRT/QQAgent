#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""审计日志（从 src_a_utils.py 迁移）：内存 deque 缓冲 + 批量 flush。"""
import os
import json
from collections import deque
from qqagent.core.config import CONFIG
from qqagent.core.logging import logger

# ---- 审计日志：内存 deque 缓存 + 批量 flush，避免每次全量读写文件 ----
_AUDIT_LOG_BUFFER = deque(maxlen=1000)
_AUDIT_LOG_DIRTY = False
_AUDIT_LOG_FLUSH_COUNTER = 0

def _append_audit_log(entry):
    global _AUDIT_LOG_DIRTY, _AUDIT_LOG_FLUSH_COUNTER
    _AUDIT_LOG_BUFFER.append(entry)
    _AUDIT_LOG_DIRTY = True
    _AUDIT_LOG_FLUSH_COUNTER += 1
    # 攒够 20 条批量写一次
    if _AUDIT_LOG_FLUSH_COUNTER >= 20:
        _flush_audit_log()

def _flush_audit_log():
    global _AUDIT_LOG_DIRTY, _AUDIT_LOG_FLUSH_COUNTER
    if not _AUDIT_LOG_DIRTY:
        return
    path = os.path.join(CONFIG["data_dir"], "audit_log.json")
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # 用内存缓冲区的完整快照写入，不需要先读文件
        data = list(_AUDIT_LOG_BUFFER)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        _AUDIT_LOG_DIRTY = False
        _AUDIT_LOG_FLUSH_COUNTER = 0
    except Exception as e:
        logger.error(f"[审计日志] 写入失败: {e}")

