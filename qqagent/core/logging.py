#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""统一日志（从 src_a_utils.py 迁移）。"""
import os
import time
import logging
from collections import deque
from logging.handlers import RotatingFileHandler
from qqagent.core.config import CONFIG

os.makedirs(CONFIG["log_dir"], exist_ok=True)
logger = logging.getLogger("rikka_v80")
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%m-%d %H:%M:%S")

for name, level, fn in [
    ("error", logging.ERROR, "error.log"),
    ("info", logging.INFO, "info.log"),
    ("chat", logging.INFO, "chat.log"),
]:
    h = RotatingFileHandler(
        os.path.join(CONFIG["log_dir"], fn),
        maxBytes=CONFIG["log_max_size"],
        backupCount=CONFIG["log_backup_count"],
        encoding="utf-8"
    )
    h.setLevel(level)
    h.setFormatter(fmt)
    logger.addHandler(h)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(fmt)
logger.addHandler(console)

_BOT_STARTED_AT = time.time()
_RECENT_ERRORS = deque(maxlen=50)


class RecentErrorHandler(logging.Handler):
    def emit(self, record):
        if record.levelno >= logging.ERROR:
            try:
                _RECENT_ERRORS.append(self.format(record))
            except Exception:
                pass


recent_error_handler = RecentErrorHandler()
recent_error_handler.setLevel(logging.ERROR)
recent_error_handler.setFormatter(fmt)
logger.addHandler(recent_error_handler)

