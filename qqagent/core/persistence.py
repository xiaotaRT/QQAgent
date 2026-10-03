#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据持久化（从 src_b_data_defense.py 迁移）：DataManager。"""
import os
import json
import time
import threading
import shutil
from qqagent.core.config import CONFIG
from qqagent.core.utils import _FakeLock

class DataManager:
    def __init__(self):
        self._cache = {}
        self._dirty_keys = set()
        self._version_cache = {}
        self._lock = _FakeLock()
        self._start_flush_worker()

    def _path(self, key):
        return os.path.join(CONFIG["data_dir"], CONFIG["files"][key])

    def _backup_path(self, key):
        return self._path(key) + ".bak"

    def load(self, key, default=None):
        if default is None:
            default = {}
        with self._lock:
            if key in self._cache:
                return self._cache[key]
        path = self._path(key)
        data = default
        loaded = False
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                data = raw["data"] if isinstance(raw, dict) and "data" in raw else raw
                loaded = True
            except Exception as e:
                logger.error(f"[Data] 主文件损坏 {key}: {e}")
        if not loaded and CONFIG["enable_double_backup"]:
            bak = self._backup_path(key)
            if os.path.exists(bak):
                try:
                    with open(bak, "r", encoding="utf-8") as f:
                        raw = json.load(f)
                    data = raw["data"] if isinstance(raw, dict) and "data" in raw else raw
                    logger.warning(f"[Data] 从备份恢复 {key}")
                    loaded = True
                except Exception as e:
                    logger.error(f"[Data] 备份损坏 {key}: {e}")
        with self._lock:
            self._cache[key] = data
        return data

    def save(self, key, data):
        with self._lock:
            self._cache[key] = data
            self._dirty_keys.add(key)
            self._version_cache[key] = self._version_cache.get(key, 0) + 1

    def get(self, key):
        return self.load(key)

    def set(self, key, data):
        self.save(key, data)

    def mark_dirty(self, key):
        with self._lock:
            self._dirty_keys.add(key)

    def flush_all(self):
        with self._lock:
            keys = list(self._dirty_keys)
            self._dirty_keys.clear()
        for k in keys:
            self._flush_one(k)

    def _flush_one(self, key):
        try:
            with self._lock:
                if key not in self._cache:
                    return
                data = self._cache[key]
            path = self._path(key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            save = {
                "version": CONFIG["data_version"],
                "data": data,
                "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "version_id": self._version_cache.get(key, 0)
            }
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(save, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            if CONFIG["enable_double_backup"]:
                shutil.copy2(path, self._backup_path(key))
        except Exception as e:
            logger.error(f"[Data] 保存失败 {key}: {e}")

    def _start_flush_worker(self):
        def w():
            while True:
                time.sleep(CONFIG["flush_interval"])
                try:
                    self.flush_all()
                except Exception as e:
                    logger.error(f"[Data] 落盘异常: {e}")
        threading.Thread(target=w, daemon=True).start()

    def graceful_shutdown(self):
        logger.info("[Data] 保存所有数据...")
        self.flush_all()
        logger.info("[Data] 完成")

