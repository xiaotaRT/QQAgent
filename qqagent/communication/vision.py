#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""V16 视觉系统（从 src_m_reality.py 迁移）。

VisionImageRouter + VisionEventBus + VisualMemoryStore + vision-gateway 设备注册。
"""
import os, sys, json, time, logging, threading, re
from qqagent.core import CONFIG, logger, context
from qqagent.tools.reality import RikuDevice, DeviceType, RiskLevel
from qqagent.core.utils import _now_str
from qqagent.core.commands import register_command

if CONFIG["modules"].get("vision_system", True) and context.device_manager is not None:
    try:
        from PIL import Image as _PILImage
        import io as _io
        import hashlib as _hashlib
        _VISION_PIL_OK = True
    except Exception:
        _VISION_PIL_OK = False

    _VIS_CFG = CONFIG.get("vision_config", {})
    _VIS_MODEL = _VIS_CFG.get("model") or {}
    _VIS_MODEL_PROVIDER = (_VIS_MODEL.get("provider") or "none").lower()
    _VIS_CAPTURE_INTERVAL = int(_VIS_CFG.get("capture_interval", 10))
    _VIS_EVENT_COOLDOWN = int(_VIS_CFG.get("event_cooldown", 30))
    _VIS_SAVE_RAW = bool(_VIS_CFG.get("save_raw", False))
    _VIS_RAW_CAP = int(_VIS_CFG.get("raw_cap", 100))

    # ---------- 摄像头源（RTSP / HTTP 快照 / 本机摄像头） ----------
    class CameraSource:
        def __init__(self, cid, name, ctype, url="", rtsp="", snapshot="", device=""):
            self.camera_id = cid
            self.name = name or cid
            self.ctype = ctype or "http_snapshot"
            self.url = url or ""
            self.rtsp = rtsp or ""
            self.snapshot_url = snapshot or ""
            self.device = device or ""
            self.enabled = True
            self.last_frame_hash = None
            self.last_frame_ts = 0
            self.error = ""

        def capture(self):
            if not _VISION_PIL_OK:
                return None, {"ok": False, "reason": "pil_unavailable"}
            try:
                if self.ctype == "rtsp":
                    return self._capture_rtsp()
                if self.ctype == "webcam":
                    return self._capture_webcam()
                return self._capture_http()
            except Exception as e:
                self.error = str(e)
                return None, {"ok": False, "reason": "capture_error", "error": str(e)}

        def _capture_http(self):
            import urllib.request as _ur
            src = self.snapshot_url or self.url
            with _ur.urlopen(src, timeout=10) as resp:
                data = resp.read()
            img = _PILImage.open(_io.BytesIO(data)).convert("RGB")
            return img, {"ok": True, "source": src}

        def _capture_rtsp(self):
            import subprocess as _sp
            src = self.rtsp or self.url
            cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error",
                   "-rtsp_transport", "tcp", "-i", src, "-frames:v", "1",
                   "-f", "image2pipe", "-vcodec", "png", "-"]
            proc = _sp.run(cmd, capture_output=True, timeout=15)
            if proc.returncode != 0 or not proc.stdout:
                return None, {"ok": False, "reason": "rtsp_error",
                              "error": proc.stderr.decode("utf-8", "replace")[:300]}
            img = _PILImage.open(_io.BytesIO(proc.stdout)).convert("RGB")
            return img, {"ok": True, "source": src}

        def _capture_webcam(self):
            import subprocess as _sp
            dev = self.device or "video=Integrated Camera"
            cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error",
                   "-f", "dshow", "-i", dev, "-frames:v", "1",
                   "-f", "image2pipe", "-vcodec", "png", "-"]
            proc = _sp.run(cmd, capture_output=True, timeout=15)
            if proc.returncode != 0 or not proc.stdout:
                return None, {"ok": False, "reason": "webcam_error",
                              "error": proc.stderr.decode("utf-8", "replace")[:300]}
            img = _PILImage.open(_io.BytesIO(proc.stdout)).convert("RGB")
            return img, {"ok": True, "source": dev}

        def snapshot(self):
            """按需截图：存文件并返回路径（隐私：默认禁止 Agent 自主访问）"""
            img, meta = self.capture()
            if img is None:
                return meta
            cap_dir = os.path.join(CONFIG["data_dir"], "vision")
            os.makedirs(cap_dir, exist_ok=True)
            path = os.path.join(cap_dir, "%s_%s.jpg" %
                                (self.camera_id, time.strftime("%Y%m%d_%H%M%S")))
            img.save(path, quality=88)
            return {"ok": True, "path": path, "camera": self.name,
                    "size": list(img.size), "hash": _img_dhash(img)}

        def change_detect(self, threshold=0.08):
            img, meta = self.capture()
            if img is None:
                return {"ok": False, "reason": meta.get("reason", "capture_failed")}
            h = _img_dhash(img)
            now = time.time()
            prev = self.last_frame_hash
            self.last_frame_hash = h
            self.last_frame_ts = now
            if prev is None:
                return {"ok": True, "first_frame": True, "changed": False,
                        "diff": 0.0, "ts": _now_str()}
            diff = _dhash_distance(prev, h) / 64.0
            return {"ok": True, "changed": diff >= threshold,
                    "diff": round(diff, 4), "keyframe": diff >= threshold * 2,
                    "ts": _now_str(), "hash": h}

    def _img_dhash(img, size=8):
        _rs = getattr(_PILImage, "Resampling", None)
        _resample = getattr(_rs, "LANCZOS", 1) if _rs is not None else 1
        g = img.convert("L").resize((size + 1, size), _resample)
        px = list(g.getdata())
        bits = 0
        for i in range(size):
            row = i * size
            for j in range(size):
                bits = (bits << 1) | (1 if px[row + j] > px[row + j + 1] else 0)
        return bits

    def _dhash_distance(a, b):
        return bin(a ^ b).count("1")

    # ---------- 图像理解路由（默认不调用模型） ----------
    class VisionImageRouter:
        """视觉模型路由：none / openai_compatible / omniparser / local_http"""

        def __init__(self, cfg):
            self.provider = (cfg.get("provider") or "none").lower()
            self.base_url = (cfg.get("base_url") or "").rstrip("/")
            self.model = cfg.get("model") or ""
            self.api_key = cfg.get("api_key") or ""
            self.available = self.provider != "none" and bool(self.base_url)
            self._cache = {}

        def analyze(self, image, prompt="描述这张图片", cache=True):
            if not self.available:
                return {"ok": False, "reason": "vision_model_not_configured",
                        "message": "未配置视觉模型（vision_config.model），默认不调用大型视觉模型"}
            if isinstance(image, bytes):
                raw = image
            else:
                buf = _io.BytesIO()
                image.save(buf, format="JPEG", quality=85)
                raw = buf.getvalue()
            h = _hashlib.sha256(raw).hexdigest()[:16]
            if cache and h in self._cache:
                return dict(self._cache[h], cached=True)
            if self.provider == "openai_compatible":
                r = self._call_openai_compat(raw, prompt)
            else:
                r = self._call_local_http(raw, prompt)
            if r.get("ok") and cache:
                self._cache[h] = dict(r)
            return r

        def _call_openai_compat(self, raw, prompt):
            import base64 as _b64
            try:
                b64 = _b64.b64encode(raw).decode("ascii")
                resp = requests.post(
                    self.base_url + "/chat/completions", timeout=60,
                    headers={"Authorization": "Bearer " + self.api_key,
                             "Content-Type": "application/json"},
                    json={"model": self.model, "max_tokens": 800, "temperature": 0.2,
                          "messages": [{"role": "user", "content": [
                              {"type": "text", "text": prompt},
                              {"type": "image_url",
                               "image_url": {"url": "data:image/jpeg;base64," + b64}}]}]})
                if resp.status_code != 200:
                    return {"ok": False, "reason": "model_http_%d" % resp.status_code}
                text = resp.json()["choices"][0]["message"]["content"]
                return {"ok": True, "text": text}
            except Exception as e:
                return {"ok": False, "reason": "model_error", "error": str(e)}

        def _call_local_http(self, raw, prompt):
            import base64 as _b64
            try:
                b64 = _b64.b64encode(raw).decode("ascii")
                resp = requests.post(self.base_url + "/analyze", timeout=60,
                                     json={"image": b64, "prompt": prompt})
                if resp.status_code != 200:
                    return {"ok": False, "reason": "model_http_%d" % resp.status_code}
                return {"ok": True, "text": resp.json().get("text", "")}
            except Exception as e:
                return {"ok": False, "reason": "model_error", "error": str(e)}

    _vis_router = VisionImageRouter(_VIS_MODEL)

    # ---------- GUI 视觉（OmniParser 可选） ----------
    class OmniParserClient:
        """截图 → UI 元素结构化（OmniParser-MCP / 本地 HTTP 服务）"""

        def __init__(self, cfg):
            self.endpoint = (cfg.get("endpoint") or "").rstrip("/")
            self.available = bool(self.endpoint)

        def parse(self, image):
            if not self.available:
                return {"ok": False, "reason": "omniparser_not_configured"}
            buf = _io.BytesIO()
            image.save(buf, format="JPEG", quality=85)
            import base64 as _b64
            try:
                resp = requests.post(self.endpoint + "/parse", timeout=60,
                                     json={"image": _b64.b64encode(buf.getvalue()).decode("ascii")})
                if resp.status_code != 200:
                    return {"ok": False, "reason": "op_http_%d" % resp.status_code}
                data = resp.json()
                elements = data.get("elements") or data.get("parsed_content") or []
                return {"ok": True, "count": len(elements), "elements": elements[:200]}
            except Exception as e:
                return {"ok": False, "reason": "op_error", "error": str(e)}

    _omniparser = OmniParserClient(_VIS_CFG.get("omniparser") or {})

    # ---------- 视觉事件系统 ----------
    class VisionEventBus:
        """视觉事件总线：变化/人物/物体/异常/OCR/屏幕状态事件，节流防刷屏"""

        EVENT_TYPES = ["frame_change", "person_appear", "person_leave",
                       "object_appear", "object_disappear", "anomaly",
                       "ocr_change", "screen_state_change"]

        def __init__(self, cooldown=30):
            self.cooldown = cooldown
            self._last = {}
            self._events = []
            self._listeners = []

        def add_listener(self, fn):
            self._listeners.append(fn)

        def emit(self, camera_id, etype, payload=None, priority=1):
            now = time.time()
            key = (camera_id, etype)
            if now - self._last.get(key, 0) < self.cooldown:
                return False            # 节流防刷屏
            self._last[key] = now
            ev = {"ts": _now_str(), "camera": camera_id, "type": etype,
                  "priority": priority, "payload": payload or {}}
            self._events.append(ev)
            self._events = self._events[-200:]
            for fn in list(self._listeners):
                try:
                    fn(ev)
                except Exception as e:
                    logger.warning("[V16] 视觉事件监听器异常: %s" % e)
            return True

        def recent(self, limit=20):
            return self._events[-limit:]

        def clear(self):
            self._events = []

    _vis_events = VisionEventBus(cooldown=_VIS_EVENT_COOLDOWN)

    # ---------- 视觉记忆 ----------
    class VisualMemoryStore:
        """视觉记忆：摘要/人物/物体/时间/来源/事件 + 检索 + 相似关联 + "以前见过" """

        def __init__(self):
            self._entries = []
            self._by_hash = {}
            self._load()

        def _load(self):
            try:
                data = dm.load("visual_memory", {})
                self._entries = data.get("entries", [])
                self._by_hash = {e.get("hash"): e for e in self._entries
                                 if e.get("hash")}
            except Exception:
                pass

        def _persist(self):
            try:
                dm.save("visual_memory", {"entries": self._entries[-500:]})
                dm.flush_all()
            except Exception as e:
                logger.warning("[V16] 视觉记忆落盘失败: %s" % e)

        def save(self, image=None, summary="", objects=None, persons=None,
                 ocr="", source="", camera_id="", event_type="", importance=0.5,
                 image_hash=None, raw_path=None):
            if image is not None:
                image_hash = image_hash or _img_dhash(image)
            if not summary and image is None:
                return {"ok": False, "reason": "nothing_to_save"}
            entry = {
                "ts": _now_str(), "time": time.time(),
                "summary": summary, "objects": objects or [],
                "persons": persons or [], "ocr": ocr or "",
                "source": source, "camera_id": camera_id,
                "event_type": event_type, "importance": importance,
                "hash": image_hash or "", "raw_path": raw_path or "",
            }
            self._entries.append(entry)
            self._entries = self._entries[-500:]
            if entry["hash"]:
                self._by_hash[entry["hash"]] = entry
            self._persist()
            return {"ok": True, "memory_id": len(self._entries)}

        def search(self, keyword=""):
            kw = (keyword or "").lower()
            rows = []
            for e in reversed(self._entries):
                if kw and not (kw in e["summary"].lower() or
                               kw in " ".join(e["objects"]).lower() or
                               kw in " ".join(e["persons"]).lower() or
                               kw in e["ocr"].lower()):
                    continue
                rows.append(e)
            return {"ok": True, "count": len(rows), "entries": rows[:50]}

        def recent(self, limit=20):
            return {"ok": True, "entries": self._entries[-limit:][::-1]}

        def find_similar(self, image=None, image_hash=None, threshold=4):
            """dHash 汉明距离 ≤ threshold 视为相似（"以前见过"）"""
            if image is None and not image_hash:
                return {"ok": False, "reason": "missing_image"}
            h = image_hash or _img_dhash(image)
            hit = self._by_hash.get(h)
            if hit:
                return {"ok": True, "seen": True, "matched": hit}
            for e in self._entries:
                if e.get("hash") and _dhash_distance(h, e["hash"]) <= threshold:
                    return {"ok": True, "seen": True, "similar_to": e}
            return {"ok": True, "seen": False}

        def flush(self):
            self._persist()

    _visual_memory = VisualMemoryStore()

    # ---------- 摄像头管理 ----------
    _cameras = {}
    for _cam in _VIS_CFG.get("cameras") or []:
        try:
            _c = CameraSource(_cam.get("id") or _cam.get("name"),
                              _cam.get("name", ""), _cam.get("type", "http_snapshot"),
                              url=_cam.get("url", ""), rtsp=_cam.get("rtsp", ""),
                              snapshot=_cam.get("snapshot", ""),
                              device=_cam.get("device", ""))
            _c.enabled = bool(_cam.get("enabled", True))
            _cameras[_c.camera_id] = _c
        except Exception as e:
            logger.warning("[V16] 摄像头配置无效 %r: %s" % (_cam, e))

    # ---------- 视觉事件 watcher（变化检测 + 可选模型低频分析） ----------
    def _vision_watch_loop():
        while _vision_watcher_alive:
            for _c in list(_cameras.values()):
                if not _c.enabled:
                    continue
                try:
                    r = _c.change_detect(threshold=_VIS_CFG.get("change_threshold", 0.08))
                    if r.get("ok") and r.get("changed"):
                        _vis_events.emit(_c.camera_id, "frame_change",
                                         {"diff": r.get("diff")}, priority=2)
                        if r.get("keyframe") and _vis_router.available:
                            _img, _ = _c.capture()
                            if _img is not None:
                                _an = _vis_router.analyze(
                                    _img, "简要描述画面内容，说明有无人物、物体或异常")
                                if _an.get("ok"):
                                    _visual_memory.save(
                                        image=_img, summary=_an["text"],
                                        source=_c.name, camera_id=_c.camera_id,
                                        event_type="frame_change", importance=0.6)
                                _vis_events.emit(_c.camera_id, "anomaly",
                                                 {"summary": _an.get("text", "")[:200]},
                                                 priority=3)
                except Exception as e:
                    logger.warning("[V16] 摄像头 %s 监控异常: %s" % (_c.camera_id, e))
            time.sleep(_VIS_CAPTURE_INTERVAL)

    _vision_watcher_alive = False
    _vision_watcher = None

    def _vision_start_watcher():
        global _vision_watcher_alive, _vision_watcher
        if _vision_watcher_alive or not _cameras:
            return
        _vision_watcher_alive = True
        _vision_watcher = threading.Thread(target=_vision_watch_loop, daemon=True)
        _vision_watcher.start()

    def _vision_stop_watcher():
        global _vision_watcher_alive
        _vision_watcher_alive = False

    # ---------- 接入统一网关（摄像头默认禁止 Agent 自主访问） ----------
    _vision_device = RikuDevice("vision-gateway", DeviceType.VISION,
                                name="视觉网关", capabilities=[
                                    "snapshot", "capture_and_analyze", "gui_parse",
                                    "visual_search", "visual_recent", "vision_status"],
                                op_risks={
                                    "snapshot": RiskLevel.HIGH,
                                    "capture_and_analyze": RiskLevel.HIGH,
                                    "gui_parse": RiskLevel.HIGH,
                                    "visual_search": RiskLevel.LOW,
                                    "visual_recent": RiskLevel.LOW,
                                    "vision_status": RiskLevel.LOW,
                                },
                                forbidden_ops=["snapshot", "capture_and_analyze",
                                               "gui_parse"])
    _vision_device.agent_deny_all = True      # 摄像头默认关闭自主访问（TODO 十八）

    def _vis_op_snapshot(params, device=None):
        cid = params.get("camera") or params.get("camera_id") or list(_cameras)[0]
        _c = _cameras.get(cid)
        if not _c:
            return {"ok": False, "reason": "camera_not_found", "camera": cid}
        if not _c.enabled:
            return {"ok": False, "reason": "camera_disabled", "camera": cid}
        return _c.snapshot()

    def _vis_op_analyze(params, device=None):
        cid = params.get("camera") or params.get("camera_id") or list(_cameras)[0]
        _c = _cameras.get(cid)
        if not _c:
            return {"ok": False, "reason": "camera_not_found", "camera": cid}
        if not _vis_router.available:
            return {"ok": False, "reason": "vision_model_not_configured"}
        img, meta = _c.capture()
        if img is None:
            return meta
        prompt = params.get("prompt", "描述这张图片并指出其中的人物、物体与异常")
        r = _vis_router.analyze(img, prompt)
        if r.get("ok"):
            _visual_memory.save(image=img, summary=r["text"], source=_c.name,
                                camera_id=cid, event_type="analysis", importance=0.5)
        return r

    def _vis_op_gui_parse(params, device=None):
        if not _omniparser.available:
            return {"ok": False, "reason": "omniparser_not_configured"}
        cid = params.get("camera") or params.get("camera_id") or list(_cameras)[0]
        _c = _cameras.get(cid)
        if not _c:
            return {"ok": False, "reason": "camera_not_found", "camera": cid}
        img, meta = _c.capture()
        if img is None:
            return meta
        return _omniparser.parse(img)

    def _vis_op_search(params, device=None):
        return _visual_memory.search(params.get("keyword", ""))

    def _vis_op_recent(params, device=None):
        return _visual_memory.recent(int(params.get("limit", 20)))

    _vision_device.local_handlers = {
        "snapshot": _vis_op_snapshot,
        "capture_and_analyze": _vis_op_analyze,
        "gui_parse": _vis_op_gui_parse,
        "visual_search": _vis_op_search,
        "visual_recent": _vis_op_recent,
    }
    _vision_device.hard_blocked_ops = set()

    # 视觉事件 → Agent 触发 + Memory（TODO 八/九）
    _vis_events.add_listener(lambda ev: _visual_memory.save(
        summary="[视觉事件] %s：%s" % (ev["type"], ev["camera"]),
        source=ev["camera"], camera_id=ev["camera"], event_type=ev["type"],
        importance=0.7))
    _vis_events.add_listener(lambda ev: _notify_visual_event(ev))

    def _notify_visual_event(ev):
        try:
            if ev["priority"] >= 2:
                context.tool_gateway._notify("[视觉事件] %s 摄像头发生%s（重要度%d）" %
                                     (ev["camera"], ev["type"], ev["priority"]))
        except Exception:
            pass

    context.device_manager.register_device(_vision_device)   # 始终注册：能力骨架可见


@register_command("private", ["视觉层状态", "摄像头状态"], perm_required=2,
                  owner_only=True)
def _cmd_vision_status(msg):
    lines = [
        "【视觉层】",
        "摄像头: %d 路" % len(_cameras),
        "视觉模型: %s%s" % (_VIS_MODEL_PROVIDER,
                           "（" + _vis_router.model + "）" if _vis_router.model else ""),
        "OmniParser: %s" % ("已配置 " + _omniparser.endpoint
                            if _omniparser.available else "未配置"),
        "视觉记忆: %d 条 · 事件: %d 条" % (len(_visual_memory._entries),
                                       len(_vis_events._events)),
        "监控线程: %s" % ("运行中" if _vision_watcher_alive else "未启动"),
    ]
    for cid, c in _cameras.items():
        lines.append("· %s [%s] %s %s" % (cid, c.ctype, c.name,
                                          "启用" if c.enabled else "禁用"))
    return "\n".join(lines)


# ============================================================
# 四十二·E V18、闲置设备 Worker 池（Tiny Container）
# ============================================================
# 对应【里克现实世界能力建设 TODO】第四章：
