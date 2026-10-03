#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""V18 WorkerPool 闲置设备池（从 src_m_reality.py 迁移）。

RikuWorkerNode + WorkerPool + Worker 池状态命令。
"""
import os, sys, json, time, logging, threading, requests
from qqagent.core import CONFIG, logger, dm, context
from qqagent.core.commands import register_command
from qqagent.tools.reality import RikuDevice, DeviceType, RiskLevel

if CONFIG["modules"].get("workers", True) and context.device_manager is not None:
    _WK_CFG = CONFIG.get("workers_config") or {}
    _WK_IDLE_THRESHOLD = int(_WK_CFG.get("idle_threshold", 300))
    _WK_LOAD_LIMIT = float(_WK_CFG.get("load_limit", 0.6))

    class RikuWorkerNode:
        """Riku Worker 节点适配器：HTTP 通信 + 心跳 + 自动重连"""

        def __init__(self, worker_id, endpoint, token="", kind="linux"):
            self.worker_id = worker_id
            self.endpoint = (endpoint or "").rstrip("/")
            self.token = token
            self.kind = kind
            self.connected = False
            self.last_heartbeat = 0
            self.load = 0.0
            self.capabilities = []
            self._fail_streak = 0

        def ping(self):
            if not self.endpoint:
                self.connected = False
                return False
            try:
                resp = requests.get(self.endpoint + "/ping", timeout=8,
                                    headers={"Authorization": "Bearer " + self.token}
                                    if self.token else {})
                if resp.status_code != 200:
                    self._fail_streak += 1
                    self.connected = self._fail_streak < 3
                    return self.connected
                data = resp.json()
                self.connected = True
                self._fail_streak = 0
                self.last_heartbeat = time.time()
                self.load = float(data.get("load", 0.0))
                self.capabilities = data.get("capabilities", []) or []
                return True
            except Exception:
                self._fail_streak += 1
                self.connected = self._fail_streak < 3
                return self.connected

        def send_task(self, operation, params, timeout=None):
            if not self.endpoint:
                raise ConnectionError("worker endpoint empty")
            try:
                resp = requests.post(self.endpoint + "/task", timeout=timeout or 30,
                                     json={"operation": operation, "params": params or {}},
                                     headers={"Authorization": "Bearer " + self.token}
                                     if self.token else {})
                if resp.status_code != 200:
                    raise ConnectionError("worker http %d" % resp.status_code)
                data = resp.json()
                return {"ok": data.get("ok", True), "data": data.get("data", data)}
            except requests.RequestException as e:
                raise ConnectionError("worker error: %s" % e)

    class WorkerPool:
        """闲置设备资源池：空闲检测 / 动态加入退出 / 调度 / 迁移 / 失败重调度"""

        def __init__(self, cfg):
            self.cfg = cfg
            self._workers = {}
            self._devices = {}
            self._idle_since = {}
            self._busy = {}
            self._alive = False
            self._lock = threading.Lock()

        def register_worker(self, worker_id, endpoint, token="", kind="linux",
                            name=None):
            with self._lock:
                node = RikuWorkerNode(worker_id, endpoint, token, kind)
                dev = RikuDevice(worker_id, DeviceType.ANDROID,
                                 name=name or ("Worker-" + worker_id),
                                 capabilities=["run_job", "run_script"],
                                 op_risks={"run_job": RiskLevel.MEDIUM,
                                           "run_script": RiskLevel.HIGH},
                                 forbidden_ops=["run_script"])
                dev.node = node
                dev.metadata = dict(getattr(dev, "metadata", {}) or {},
                                    kind=kind, pool="idle_pool")
                self._workers[worker_id] = node
                self._devices[worker_id] = dev
                self._idle_since[worker_id] = time.time()
                self._busy[worker_id] = False
                context.device_manager.register_device(dev)
                return {"ok": True, "worker_id": worker_id}

        def unregister_worker(self, worker_id):
            with self._lock:
                dev = self._devices.pop(worker_id, None)
                self._workers.pop(worker_id, None)
                self._idle_since.pop(worker_id, None)
                self._busy.pop(worker_id, None)
                if dev is not None:
                    dev.enabled = False     # RikuDeviceManager 无 remove_device：禁用即可
                return {"ok": True}

        # ---------- 空闲检测（动态加入/退出资源池） ----------
        def _poll_once(self):
            for wid, node in list(self._workers.items()):
                try:
                    online = node.ping()
                except Exception:
                    online = False
                if not online:
                    continue
                if node.load <= _WK_LOAD_LIMIT:
                    self._idle_since.setdefault(wid, time.time())
                    if time.time() - self._idle_since[wid] >= _WK_IDLE_THRESHOLD:
                        self._busy[wid] = False
                else:
                    self._idle_since[wid] = time.time()
                    self._busy[wid] = True

        def idle_workers(self):
            return [wid for wid, n in self._workers.items()
                    if n.connected and not self._busy.get(wid)]

        # ---------- 调度 + 迁移 + 失败重调度 ----------
        def dispatch(self, operation, params=None, actor="agent",
                     prefer=None, max_migrations=2):
            """在空闲 worker 上执行任务；节点失败 → 迁移到下一个空闲 worker"""
            candidates = self.idle_workers()
            if prefer and prefer in self._workers and \
                    self._workers[prefer].connected:
                candidates.insert(0, prefer)
            last_err = None
            tried = []
            for wid in candidates:
                if wid in tried:
                    continue
                tried.append(wid)
                self._busy[wid] = True
                try:
                    r = context.tool_gateway.execute(wid, operation, params, actor=actor)
                    if r.get("ok"):
                        return {"ok": True, "worker": wid,
                                "result": r.get("result"),
                                "migrations": len(tried) - 1}
                    last_err = r
                    if r.get("result", {}).get("reason") != "node_error" and \
                            r.get("reason") not in ("node_error", "task_failed",
                                                    "task_timeout"):
                        return r
                except Exception as e:
                    last_err = {"ok": False, "reason": "dispatch_error",
                                "error": str(e)}
                finally:
                    self._busy[wid] = False
                if len(tried) >= max_migrations + 1:
                    break
            return {"ok": False, "reason": "no_worker_available",
                    "error": last_err, "tried": tried}

        def start(self):
            if self._alive:
                return
            self._alive = True

            def _loop():
                while self._alive:
                    try:
                        self._poll_once()
                    except Exception as e:
                        logger.warning("[V18] Worker 池轮询失败: %s" % e)
                    time.sleep(int(self.cfg.get("poll_interval", 30)))

            threading.Thread(target=_loop, daemon=True).start()

        def stop(self):
            self._alive = False

        def status(self):
            return {"workers": len(self._workers),
                    "online": sum(1 for n in self._workers.values() if n.connected),
                    "idle": self.idle_workers(),
                    "busy": [w for w, b in self._busy.items() if b]}

        def flush(self):
            try:
                dm.save("worker_registry", {
                    wid: {"endpoint": n.endpoint, "kind": n.kind,
                          "connected": n.connected, "load": n.load,
                          "capabilities": n.capabilities}
                    for wid, n in self._workers.items()})
                dm.flush_all()
            except Exception as e:
                logger.warning("[V18] Worker 注册表落盘失败: %s" % e)

    _worker_pool = WorkerPool(_WK_CFG)

    # 从配置注册静态 Worker（Tiny Container / Android Linux / 任意 Linux 主机）
    for _w in _WK_CFG.get("workers") or []:
        try:
            _worker_pool.register_worker(
                _w.get("id") or _w.get("name"),
                _w.get("endpoint", ""),
                token=_w.get("token", ""),
                kind=_w.get("kind", "linux"),
                name=_w.get("name"))
        except Exception as e:
            logger.warning("[V18] Worker 注册失败 %r: %s" % (_w, e))

    _worker_pool.start()
    try:
        _DIRTY_MODULES["worker_registry"] = _worker_pool
    except NameError:
        pass

    # Tiny Container 定位说明（TODO 四：明确职责边界）
    _TINY_CONTAINER_NOTE = (
        "Tiny Container 定位：Android 上的 Linux 工作环境（跑 Debian/脚本/常驻 Worker），"
        "本身不做集群调度；调度由主机侧 WorkerPool 负责。部署后在该环境运行 worker.py：\n"
        "  from http.server import BaseHTTPRequestHandler, HTTPServer\n"
        "  # /ping 返回 {'load': psutil.cpu_percent()/100, 'capabilities': [...]}\n"
        "  # /task 执行 operation（白名单 shell）并返回 {'ok': True, 'data': ...}\n"
        "  # 开机自启：Android 用 Termux:Boot / 前台服务；Linux 用 systemd 或 init.d\n"
        "Worker 协议与 Riku Node 一致（心跳/能力/任务/结果/错误），支持自动重连。")

    @register_command("private", ["Worker池状态", "闲置设备池"], perm_required=2,
                      owner_only=True)
    def _cmd_worker_status(msg):
        st = _worker_pool.status()
        lines = [
            "【闲置设备 Worker 池】",
            "Worker 总数: %d · 在线: %d · 空闲: %d · 忙: %d" %
            (st["workers"], st["online"], len(st["idle"]), len(st["busy"])),
            "空闲列表: %s" % ("、".join(st["idle"]) if st["idle"] else "无"),
            "",
            "Tiny Container 定位：",
            _TINY_CONTAINER_NOTE.replace("\n", " ")[:200],
        ]
        return "\n".join(lines)
