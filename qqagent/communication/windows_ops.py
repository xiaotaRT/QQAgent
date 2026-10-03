#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""V14 Windows 现实交互（从 src_m_reality.py 迁移）。

跨平台：Windows 真实执行；非 Windows 返回 unsupported_platform（能力仍注册）。
"""
import os, sys, json, time, logging, threading
from qqagent.core import CONFIG, logger, context
from qqagent.tools.reality import RikuDevice, DeviceType, RiskLevel
from qqagent.core.commands import register_command

# ============================================================
# 四十二·A V14、Windows 现实交互
# ============================================================
# 对应【里克现实世界能力建设 TODO】第二章：
#   系统状态 / 进程列表 / 进程详情 / 白名单启动进程 / 终止进程（系统进程硬拦截）/
#   窗口列表 / 聚焦窗口 / 窗口截图 / 鼠标移动 / 点击 / 滚动 / 键盘输入 / 热键 /
#   受限目录文件列读写下删 / 屏幕截图 / 剪贴板 / 白名单 PowerShell /
#   注册表读+写（默认禁）删（硬禁止）/
#   纳入权限/风险/确认体系 / 危险 PS 默认禁止 / 禁止大规模删除 / 禁止任意进程终止
# 跨平台：Windows 真实执行；非 Windows 平台返回 unsupported_platform（能力仍注册）
# ============================================================
if CONFIG["modules"].get("windows_ops", True) and context.device_manager is not None:
    _WINDOWS_CFG = CONFIG.get("windows_config", {})
    _IS_WIN = sys.platform.startswith("win") if "sys" in globals() else False
    try:
        import sys as _sys
        _IS_WIN = _sys.platform.startswith("win")
    except Exception:
        _IS_WIN = False

    try:
        import psutil as _psutil
        _HAS_PSUTIL = True
    except Exception:
        _HAS_PSUTIL = False

    _HAS_WINDOWS_OPS = True          # 能力层已启用（实际执行按平台守卫）

    # 危险命令模式（命令级硬拦截：provider 二次防线，owner 也不可绕过）
    _WIN_HARD_BLOCKED = set(_WINDOWS_CFG.get("hard_blocked_ops") or [
        "format c:", "del /s", "rmdir /s", "rd /s", "reg delete", "reg import",
        "taskkill /f /im svchost", "shutdown /s", "shutdown /r", "diskpart",
        "rm -rf", ":(){", "remove-item -recurse -force c:", "clear-disk",
    ])

    # Agent 黑名单：默认禁止，owner 二次确认可临时放行
    _WIN_FORBIDDEN_AGENT = set(_WINDOWS_CFG.get("forbidden_agent_ops") or [
        "stop_process", "delete_file", "registry_write", "registry_delete",
        "run_powershell", "start_process", "write_file", "set_clipboard",
        "keyboard_hotkey", "mouse_click", "focus_window",
    ])

    _WIN_OP_RISKS = {
        "system_status": RiskLevel.LOW,
        "list_processes": RiskLevel.LOW,
        "process_info": RiskLevel.LOW,
        "list_windows": RiskLevel.LOW,
        "list_files": RiskLevel.LOW,
        "device_status": RiskLevel.LOW,
        "process_start_whitelist": RiskLevel.MEDIUM,
        "start_process": RiskLevel.MEDIUM,
        # 黑名单操作（_WIN_FORBIDDEN_AGENT）由 forbidden 闸门对 owner 做单次强确认
        # （ticket risk=CRITICAL），此处标 MEDIUM 避免与单一确认闸门（risk>=HIGH 再建 ticket）
        # 叠加成双重确认；agent 一律在 forbidden 闸门被拒。
        "stop_process": RiskLevel.MEDIUM,
        "focus_window": RiskLevel.MEDIUM,
        "window_snapshot": RiskLevel.HIGH,
        "mouse_move": RiskLevel.MEDIUM,
        "mouse_click": RiskLevel.MEDIUM,
        "mouse_scroll": RiskLevel.MEDIUM,
        "keyboard_type": RiskLevel.MEDIUM,
        "keyboard_hotkey": RiskLevel.MEDIUM,
        "screen_snapshot": RiskLevel.HIGH,
        "get_clipboard": RiskLevel.LOW,
        "set_clipboard": RiskLevel.MEDIUM,
        "read_file": RiskLevel.LOW,
        "write_file": RiskLevel.MEDIUM,
        "delete_file": RiskLevel.MEDIUM,
        "run_powershell": RiskLevel.MEDIUM,
        "registry_read": RiskLevel.LOW,
        "registry_write": RiskLevel.MEDIUM,
        "registry_delete": RiskLevel.CRITICAL,
    }

    _WIN_PS_WHITELIST = _WINDOWS_CFG.get("ps_whitelist") or [
        "get-process", "get-service", "get-computerinfo", "get-netipconfiguration",
        "get-disk", "get-volume", "get-wmiobject win32_operatingsystem",
        "get-itemproperty", "get-childitem", "test-connection", "get-date",
        "get-process -name",
    ]

    # 受限目录：默认仅用户目录
    _home_dir = os.path.expanduser("~")
    _WIN_ALLOWED_DIRS = _WINDOWS_CFG.get("allowed_dirs") or [_home_dir]

    def _win_path_allowed(path):
        path = os.path.abspath(os.path.expanduser(str(path or "")))
        for base in _WIN_ALLOWED_DIRS:
            base = os.path.abspath(os.path.expanduser(str(base)))
            if path == base or path.startswith(base + os.sep):
                return True
        return False

    class WindowsOpsProvider:
        """Windows 现实交互提供者（ctypes + psutil + PIL；非 Windows 返回 unsupported_platform）"""

        # ---------- 系统 / 进程 ----------
        def system_status(self, params, device=None):
            import platform as _pl
            try:
                import psutil as _ps
                return {"ok": True, "hostname": _pl.node(), "platform": _pl.platform(),
                        "python": _pl.python_version(),
                        "cpu_percent": _ps.cpu_percent(interval=None),
                        "cpu_cores": _ps.cpu_count(logical=True),
                        "memory_percent": _ps.virtual_memory().percent,
                        "memory_used_mb": round(_ps.virtual_memory().used / 1048576, 1),
                        "memory_total_mb": round(_ps.virtual_memory().total / 1048576, 1),
                        "disk_used_percent": _ps.disk_usage("/").percent,
                        "boot_time": time.strftime("%Y-%m-%d %H:%M:%S",
                                                   time.localtime(_ps.boot_time())),
                        "uptime_seconds": int(time.time() - _ps.boot_time())}
            except Exception as e:
                return {"ok": False, "reason": "psutil_unavailable", "error": str(e)}

        def device_status(self, params, device=None):
            return {"ok": True, "platform": _sys.platform,
                    "hostname": getattr(_sys, "platform", ""),
                    "os": _sys.platform}

        def list_processes(self, params, device=None):
            if not _HAS_PSUTIL:
                return {"ok": False, "reason": "psutil_unavailable"}
            try:
                limit = int(params.get("limit", 20))
                rows = []
                for p in _psutil.process_iter(["pid", "name", "cpu_percent",
                                               "memory_percent", "status"]):
                    try:
                        rows.append({"pid": p.info["pid"], "name": p.info["name"],
                                     "cpu": round(p.info["cpu_percent"] or 0, 1),
                                     "mem": round(p.info["memory_percent"] or 0, 1),
                                     "status": p.info["status"]})
                    except Exception:
                        continue
                rows.sort(key=lambda r: r["mem"], reverse=True)
                return {"ok": True, "count": len(rows), "top": rows[:limit]}
            except Exception as e:
                return {"ok": False, "reason": "process_list_error", "error": str(e)}

        def process_info(self, params, device=None):
            if not _HAS_PSUTIL:
                return {"ok": False, "reason": "psutil_unavailable"}
            try:
                pid = int(params.get("pid", 0))
                p = _psutil.Process(pid)
                with p.oneshot():
                    return {"ok": True, "pid": pid, "name": p.name(),
                            "status": p.status(), "cpu": p.cpu_percent(interval=None),
                            "mem_mb": round(p.memory_info().rss / 1048576, 1),
                            "create_time": time.strftime("%Y-%m-%d %H:%M:%S",
                                                         time.localtime(p.create_time())),
                            "cmdline": " ".join(p.cmdline())[:300]}
            except Exception as e:
                return {"ok": False, "reason": "process_not_found", "error": str(e)}

        def start_process(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            if not _HAS_PSUTIL:
                return {"ok": False, "reason": "psutil_unavailable"}
            try:
                name = str(params.get("name") or params.get("path") or "")
                if not name:
                    return {"ok": False, "reason": "missing_process"}
                # 白名单检查
                wl = _WINDOWS_CFG.get("process_whitelist") or []
                if wl and not any(w.lower() in name.lower() for w in wl):
                    return {"ok": False, "reason": "process_not_whitelisted"}
                p = _psutil.Popen([name], stdout=open(os.devnull, "w"),
                                  stderr=open(os.devnull, "w"))
                return {"ok": True, "pid": p.pid, "name": name}
            except Exception as e:
                return {"ok": False, "reason": "start_failed", "error": str(e)}

        def stop_process(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            if not _HAS_PSUTIL:
                return {"ok": False, "reason": "psutil_unavailable"}
            try:
                pid = int(params.get("pid", 0))
                name = str(params.get("name", ""))
                if pid <= 0 and not name:
                    return {"ok": False, "reason": "missing_target"}
                # 系统进程硬拦截
                if pid in (0, 1, 4) or (name and any(
                        n in name.lower() for n in ("system", "svchost", "csrss",
                                                    "winlogon", "lsass", "services"))):
                    return {"ok": False, "reason": "hard_blocked",
                            "message": "系统关键进程禁止终止。"}
                proc = _psutil.Process(pid) if pid else None
                if proc is None and name:
                    for p in _psutil.process_iter(["pid", "name"]):
                        if p.info["name"] and name.lower() in p.info["name"].lower():
                            proc = p
                            break
                if proc is None:
                    return {"ok": False, "reason": "process_not_found"}
                proc.terminate()
                return {"ok": True, "pid": proc.pid, "name": proc.name(),
                        "note": "已发送终止信号"}
            except Exception as e:
                return {"ok": False, "reason": "stop_failed", "error": str(e)}

        # ---------- 窗口 ----------
        def list_windows(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            try:
                import ctypes as _ct
                _EnumWindows = _ct.windll.user32.EnumWindows
                _GetWindowTextW = _ct.windll.user32.GetWindowTextW
                _GetWindowTextLengthW = _ct.windll.user32.GetWindowTextLengthW
                _IsWindowVisible = _ct.windll.user32.IsWindowVisible
                _GetWindowThreadProcessId = _ct.windll.user32.GetWindowThreadProcessId

                result = []

                def _cb(hwnd, _):
                    if not _IsWindowVisible(hwnd):
                        return True
                    length = _GetWindowTextLengthW(hwnd)
                    if length <= 0:
                        return True
                    buf = _ct.create_unicode_buffer(length + 1)
                    _GetWindowTextW(hwnd, buf, length + 1)
                    pid = _ct.c_ulong()
                    _GetWindowThreadProcessId(hwnd, _ct.byref(pid))
                    result.append({"hwnd": hwnd, "title": buf.value[:100],
                                   "pid": pid.value})
                    return True

                _WNDENUMPROC = _ct.WINFUNCTYPE(_ct.c_bool, _ct.c_void_p, _ct.c_void_p)
                _EnumWindows(_WNDENUMPROC(_cb), 0)
                limit = int(params.get("limit", 30))
                return {"ok": True, "count": len(result),
                        "windows": result[:limit]}
            except Exception as e:
                return {"ok": False, "reason": "window_error", "error": str(e)}

        def focus_window(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            try:
                import ctypes as _ct
                hwnd = int(params.get("hwnd", 0))
                if not hwnd:
                    return {"ok": False, "reason": "missing_hwnd"}
                _ct.windll.user32.SetForegroundWindow(hwnd)
                return {"ok": True, "hwnd": hwnd}
            except Exception as e:
                return {"ok": False, "reason": "focus_error", "error": str(e)}

        def window_snapshot(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            try:
                from PIL import ImageGrab as _Grab
                hwnd = int(params.get("hwnd", 0))
                if not hwnd:
                    return {"ok": False, "reason": "missing_hwnd"}
                import ctypes as _ct
                _GetWindowRect = _ct.windll.user32.GetWindowRect
                rect = _ct.wintypes.RECT()
                if not _GetWindowRect(hwnd, _ct.byref(rect)):
                    return {"ok": False, "reason": "window_rect_failed"}
                img = _Grab.grab(bbox=(rect.left, rect.top, rect.right, rect.bottom))
                cap_dir = os.path.join(CONFIG["data_dir"], "vision")
                os.makedirs(cap_dir, exist_ok=True)
                path = os.path.join(cap_dir, "win_%s_%s.png" % (
                    hwnd, time.strftime("%Y%m%d_%H%M%S")))
                img.save(path)
                return {"ok": True, "path": path}
            except Exception as e:
                return {"ok": False, "reason": "window_snapshot_error", "error": str(e)}

        # ---------- 鼠标 / 键盘 ----------
        def mouse_move(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            try:
                import ctypes as _ct
                x, y = int(params.get("x", 0)), int(params.get("y", 0))
                _ct.windll.user32.SetCursorPos(x, y)
                return {"ok": True, "x": x, "y": y}
            except Exception as e:
                return {"ok": False, "reason": "mouse_error", "error": str(e)}

        def mouse_click(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            try:
                import ctypes as _ct
                x, y = int(params.get("x", 0)), int(params.get("y", 0))
                _ct.windll.user32.SetCursorPos(x, y)
                btn = str(params.get("button", "left")).lower()
                down = 0x01 if btn == "left" else 0x04
                up = 0x02 if btn == "left" else 0x08
                _ct.windll.user32.mouse_event(down, 0, 0, 0, 0)
                _ct.windll.user32.mouse_event(up, 0, 0, 0, 0)
                return {"ok": True, "x": x, "y": y, "button": btn}
            except Exception as e:
                return {"ok": False, "reason": "click_error", "error": str(e)}

        def mouse_scroll(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            try:
                import ctypes as _ct
                delta = int(params.get("delta", 120))
                _ct.windll.user32.mouse_event(0x0800, 0, 0, delta, 0)
                return {"ok": True, "delta": delta}
            except Exception as e:
                return {"ok": False, "reason": "scroll_error", "error": str(e)}

        def keyboard_type(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            try:
                import ctypes as _ct
                text = str(params.get("text", ""))
                for ch in text:
                    vk = _ct.windll.user32.VkKeyScanW(ord(ch))
                    _ct.windll.user32.keybd_event(vk & 0xFF, 0, 0, 0)
                    _ct.windll.user32.keybd_event(vk & 0xFF, 0, 2, 0)
                return {"ok": True, "text_len": len(text)}
            except Exception as e:
                return {"ok": False, "reason": "keyboard_error", "error": str(e)}

        def keyboard_hotkey(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            try:
                import ctypes as _ct
                keys = params.get("keys") or []
                vks = {"ctrl": 0x11, "alt": 0x12, "shift": 0x10, "win": 0x5B,
                       "enter": 0x0D, "tab": 0x09, "esc": 0x1B}
                down = []
                for k in keys:
                    k = str(k).lower()
                    vk = vks.get(k, 0)
                    if not vk:
                        return {"ok": False, "reason": "unknown_key", "key": k}
                    _ct.windll.user32.keybd_event(vk, 0, 0, 0)
                    down.append(vk)
                for vk in reversed(down):
                    _ct.windll.user32.keybd_event(vk, 0, 2, 0)
                return {"ok": True, "keys": keys}
            except Exception as e:
                return {"ok": False, "reason": "hotkey_error", "error": str(e)}

        # ---------- 屏幕 / 剪贴板 ----------
        def screen_snapshot(self, params, device=None):
            try:
                from PIL import ImageGrab as _Grab
                img = _Grab.grab()
                cap_dir = os.path.join(CONFIG["data_dir"], "vision")
                os.makedirs(cap_dir, exist_ok=True)
                path = os.path.join(cap_dir, "screen_%s.png" %
                                    time.strftime("%Y%m%d_%H%M%S"))
                img.save(path)
                return {"ok": True, "path": path, "size": list(img.size)}
            except Exception as e:
                return {"ok": False, "reason": "screen_capture_failed", "error": str(e)}

        def get_clipboard(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            try:
                import ctypes as _ct
                _ct.windll.user32.OpenClipboard(0)
                h = _ct.windll.user32.GetClipboardData(13)
                text = ""
                if h:
                    data = _ct.windll.kernel32.GlobalLock(h)
                    if data:
                        text = _ct.c_char_p(data).value.decode("utf-8", "replace")[:4000]
                        _ct.windll.kernel32.GlobalUnlock(h)
                _ct.windll.user32.CloseClipboard()
                return {"ok": True, "text": text}
            except Exception as e:
                return {"ok": False, "reason": "clipboard_error", "error": str(e)}

        def set_clipboard(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            try:
                import ctypes as _ct
                text = str(params.get("text", ""))
                data = text.encode("utf-16-le") + b"\x00\x00"
                h = _ct.windll.kernel32.GlobalAlloc(0x0042, len(data))
                p = _ct.windll.kernel32.GlobalLock(h)
                _ct.cdll.msvcrt.memcpy(p, data, len(data))
                _ct.windll.kernel32.GlobalUnlock(h)
                _ct.windll.user32.OpenClipboard(0)
                _ct.windll.user32.EmptyClipboard()
                _ct.windll.user32.SetClipboardData(13, h)
                _ct.windll.user32.CloseClipboard()
                return {"ok": True, "text_len": len(text)}
            except Exception as e:
                return {"ok": False, "reason": "clipboard_error", "error": str(e)}

        # ---------- 文件（受限目录） ----------
        def list_files(self, params, device=None):
            path = str(params.get("path") or params.get("dir") or _home_dir)
            if not _win_path_allowed(path):
                return {"ok": False, "reason": "file_not_allowed", "path": path}
            try:
                rows = []
                for name in sorted(os.listdir(path))[:int(params.get("limit", 100))]:
                    fp = os.path.join(path, name)
                    try:
                        st = os.stat(fp)
                        rows.append({"name": name, "dir": os.path.isdir(fp),
                                     "size": st.st_size,
                                     "mtime": time.strftime("%Y-%m-%d %H:%M",
                                                            time.localtime(st.st_mtime))})
                    except Exception:
                        continue
                return {"ok": True, "path": path, "count": len(rows), "files": rows}
            except Exception as e:
                return {"ok": False, "reason": "list_failed", "error": str(e)}

        def read_file(self, params, device=None):
            path = str(params.get("path", ""))
            if not _win_path_allowed(path):
                return {"ok": False, "reason": "file_not_allowed", "path": path}
            try:
                with io.open(path, "r", encoding="utf-8", errors="replace") as f:
                    content = f.read(int(params.get("max_len", 8000)))
                return {"ok": True, "path": path, "content": content[:8000]}
            except Exception as e:
                return {"ok": False, "reason": "read_failed", "error": str(e)}

        def write_file(self, params, device=None):
            path = str(params.get("path", ""))
            if not _win_path_allowed(path):
                return {"ok": False, "reason": "file_not_allowed", "path": path}
            try:
                mode = "w" if params.get("mode") != "append" else "a"
                with io.open(path, mode, encoding="utf-8", newline="") as f:
                    f.write(str(params.get("content", "")))
                return {"ok": True, "path": path}
            except Exception as e:
                return {"ok": False, "reason": "write_failed", "error": str(e)}

        def delete_file(self, params, device=None):
            path = str(params.get("path", ""))
            if not _win_path_allowed(path):
                return {"ok": False, "reason": "file_not_allowed", "path": path}
            try:
                if os.path.isdir(path):
                    if os.path.abspath(path) in (
                            os.path.abspath(_home_dir),
                            os.path.abspath(CONFIG["data_dir"])):
                        return {"ok": False, "reason": "hard_blocked",
                                "message": "禁止删除用户目录与数据目录本身。"}
                    import shutil as _sh
                    _sh.rmtree(path)
                else:
                    os.remove(path)
                return {"ok": True, "path": path, "deleted": True}
            except Exception as e:
                return {"ok": False, "reason": "delete_failed", "error": str(e)}

        # ---------- PowerShell（白名单 + 危险模式拦截） ----------
        def run_powershell(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            cmd = str(params.get("command") or params.get("cmd") or "").strip()
            if not cmd:
                return {"ok": False, "reason": "empty_command"}
            low = cmd.lower()
            for bad in _WIN_HARD_BLOCKED:
                if bad in low:
                    return {"ok": False, "reason": "hard_blocked", "matched": bad}
            ok = any(low.startswith(w) or (" " + w) in (" " + low)
                     for w in _WIN_PS_WHITELIST)
            if not ok:
                return {"ok": False, "reason": "ps_not_whitelisted", "command": cmd[:80]}
            try:
                import subprocess as _sp
                proc = _sp.run(
                    ["powershell", "-NoProfile", "-NonInteractive", "-Command", cmd],
                    capture_output=True, text=True, timeout=int(params.get("timeout", 20)),
                    creationflags=getattr(_sp, "CREATE_NO_WINDOW", 0))
                return {"ok": proc.returncode == 0, "exit": proc.returncode,
                        "stdout": proc.stdout[:8000], "stderr": proc.stderr[:2000]}
            except _sp.TimeoutExpired:
                return {"ok": False, "reason": "timeout"}
            except Exception as e:
                return {"ok": False, "reason": "exec_error", "error": str(e)}

        # ---------- 注册表 ----------
        def registry_read(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            try:
                import winreg as _reg
                hive = str(params.get("hive", "HKCU"))
                key = str(params.get("key", ""))
                name = params.get("name", None)
                with _reg.OpenKey(getattr(_reg, hive), key) as k:
                    if name is None:
                        vals = []
                        i = 0
                        while True:
                            try:
                                n, v, t = _reg.EnumValue(k, i)
                                vals.append({"name": n, "value": str(v)[:200],
                                             "type": t})
                                i += 1
                            except OSError:
                                break
                        return {"ok": True, "hive": hive, "key": key, "values": vals}
                    v, t = _reg.QueryValueEx(k, name)
                    return {"ok": True, "hive": hive, "key": key, "name": name,
                            "value": str(v)[:500], "type": t}
            except Exception as e:
                return {"ok": False, "reason": "registry_read_error", "error": str(e)}

        def registry_write(self, params, device=None):
            if not _IS_WIN:
                return {"ok": False, "reason": "unsupported_platform"}
            return {"ok": False, "reason": "registry_write_denied",
                    "message": "注册表写入默认禁止（owner 确认后才执行）。"}

        def registry_delete(self, params, device=None):
            return {"ok": False, "reason": "hard_blocked",
                    "message": "注册表删除为硬禁止操作，任何角色不可执行。"}

    # ---------- 组装：挂到 local-host 设备 ----------
    _win_provider = WindowsOpsProvider()
    _WIN_OPS = [
        "system_status", "device_status", "list_processes", "process_info",
        "start_process", "stop_process",
        "list_windows", "focus_window", "window_snapshot",
        "mouse_move", "mouse_click", "mouse_scroll",
        "keyboard_type", "keyboard_hotkey",
        "screen_snapshot", "get_clipboard", "set_clipboard",
        "list_files", "read_file", "write_file", "delete_file",
        "run_powershell", "registry_read", "registry_write", "registry_delete",
    ]
    _local_host_dev = context.device_manager.get_device("local-host")
    for _op in _WIN_OPS:
        _fn = getattr(_win_provider, _op, None)
        if _fn is not None:
            _local_host_dev.capabilities.append(_op)
            _local_host_dev.op_risks[_op] = _WIN_OP_RISKS.get(_op, RiskLevel.MEDIUM)
            _local_host_dev.local_handlers[_op] = _fn
    _local_host_dev.forbidden_ops |= _WIN_FORBIDDEN_AGENT
    # 操作级硬禁止：registry_delete 任何角色不可执行
    # （命令级危险模式由 provider 内 _WIN_HARD_BLOCKED 二次拦截）
    _local_host_dev.hard_blocked_ops = getattr(
        _local_host_dev, "hard_blocked_ops", set()) | {"registry_delete"}
    _local_host_dev._dirty = True
    logger.info("[V14] Windows 现实交互能力已接入 local-host：%d 项操作"
                % len(_WIN_OPS))

    @register_command("private", ["Windows操作状态", "本地操作状态"], perm_required=2,
                      owner_only=True)
    def _cmd_win_status(msg):
        return ("【Windows 现实交互】\n"
                "平台: %s\n受限目录: %s\nPS 白名单: %d 条\n"
                "危险硬禁止规则: %d 条" % (
                    _sys.platform,
                    "、".join(_WIN_ALLOWED_DIRS) or "无",
                    len(_WIN_PS_WHITELIST),
                    len(_WIN_HARD_BLOCKED)))
