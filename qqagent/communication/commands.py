#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""设备管理命令（从 src_m_reality.py 迁移）。"""
import os, re, json, time, logging
from qqagent.core import CONFIG, logger, context
from qqagent.core.commands import register_command

@register_command("private", ["设备列表"], perm_required=2, owner_only=True)
def _cmd_device_list(msg):
    rows = context.device_manager.list_devices()
    lines = ["【设备列表】共 %d 台" % len(rows)]
    for r in rows:
        lines.append("· %s [%s] %s %s %s" % (
            r["device_id"], r["type"], r["name"],
            "在线" if r["online"] else "离线",
            "启用" if r["enabled"] else "禁用"))
    return "\n".join(lines)


@register_command("private", ["设备详情"], perm_required=2, owner_only=True)
def _cmd_device_detail(msg):
    parts = (msg or "").split()
    did = parts[-1] if len(parts) > 1 else "local-host"
    dev = context.device_manager.get_device(did)
    if dev is None:
        return "未找到设备：%s" % did
    return ("【设备详情】%s（%s）\n"
            "类型: %s\n在线: %s | 启用: %s | Agent自主: %s\n"
            "能力: %s\n黑名单: %s\n硬禁止: %s\n行驶状态: %s" % (
                dev.name, dev.device_id, dev.device_type,
                "是" if dev.online else "否", "是" if dev.enabled else "否",
                "禁止" if dev.agent_deny_all else "允许",
                "、".join(sorted(dev.capabilities)) or "无",
                "、".join(sorted(dev.forbidden_ops)) or "无",
                "、".join(sorted(dev.hard_blocked_ops)) or "无",
                dev.driving_state))


@register_command("private", ["设备添加"], perm_required=2, owner_only=True)
def _cmd_device_add(msg):
    parts = (msg or "").split()
    if len(parts) < 2:
        return "用法：设备添加 <device_id> <类型:windows/android/vision/homeassistant> [名称]"
    did, dtype = parts[1], parts[2]
    name = " ".join(parts[3:]) or did
    if dtype not in ("windows", "android", "vision", "homeassistant"):
        return "不支持的设备类型：%s" % dtype
    if context.device_manager.get_device(did) is not None:
        return "设备已存在：%s" % did
    dev = RikuDevice(did, dtype, name=name)
    context.device_manager.register_device(dev)
    return "设备已添加：%s（%s）" % (name, did)


@register_command("private", ["紧急停止", "紧急冻结"], perm_required=2, owner_only=True)
def _cmd_emergency_stop(msg):
    context.tool_gateway.emergency_stop("用户命令")
    return "已紧急停止：所有设备操作被冻结。使用「恢复设备控制」解冻。"


@register_command("private", ["恢复设备控制", "解除紧急停止"], perm_required=2, owner_only=True)
def _cmd_resume(msg):
    context.tool_gateway.resume("用户命令")
    return "已恢复：设备控制正常。"


@register_command("private", ["待确认操作"], perm_required=2, owner_only=True)
def _cmd_pending(msg):
    tickets = context.tool_gateway.list_pending_tickets()
    lines = ["【待确认操作】%d 条" % len(tickets)]
    for t in tickets:
        lines.append("· %s | %s | %s（%s）| %s | %s" % (
            t["id"], t["operation"], t["device_name"], t["device_id"],
            t["risk_name"], t["created_at"]))
    return "\n".join(lines)


@register_command("private", ["确认操作"], perm_required=2, owner_only=True)
def _cmd_approve(msg):
    parts = (msg or "").split()
    if len(parts) < 2:
        return "用法：确认操作 <ticket_id>"
    r = context.tool_gateway.resolve_ticket(parts[1], True, "owner")
    return "确认成功：%s" % parts[1] if r.get("ok") else "确认失败：%s" % r.get("reason")


@register_command("private", ["拒绝操作"], perm_required=2, owner_only=True)
def _cmd_reject(msg):
    parts = (msg or "").split()
    if len(parts) < 2:
        return "用法：拒绝操作 <ticket_id>"
    r = context.tool_gateway.resolve_ticket(parts[1], False, "owner")
    return "已拒绝：%s" % parts[1] if r.get("ok") else "拒绝失败：%s" % r.get("reason")


@register_command("private", ["设备操作日志"], perm_required=2, owner_only=True)
def _cmd_audit(msg):
    audit = context.tool_gateway.get_audit(20)
    lines = ["【设备操作日志】最近 %d 条" % len(audit)]
    for a in audit:
        lines.append("· %s %s→%s %s | %s" % (
            a.get("ts"), a.get("actor"), a.get("device_id"), a.get("operation"),
            a.get("status")))
    return "\n".join(lines)


@register_command("private", ["能力层状态"], perm_required=2, owner_only=True)
def _cmd_gateway_status(msg):
    devs = context.device_manager.list_devices()
    return ("【现实世界能力层】\n"
            "设备: %d 台 | 在线: %d | 紧急停止: %s\n"
            "审计: %d 条 | 待确认: %d 条\n"
            "设备: %s" % (
                len(devs), sum(1 for d in devs if d["online"]),
                "已启用" if context.tool_gateway.is_emergency_stopped() else "未启用",
                len(context.tool_gateway._log),
                len(context.tool_gateway.list_pending_tickets()),
                "、".join(d["device_id"] for d in devs) or "无"))
