#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""命令注册表（从 src_b_data_defense.py 迁移）。"""

_COMMAND_REGISTRY: dict = {}


def register_command(scope, aliases, perm_required=0, admin_only=False, owner_only=False):
    """注册命令装饰器。"""
    def decorator(func):
        for alias in aliases:
            if scope not in _COMMAND_REGISTRY:
                _COMMAND_REGISTRY[scope] = {}
            _COMMAND_REGISTRY[scope][alias] = {
                "func": func,
                "perm_required": perm_required,
                "admin_only": admin_only,
                "owner_only": owner_only,
                "aliases": aliases,
            }
        return func
    return decorator


def get_command(scope, command_name):
    if scope in _COMMAND_REGISTRY:
        return _COMMAND_REGISTRY[scope].get(command_name)
    return None


def list_commands(scope, perm=0):
    if scope not in _COMMAND_REGISTRY:
        return []
    result = []
    for cmd, info in _COMMAND_REGISTRY[scope].items():
        if info["perm_required"] <= perm:
            result.append(cmd)
    return sorted(result)
