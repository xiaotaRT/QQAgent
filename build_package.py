#!/usr/bin/env python3
"""qqagent 包 → 单文件内联打包器。

用法：
    python build_package.py [输出路径]

逻辑：
1. 扫描 qqagent/ 包所有 .py 模块
2. 解析 `from qqagent.xxx import ...` / `import qqagent.xxx` 构建依赖图
3. 拓扑排序（被依赖的模块先内联）
4. 读取模块内容，删除包内 import 语句（内联后名字已全局可见）
5. 按序拼接 + 入口，输出单文件

设计目标：包直接 `python -m qqagent.main` 与单文件 `python QQAgent_v23.py` 行为一致。
"""
from __future__ import annotations

import ast
import io
import os
import re
import sys
from collections import deque

HERE = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.join(HERE, "qqagent")
DEFAULT_OUT = os.path.join(HERE, "QQAgent_v23.py")

# 匹配包内 import（单行；多行 from ... import (a, b) 暂不支持，骨架阶段够用）
FROM_IMPORT_RE = re.compile(
    r"^\s*from\s+(qqagent(?:\.\w+)*)\s+import\s+(.+?)\s*$"
)
IMPORT_RE = re.compile(r"^\s*import\s+(qqagent(?:\.\w+)*)\s*$")


def scan_modules() -> list[tuple[str, str]]:
    """返回 [(模块全限定名, 文件绝对路径), ...]。"""
    mods: list[tuple[str, str]] = []
    for root, _dirs, files in os.walk(PKG_DIR):
        for fname in files:
            if not fname.endswith(".py"):
                continue
            path = os.path.join(root, fname)
            rel = os.path.relpath(path, HERE)
            mod = rel.replace(os.sep, ".")[:-3]  # qqagent.core.context
            # __init__.py 的模块名是包名本身：qqagent/core/__init__.py -> qqagent.core
            if mod.endswith(".__init__"):
                mod = mod[: -len(".__init__")]
            mods.append((mod, path))
    return mods


def parse_deps(content: str) -> set[str]:
    """从模块内容解析它依赖的 qqagent 子模块（用 ast，避免 docstring/注释误判）。"""
    deps: set[str] = set()
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return deps
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("qqagent"):
                    deps.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module.startswith("qqagent"):
                deps.add(node.module)
    return deps


def topo_sort(
    mods: list[tuple[str, str]],
) -> tuple[list[str], dict[str, str]]:
    """拓扑排序。返回 (顺序列表, {模块名: 内容})。

    规则：A 依赖 B → B 排在 A 前面。
    检测循环依赖并报错。
    """
    content_map: dict[str, str] = {}
    graph: dict[str, set[str]] = {}  # mod -> set(它依赖的模块)
    for mod, path in mods:
        with io.open(path, encoding="utf-8") as f:
            c = f.read()
        content_map[mod] = c
        graph[mod] = set()

    for mod, c in content_map.items():
        for d in parse_deps(c):
            if d in graph and d != mod:
                graph[mod].add(d)

    # Kahn 算法：入度 = 该模块依赖的数量
    in_deg = {m: len(deps) for m, deps in graph.items()}
    queue: deque[str] = deque(m for m, d in in_deg.items() if d == 0)
    order: list[str] = []
    while queue:
        m = queue.popleft()
        order.append(m)
        for other, deps in graph.items():
            if m in deps:
                in_deg[other] -= 1
                if in_deg[other] == 0:
                    queue.append(other)

    if len(order) != len(graph):
        remaining = set(graph) - set(order)
        raise RuntimeError(f"检测到循环依赖，涉及模块: {sorted(remaining)}")
    return order, content_map


def strip_package_imports(content: str) -> str:
    """删除包内 import 语句（含多行括号形式），用 ast 精确定位行范围。

    保留标准库/第三方 import（不删）。
    """
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return content

    lines = content.splitlines(keepends=True)
    remove_ranges: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name.startswith("qqagent") for alias in node.names):
                remove_ranges.append((node.lineno, node.end_lineno or node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module.startswith("qqagent"):
                remove_ranges.append((node.lineno, node.end_lineno or node.lineno))

    if not remove_ranges:
        return content

    # 合并重叠/相邻范围
    remove_ranges.sort()
    merged: list[tuple[int, int]] = []
    for start, end in remove_ranges:
        if merged and start <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    remove_set: set[int] = set()
    for start, end in merged:
        for i in range(start, end + 1):
            remove_set.add(i)

    return "".join(line for i, line in enumerate(lines, 1) if i not in remove_set)


FUTURE_RE = re.compile(r"^\s*from\s+__future__\s+import\s+.+?\s*$")


def collect_future_imports(contents: dict[str, str]) -> list[str]:
    """收集所有模块的 from __future__ import，去重保序。"""
    seen: set[str] = set()
    result: list[str] = []
    for c in contents.values():
        for line in c.splitlines():
            if FUTURE_RE.match(line):
                stripped = line.strip()
                if stripped not in seen:
                    seen.add(stripped)
                    result.append(stripped)
    return result


def strip_future_imports(content: str) -> str:
    """从模块内容中删除 from __future__ import（已统一提到文件头）。"""
    return "".join(
        line for line in content.splitlines(keepends=True)
        if not FUTURE_RE.match(line)
    )


def build(output_path: str = DEFAULT_OUT) -> str:
    """执行内联打包，返回输出文件路径。"""
    mods = scan_modules()
    if not mods:
        raise RuntimeError(f"未在 {PKG_DIR} 找到任何 .py 模块")

    order, content_map = topo_sort(mods)
    future_imports = collect_future_imports(content_map)

    parts: list[str] = []
    parts.append("#!/usr/bin/env python3\n")
    parts.append("# -*- coding: utf-8 -*-\n")
    parts.append("# QQAgent (modular) — 由 build_package.py 内联打包生成\n")
    parts.append("# 请勿手动编辑；修改请改 qqagent/ 包源码后重新构建。\n")
    parts.append(f"# 模块数: {len(order)}\n\n")
    for fi in future_imports:
        parts.append(fi + "\n")
    if future_imports:
        parts.append("\n")

    for mod in order:
        c = strip_package_imports(strip_future_imports(content_map[mod]))
        parts.append(f"# ===== module: {mod} =====\n")
        parts.append(c.rstrip() + "\n\n")

    parts.append('if __name__ == "__main__":\n')
    parts.append("    import sys\n")
    parts.append("    sys.exit(main())\n")

    out = "".join(parts)
    with io.open(output_path, "w", encoding="utf-8", newline="") as f:
        f.write(out)

    line_count = out.count("\n")
    print(f"[build_package] 已生成 {output_path}")
    print(f"  模块数: {len(order)}  行数: {line_count}")
    print(f"  拓扑顺序: {' -> '.join(order)}")
    return output_path


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUT
    build(out)
