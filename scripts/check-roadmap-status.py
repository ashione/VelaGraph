#!/usr/bin/env python3
"""Narrow #285 documentation regression guard, not a live GitHub status checker.

When the documented contracts actually change, update this guard with evidence.
This intentionally does not ban historical TCP/JSON or retained format records.
"""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent.parent
roadmap = (ROOT / "docs/ROADMAP.md").read_text()
current = (ROOT / "docs/CURRENT_STATUS.md").read_text()

for stale in (
    "The implementation candidate exposes the existing bounded job lifecycle",
    "Remaining graph library TCP listeners need retirement",
    "GraphService retirement remains separate; this slice does not complete #218.",
    "本地状态机快照/未来 RocksDB checkpoint",
    "当前 Raft 快照不等于已支持 RocksDB checkpoint 或对象存储备份。",
):
    assert stale not in roadmap, f"stale roadmap statement: {stale}"
assert "`ManagedQueryJobService` candidate" not in current

jobs = roadmap.split("### Typed managed job lifecycle (#268)", 1)[1].split("###", 1)[0]
assert "#271" in jobs and "#268 acceptance is complete" in jobs
assert "do not resume" in jobs and "HTTP job parity remains open" in jobs
checkpoint = roadmap.split("### D6a ", 1)[1].split("## 5.", 1)[0]
for required in ("#194", "checkpoint-object-store", "RESTORED_OFFLINE_ONLY", "#193", "#191", "temporal"):
    assert required in checkpoint, f"missing checkpoint boundary: {required}"

for path, text in ((ROOT / "docs/ROADMAP.md", roadmap), (ROOT / "docs/CURRENT_STATUS.md", current)):
    for link in re.findall(r"\]\(([^)]+)\)", text):
        target = link.split("#", 1)[0]
        if target and not re.match(r"[a-zA-Z]+://", target):
            assert (path.parent / target).is_file(), f"missing local link: {path.name}: {link}"
anchor = "d6a-远程-checkpoint-与灾难恢复基础备份已实现全局恢复待实现"
assert anchor in current
assert "### D6a 远程 checkpoint 与灾难恢复（基础备份已实现，全局恢复待实现）" in roadmap
print("PASS: #285 current-status regressions, checkpoint/job boundaries and local links")
