#!/usr/bin/env python3
"""Compare release RocksDB libraries with the same Go native benchmark executable."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys


PHASES = {
    "Warm": ("BenchmarkNativeTemporalRevisions/Warm", {"Newest", "Previous"}, 10000),
    "FirstRead": ("BenchmarkNativeTemporalRevisions/FirstRead", {"Newest", "Previous"}, 64),
    "Tree": ("BenchmarkNativeTemporalTree", {"Parents16", "Subtree43", "Whole259"}, 128),
}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def measure(binary, library, phase):
    env = dict(os.environ, GOMAXPROCS="4")
    if sys.platform == "darwin":
        env.update(DYLD_LIBRARY_PATH=str(library.parent), DYLD_PRINT_LIBRARIES="1")
        loaded = r"^dyld\[\d+\]: <[^>]+> (.+/libvelagraph_ffi\.dylib)$"
    elif sys.platform == "linux":
        paths = [str(library.parent)]
        if env.get("LD_LIBRARY_PATH"):
            paths.append(env["LD_LIBRARY_PATH"])
        env.update(LD_LIBRARY_PATH=os.pathsep.join(paths), LD_DEBUG="libs")
        loaded = r"^\s*\d+:\s+calling init: (.+/libvelagraph_ffi\.so)\s*$"
    else:
        raise ValueError("dynamic library verification requires Linux or macOS")
    prefix, expected, iterations = PHASES[phase]
    selector = "/".join("^" + re.escape(part) + "$" for part in prefix.split("/")) + "/"
    proc = subprocess.run(
        [str(binary), "-test.run=^$", f"-test.bench={selector}",
         f"-test.benchtime={iterations}x", "-test.benchmem"],
        env=env, capture_output=True, text=True, timeout=180,
    )
    if proc.returncode:
        raise RuntimeError(proc.stdout[-4000:] + "\n" + proc.stderr[-1000:])
    if {Path(x).resolve() for x in re.findall(loaded, proc.stderr, re.MULTILINE)} != {library}:
        raise RuntimeError("could not verify the loaded native library")
    rows = {}
    pattern = (r"^" + re.escape(prefix) + r"/(\w+)-4\s+(\d+)\s+([\d.]+) ns/op"
               r"\s+(\d+) B/op\s+(\d+) allocs/op\s*$")
    for name, n, ns, mem, allocs in re.findall(pattern, proc.stdout, re.MULTILINE):
        if int(n) != iterations or name in rows:
            raise ValueError("unexpected sample or iteration count")
        rows[name] = dict(ns_op=float(ns), go_bytes_op=int(mem), go_allocs_op=int(allocs))
    if set(rows) != expected:
        raise ValueError("incomplete benchmark output")
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    for label in ("base", "candidate"):
        parser.add_argument("--" + label, type=Path, required=True)
        parser.add_argument("--" + label + "-revision", required=True)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.rounds < 2:
        parser.error("at least two rounds are required for alternating order")
    libs = {key: getattr(args, key).resolve(strict=True) for key in ("base", "candidate")}
    binary = args.binary.resolve(strict=True)
    hashes = {key: sha256(path) for key, path in libs.items()}
    if len(set(hashes.values())) != 2:
        parser.error("expected two distinct library artifacts")
    samples = {"base": {}, "candidate": {}}
    for index in range(args.rounds):
        order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
        for key in order:
            for phase in PHASES:
                for name, row in measure(binary, libs[key], phase).items():
                    samples[key].setdefault(f"{phase}/{name}", []).append(row)
        print(f"Completed pair {index + 1}/{args.rounds}", flush=True)
    summary = {}
    for name in samples["base"]:
        base, candidate = ([v["ns_op"] for v in samples[k][name]] for k in ("base", "candidate"))
        summary[name] = {
            "base_ns_op": statistics.median(base),
            "candidate_ns_op": statistics.median(candidate),
            "median_paired_change_pct": statistics.median(100 * (b / a - 1) for a, b in zip(base, candidate)),
            "candidate_faster_pairs": sum(b < a for a, b in zip(base, candidate)),
        }
    result = dict(base_revision=args.base_revision, candidate_revision=args.candidate_revision,
                  library_sha256=hashes, benchmark_sha256=sha256(binary), system=platform.system(),
                  arch=platform.machine(), rounds=args.rounds, gomaxprocs=4, iterations={k: v[2] for k, v in PHASES.items()},
                  summary=summary, samples=samples)
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
