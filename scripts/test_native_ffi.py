#!/usr/bin/env python3
"""Build a C consumer, then check native cgo and portable Go SDK builds.

Build velagraph-ffi first and pass the target debug/release library directory.
"""
import argparse
import os
import pathlib
import subprocess
import sys
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lib-dir", required=True)
    parser.add_argument("--no-race", action="store_true")
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parents[1]
    lib = pathlib.Path(args.lib_dir).resolve()
    env = os.environ.copy()
    env["CGO_LDFLAGS"] = "-L" + str(lib)
    loader = "DYLD_LIBRARY_PATH" if sys.platform == "darwin" else "LD_LIBRARY_PATH"
    env[loader] = str(lib) + (os.pathsep + env[loader] if env.get(loader) else "")
    with tempfile.TemporaryDirectory(prefix="velagraph-ffi-c-") as tmp:
        executable = pathlib.Path(tmp) / "smoke"
        subprocess.run([
            os.environ.get("CC", "cc"), "-std=c11", "-Wall", "-Wextra", "-Werror",
            "-I" + str(root / "include"),
            str(root / "crates/velagraph-ffi/tests/smoke.c"),
            "-L" + str(lib), "-lvelagraph_ffi", "-o", str(executable),
        ], check=True, env=env)
        subprocess.run([str(executable)], check=True, env=env)
    command = ["go", "test", "-count=1", "-tags", "velagraph_native"]
    if not args.no_race:
        command.append("-race")
    subprocess.run(command + ["./native/..."], cwd=root / "sdk/go", check=True, env=env)
    portable = env.copy()
    portable["CGO_ENABLED"] = "0"
    subprocess.run(["go", "test", "./..."], cwd=root / "sdk/go", check=True, env=portable)


if __name__ == "__main__":
    main()
