#!/usr/bin/env python3
"""Test a native SDK consumer using an isolated Go module zip and vendored copy.

Only --lib-dir is supplied externally. No repository include paths, replace
workspace, network proxy or existing Go build/module cache can satisfy the test.
"""
import argparse
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import zipfile

MODULE = "github.com/ashione/VelaGraph/sdk/go"
VERSION = "v0.0.0-native-test"
DOWNSTREAM_TEST = '''package downstream

import (
    "context"
    "math"
    "testing"
    native "github.com/ashione/VelaGraph/sdk/go/native"
)

func TestNativeModuleDistribution(t *testing.T) {
    before, err := native.Stats()
    if err != nil { t.Fatal(err) }
    store, err := native.OpenMemory(context.Background(), native.MemoryOptions{
        Identity: "downstream", Vertices: []native.Vertex{{ID: math.MaxUint64}},
    })
    if err != nil { t.Fatal(err) }
    defer store.Close()
    view, err := store.QueryView(context.Background(), []native.Query{{Text: "g.V()"}}, native.QueryOptions{})
    if err != nil { t.Fatal(err) }
    defer view.Close()
    if err := store.Close(); err != nil { t.Fatal(err) }
    row, err := view.Row(0, 0)
    if err != nil { t.Fatal(err) }
    id, err := row.ID()
    if err != nil || id != math.MaxUint64 { t.Fatalf("id=%d err=%v", id, err) }
    if err := view.Close(); err != nil { t.Fatal(err) }
    after, err := native.Stats()
    if err != nil || before != after { t.Fatalf("before=%+v after=%+v err=%v", before, after, err) }
}
'''


def create_proxy(root, proxy):
    """Package the actual SDK subtree, without its repository parents."""
    escaped = "".join("!" + c.lower() if c.isupper() else c for c in MODULE)
    versions = proxy / escaped / "@v"
    versions.mkdir(parents=True)
    sdk = root / "sdk/go"
    (versions / (VERSION + ".mod")).write_bytes((sdk / "go.mod").read_bytes())
    (versions / (VERSION + ".info")).write_text(json.dumps({
        "Version": VERSION, "Time": "2026-01-01T00:00:00Z",
    }))
    (versions / "list").write_text(VERSION + "\n")
    # Use tracked source plus the canonical SDK header path, which can be new
    # during a local red/green cycle. Never archive local binaries or reports.
    paths = subprocess.check_output(
        ["git", "ls-files", "-z", "--", "sdk/go"], cwd=root,
    ).decode().split("\0")
    paths = {root / p for p in paths if p}
    header = sdk / "native/velagraph.h"
    if header.exists():
        paths.add(header)
    with zipfile.ZipFile(versions / (VERSION + ".zip"), "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(paths):
            if path.is_file() and not path.is_symlink():
                archive.write(path, MODULE + "@" + VERSION + "/" + path.relative_to(sdk).as_posix())


def run(command, cwd, env, allow_failure=False):
    result = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True)
    if result.returncode:
        print(result.stdout + result.stderr, file=sys.stderr)
        if not allow_failure:
            raise subprocess.CalledProcessError(result.returncode, command)
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lib-dir", required=True)
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parents[1]
    lib = pathlib.Path(args.lib_dir).resolve()
    canonical = root / "include/velagraph.h"
    bundled = root / "sdk/go/native/velagraph.h"
    if not bundled.is_file() or bundled.read_bytes() != canonical.read_bytes():
        raise SystemExit("SDK header differs from canonical ABI: copy include/velagraph.h to sdk/go/native/velagraph.h")
    with tempfile.TemporaryDirectory(prefix="velagraph-downstream-") as directory:
        temp = pathlib.Path(directory)
        proxy = temp / "proxy"
        create_proxy(root, proxy)
        consumer = temp / "consumer"
        consumer.mkdir()
        (consumer / "go.mod").write_text(
            "module example.com/native-consumer\n\ngo 1.23.0\n\nrequire " + MODULE + " " + VERSION + "\n"
        )
        (consumer / "native_test.go").write_text(DOWNSTREAM_TEST)
        env = os.environ.copy()
        for name in ("CPATH", "C_INCLUDE_PATH", "CPLUS_INCLUDE_PATH", "OBJC_INCLUDE_PATH",
                     "CFLAGS", "CPPFLAGS", "CGO_CFLAGS", "CGO_CPPFLAGS", "CGO_CXXFLAGS"):
            env[name] = ""
        env.update({
            "GOWORK": "off", "GOENV": "off", "GOFLAGS": "", "GOTOOLCHAIN": "local",
            "GOPROXY": proxy.as_uri(), "GOSUMDB": "off", "GOPRIVATE": "",
            "GONOPROXY": "", "GONOSUMDB": "", "CGO_ENABLED": "1",
            "GOMODCACHE": str(temp / "module-cache"), "GOCACHE": str(temp / "build-cache"),
            "CGO_LDFLAGS": "-L" + str(lib),
        })
        loader = "DYLD_LIBRARY_PATH" if sys.platform == "darwin" else "LD_LIBRARY_PATH"
        env[loader] = str(lib)
        run(["go", "mod", "tidy"], consumer, env)
        module_ok = run(["go", "test", "-mod=readonly", "-tags", "velagraph_native",
                         "-count=1", "./..."], consumer, env, allow_failure=True)
        run(["go", "mod", "vendor"], consumer, env)
        # Vendor must stand on its own even without a module cache/proxy fallback.
        env["GOMODCACHE"] = str(temp / "empty-module-cache")
        env["GOPROXY"] = "off"
        vendor_ok = run(["go", "test", "-mod=vendor", "-tags", "velagraph_native",
                         "-count=1", "./..."], consumer, env, allow_failure=True)
        print("isolated module:", "PASS" if module_ok else "FAIL", flush=True)
        print("isolated vendor:", "PASS" if vendor_ok else "FAIL", flush=True)
        if not module_ok or not vendor_ok:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
