#!/usr/bin/env python3
"""Explicit opt-in build/run; never imports or modifies frozen evidence binaries."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--baseline', type=Path, required=True)
parser.add_argument('--candidate', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--target-dir', type=Path, required=True, help='explicit shared warm Cargo target; coordinate exclusive use before running')
parser.add_argument('--journal', type=Path, help='explicit 149-terminal Journal JSON for candidate-only codec diagnostic')
parser.add_argument('--execute', action='store_true', help='required to build and run')
args = parser.parse_args()
if not args.execute:
    parser.error('No action taken. Pass --execute to authorize builds and sequential runs.')
args.output.mkdir(parents=True, exist_ok=False)
source = Path(__file__).with_name('prefix_comparison.rs').resolve()
env = os.environ.copy()
env.update(CARGO_BUILD_JOBS='1', CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0')
# Shared warm cache is explicitly selected by the caller under root coordination.
# Every completed main ELF is frozen below before another build starts.
env['CARGO_TARGET_DIR'] = str(args.target_dir.resolve())
manifest = {'target_dir': env['CARGO_TARGET_DIR'], 'harness_sha256': hashlib.sha256(source.read_bytes()).hexdigest(), 'runs': []}
for label, root in [('baseline', args.baseline.resolve()), ('candidate', args.candidate.resolve())]:
    stage = args.output / (label + '-source')
    stage.mkdir()
    names = subprocess.check_output(['git', '-C', str(root), 'ls-files', '-z', '--cached', '--others', '--exclude-standard']).split(b'\0')
    for raw in names:
        if not raw:
            continue
        rel = Path(os.fsdecode(raw))
        if rel.parts[0] in ('measurement-evidence', '.git', 'target'):
            continue
        src = root / rel
        if not src.is_file():
            continue
        dst = stage / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
    dst = stage / 'crates/replicated-graph/tests/prefix_comparison.rs'
    shutil.copyfile(source, dst)
    if label == 'candidate' and args.journal:
        example = stage / 'crates/replicated-graph/examples/proof_sizes.rs'
        example.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source.with_name('proof_sizes.rs'), example)
    inventory = {str(p.relative_to(stage)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in sorted(stage.rglob('*')) if p.is_file()}
    inventory_bytes = json.dumps(inventory, sort_keys=True, indent=2).encode()
    (args.output / (label + '-source-inventory.json')).write_bytes(inventory_bytes)
    status = subprocess.check_output(['git', '-C', str(root), 'status', '--porcelain=v1', '--untracked-files=all'])
    diff = subprocess.check_output(['git', '-C', str(root), 'diff', '--binary', 'HEAD', '--'])
    (args.output / (label + '-original-status.txt')).write_bytes(status)
    (args.output / (label + '-original-diff.patch')).write_bytes(diff)
    run_record = {
        'label': label, 'source': str(root), 'staged_source': str(stage.resolve()),
        'original_head_context_only_not_built_commit': subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD'], text=True).strip(),
        'original_dirty': bool(status), 'original_status_sha256': hashlib.sha256(status).hexdigest(),
        'original_diff_sha256': hashlib.sha256(diff).hexdigest(),
        'staged_source_inventory_sha256': hashlib.sha256(inventory_bytes).hexdigest(),
        'build_config': {k: v for k, v in env.items() if k in ('CARGO_HOME', 'CARGO_TARGET_DIR', 'CARGO_BUILD_JOBS', 'CARGO_INCREMENTAL', 'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG', 'RUSTUP_HOME', 'RUSTFLAGS', 'RUSTDOCFLAGS', 'ROCKSDB_LIB_DIR', 'ROCKSDB_STATIC', 'LIBCLANG_PATH', 'BINDGEN_EXTRA_CLANG_ARGS', 'PATH', 'CC', 'CXX', 'AR', 'CFLAGS', 'CXXFLAGS', 'LDFLAGS', 'LD_LIBRARY_PATH')},
    }
    manifest['runs'].append(run_record)
    command = ['cargo', 'test', '--locked', '-p', 'replicated-graph', '--test', 'prefix_comparison', '--no-run', '--message-format=json']
    run_record['build_command'] = command
    (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    with (args.output / (label + '-build.jsonl')).open('w') as log, (args.output / (label + '-build.stderr.log')).open('w') as err:
        subprocess.run(command, cwd=stage, env=env, stdout=log, stderr=err, check=True)
    executable = None
    for line in (args.output / (label + '-build.jsonl')).read_text().splitlines():
        message = json.loads(line)
        if message.get('reason') == 'compiler-artifact' and message.get('target', {}).get('name') == 'prefix_comparison':
            executable = message.get('executable') or executable
    if not executable:
        raise RuntimeError('No harness executable reported')
    # Retain binary before the next build can replace artifacts.
    retained = (args.output / (label + '-executable')).resolve()
    shutil.copy2(executable, retained)
    runenv = env | {'PREFIX_COMPARISON_LABEL': label, 'PREFIX_COMPARISON_OUTPUT': str((args.output / (label + '.json')).resolve())}
    run_command = [str(retained), '--ignored', '--exact', 'prefix_comparison', '--nocapture', '--test-threads=1']
    run_record.update(binary_sha256=hashlib.sha256(retained.read_bytes()).hexdigest(), run_command=run_command,
                      run_environment={k: runenv[k] for k in ('PREFIX_COMPARISON_LABEL', 'PREFIX_COMPARISON_OUTPUT')})
    (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    with (args.output / (label + '-run.log')).open('w') as log:
        subprocess.run(run_command, cwd=stage, env=runenv, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=1800)
    run_record['main_run_completed'] = True
    (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    if label == 'candidate' and args.journal:
        command = ['cargo','run','--locked','-p','replicated-graph','--example','proof_sizes','--',str(args.journal.resolve())]
        run_record['proof_command'] = command
        run_record['proof_journal_sha256'] = hashlib.sha256(args.journal.read_bytes()).hexdigest()
        (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
        with (args.output / 'proof-sizes.json').open('w') as log, (args.output / 'proof-sizes.stderr.log').open('w') as err:
            subprocess.run(command, cwd=stage, env=env, stdout=log, stderr=err, check=True)
        run_record['proof_run_completed'] = True
        (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
print(args.output.resolve())
