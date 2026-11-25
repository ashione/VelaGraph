# Direct RocksDB FFI baseline

Use this suite when comparing changes to on-disk temporal queries, the packed native ABI, or Go result decoding. The same Go executable loads each release library and calls `OpenTemporal` on a fresh on-disk fixture. It closes and reopens the writer, verifies the backend and stored version, pins graph version and valid time, and checks every timed result.

| Case | Work per timed Go call | Iterations per run |
| --- | --- | ---: |
| Warm / Newest or Previous | Read the selected integer from 512 overlapping revisions after an explicit cache warm-up. | 10,000 each |
| FirstRead / Newest or Previous | Perform the same read on a newly opened handle. Open and close are outside the timer. The OS file cache can remain warm. | 64 each |
| Tree / Parents16 | Traverse one incoming edge from each of 16 leaves of a generated six-way tree. | 128 |
| Tree / Subtree43 | Fetch the three levels of a 43-vertex subtree in one packed batch. | 128 |
| Tree / Whole259 | Fetch the four levels of the 259-vertex tree in one packed batch. | 128 |

Tree cases warm the exact query before timing. The decoder and checks for row counts, vertex membership and duplicate rows at each level are included. Go allocation counters cover the Go heap. The single-client read measurements exclude writes, network transport and concurrent request queuing.

## Reproduce a comparison

Run these commands in a checkout containing this guide on Linux or macOS, with Git, Rust, Go, Python 3, a C compiler and libclang available. Set `BASE` and `CANDIDATE` to the Git revisions under comparison. Both must support the measured ABI. For a new comparison, keep the benchmark source and Go toolchain identical across the two libraries.

```sh
set -eu
root=$(git rev-parse --show-toplevel)
out=$(mktemp -d)
base_sha=$(git rev-parse "${BASE}^{commit}")
candidate_sha=$(git rev-parse "${CANDIDATE}^{commit}")
git worktree add --detach "$out/base" "$base_sha"
git worktree add --detach "$out/candidate" "$candidate_sha"

for arm in base candidate; do
  (cd "$out/$arm" && CARGO_TARGET_DIR="$out/target-$arm" \
    cargo build --locked --release -p velagraph-ffi)
done

case "$(uname -s)" in
  Darwin) lib=libvelagraph_ffi.dylib ;;
  Linux) lib=libvelagraph_ffi.so ;;
  *) exit 1 ;;
esac
(cd "$root/sdk/go" && CGO_ENABLED=1 CGO_LDFLAGS="-L$out/target-candidate/release" \
  go test -c -tags velagraph_native -o "$out/native-bench" ./native)

python3 "$root/scripts/bench_native_temporal.py" \
  --binary "$out/native-bench" \
  --base "$out/target-base/release/$lib" --base-revision "$base_sha" \
  --candidate "$out/target-candidate/release/$lib" --candidate-revision "$candidate_sha" \
  --output "$out/results.json"

git worktree remove "$out/base"
git worktree remove "$out/candidate"
echo "$out/results.json"
```

Run on an otherwise quiet host. The driver alternates library order for ten pairs, fixes `GOMAXPROCS=4`, checks the dynamic loader's selected library on every invocation, and records all per-run means and artifact hashes. Use the median of the paired percentage changes and the count of faster pairs to judge repeatability. The reported timings are means per invocation of the Go benchmark; request-level latency percentiles require a separate measurement.

## Reference measurement

Measured on an Apple M2 Pro (macOS 14.7.8 arm64), with Rust 1.95.0, Go 1.23.2, and the same prebuilt static RocksDB archive for both release builds. The base is `570c3e3` and the candidate is `7098089`. Each timing below is the median of ten run means in microseconds per query or packed batch. The change column is computed separately on paired runs.

| Case | Base (µs) | Candidate (µs) | Median paired change | Candidate faster |
| --- | ---: | ---: | ---: | ---: |
| Warm / Newest | 4.99 | 4.08 | -18.2% | 10/10 |
| Warm / Previous | 4.99 | 4.09 | -18.8% | 10/10 |
| FirstRead / Newest | 46.24 | 47.18 | +5.0% | 2/10 |
| FirstRead / Previous | 47.73 | 48.02 | -0.1% | 6/10 |
| Tree / Parents16 | 51.93 | 54.25 | +1.3% | 5/10 |
| Tree / Subtree43 | 101.37 | 101.68 | -1.0% | 6/10 |
| Tree / Whole259 | 850.46 | 860.68 | +1.7% | 3/10 |

Use the individual pairs when investigating first-read or traversal variance. The native CI workflow runs each generated tree case once on Linux and macOS to catch fixture and ABI failures. The release comparison runs separately from CI. [Raw timings, Go allocation counts and artifact hashes](direct-ffi-7098089.json)
