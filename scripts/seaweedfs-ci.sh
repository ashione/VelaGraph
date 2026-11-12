#!/usr/bin/env bash
# Real S3-compatible integration service, not cloud-provider certification.
set -euo pipefail
# Official Apache-2.0 SeaweedFS release asset digest, verified on 2026-11-12:
# https://api.github.com/repos/seaweedfs/seaweedfs/releases/tags/4.53
readonly VERSION=4.53
readonly SHA256=d1a7dec9f48d8b33895a0ea8951971defc2dc5312906dcc76fa5a5f74a6ee90d
[[ $(uname -s) == Linux && $(uname -m) == x86_64 ]] || { echo 'Linux x86_64 required' >&2; exit 1; }
[[ $# -gt 0 ]] || { echo "Usage: $0 command [args...]" >&2; exit 1; }
state=$(mktemp -d "${TMPDIR:-/tmp}/velagraph-s3.XXXXXXXX")
log_dir=${SEAWEEDFS_LOG_DIR:-$state/logs}
mkdir -p "$log_dir" "$state/data"
pid=''
cleanup() {
  rc=$?
  trap - EXIT INT TERM
  if [[ -n $pid ]]; then
    kill "$pid" 2>/dev/null || true
    for _ in {1..50}; do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  if [[ $rc != 0 ]]; then tail -n 120 "$log_dir/seaweedfs.log" 2>/dev/null || true; fi
  rm -rf -- "$state"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
curl --fail --location --retry 3 --connect-timeout 15 --max-time 180 \
  "https://github.com/seaweedfs/seaweedfs/releases/download/$VERSION/linux_amd64.tar.gz" -o "$state/weed.tar.gz"
printf '%s  %s\n' "$SHA256" "$state/weed.tar.gz" | sha256sum --check --strict
# Extract only the verified executable.
tar -xzf "$state/weed.tar.gz" -C "$state" weed
# Never inherit user cloud credentials. Service and bucket are disposable.
export AWS_ACCESS_KEY_ID=velagraph-ci-only AWS_SECRET_ACCESS_KEY=velagraph-ci-only-secret
export AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
export NO_PROXY=127.0.0.1,localhost
export no_proxy="$NO_PROXY"
unset AWS_SESSION_TOKEN AWS_PROFILE AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN
export VELAGRAPH_TEST_S3_ENDPOINT=http://127.0.0.1:8333 VELAGRAPH_TEST_S3_BUCKET=velagraph-ci
"$state/weed" mini -dir="$state/data" -ip=127.0.0.1 -ip.bind=127.0.0.1 \
  -bucket="$VELAGRAPH_TEST_S3_BUCKET" -s3.autoCreateBucket=false \
  -admin.ui=false -webdav=false -s3.port.iceberg=0 -s3.port.lance=0 \
  >"$log_dir/seaweedfs.log" 2>&1 &
pid=$!
ready=false
for _ in {1..90}; do
  kill -0 "$pid" 2>/dev/null || { echo 'SeaweedFS exited during startup' >&2; exit 1; }
  if curl --silent --fail --max-time 2 --head --aws-sigv4 "aws:amz:$AWS_DEFAULT_REGION:s3" \
    --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
    "$VELAGRAPH_TEST_S3_ENDPOINT/$VELAGRAPH_TEST_S3_BUCKET" >/dev/null; then ready=true; break; fi
  sleep 1
done
$ready || { echo 'SeaweedFS bucket readiness timed out' >&2; exit 1; }
echo "SeaweedFS $VERSION ready: ephemeral loopback S3 compatibility test"
"$@"
