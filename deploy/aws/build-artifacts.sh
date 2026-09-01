#!/usr/bin/env bash
#
# build-artifacts.sh - build a self-contained justrade runtime tarball for the
# AWS benchmark deployment: a pinned Eclipse Temurin 21 JRE plus the five
# application distributions (launcher, read, gateway, bench, xcore-bench) and
# the bin/ entrypoints. The Ansible deploy playbook pushes the tarball to each
# instance over SSH; nothing is uploaded to S3.
#
# Usage:
#   ./deploy/aws/build-artifacts.sh    # build ./deploy/aws/justrade-runtime.tgz
#
# Env overrides:
#   JRE_URL   Temurin 21 JRE tarball URL (default: Adoptium "latest" for
#             linux/x64; set a pinned release URL for reproducibility).
#
# The tarball layout mirrors the Docker image so the same entrypoint classpaths
# work unchanged:
#   jre/          Java 21 runtime
#   launcher/     launcher installDist      (ClusterLauncher)
#   read/         read installDist          (ReadServiceLauncher)
#   gateway/      gateway installDist       (GatewayLauncher)
#   bench/        bench installDist         (ExternalLoadRunner / ReadVerifyRunner)
#   xcore-bench/  xcore-bench installDist   (XcoreWorkloadRunner)
#   bin/          node.sh read.sh gateway.sh load.sh verify.sh xcore-load.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEPLOY_DIR="$ROOT/deploy/aws"
OUT="$DEPLOY_DIR/justrade-runtime.tgz"
JRE_URL="${JRE_URL:-https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jre/hotspot/normal/eclipse}"

echo "[build-artifacts] building application distributions ..."
"$ROOT/gradlew" --quiet \
    :launcher:installDist \
    :read:installDist \
    :gateway:installDist \
    :bench:installDist \
    :xcore-bench:installDist

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

echo "[build-artifacts] downloading Temurin 21 JRE ..."
mkdir -p "$STAGE/jre"
curl -fsSL "$JRE_URL" -o "$STAGE/jre.tgz"
tar -xzf "$STAGE/jre.tgz" -C "$STAGE/jre" --strip-components=1
rm -f "$STAGE/jre.tgz"

echo "[build-artifacts] staging distributions ..."
cp -R "$ROOT/launcher/build/install/launcher" "$STAGE/launcher"
cp -R "$ROOT/read/build/install/read" "$STAGE/read"
cp -R "$ROOT/gateway/build/install/gateway" "$STAGE/gateway"
cp -R "$ROOT/bench/build/install/bench" "$STAGE/bench"
cp -R "$ROOT/xcore-bench/build/install/xcore-bench" "$STAGE/xcore-bench"
cp -R "$DEPLOY_DIR/bin" "$STAGE/bin"
chmod +x "$STAGE/bin/"*.sh

echo "[build-artifacts] packing $OUT ..."
tar -czf "$OUT" -C "$STAGE" jre launcher read gateway bench xcore-bench bin

echo "[build-artifacts] done: $OUT"
