#!/usr/bin/env bash
#
# build-artifacts.sh - build a self-contained excoredum runtime tarball for the
# AWS benchmark deployment: a pinned Eclipse Temurin 21 JRE plus the four
# application distributions (launcher, read, gateway, bench) and the bin/
# entrypoints. The Ansible deploy playbook pushes the tarball to each instance
# over SSH; nothing is uploaded to S3.
#
# Usage:
#   ./deploy/aws/build-artifacts.sh    # build ./deploy/aws/excoredum-runtime.tgz
#
# Env overrides:
#   JRE_URL   Temurin 21 JRE tarball URL (default: Adoptium "latest" for
#             linux/x64; set a pinned release URL for reproducibility).
#
# The tarball layout mirrors the Docker image so the same entrypoint classpaths
# work unchanged:
#   jre/        Java 21 runtime
#   launcher/   exc-launcher installDist   (ClusterLauncher)
#   read/       exc-read installDist       (ReadServiceLauncher)
#   gateway/    exc-gateway installDist    (GatewayLauncher)
#   bench/      exc-bench installDist      (ExternalLoadRunner / ReadVerifyRunner)
#   bin/        node.sh read.sh gateway.sh load.sh verify.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEPLOY_DIR="$ROOT/deploy/aws"
OUT="$DEPLOY_DIR/excoredum-runtime.tgz"
JRE_URL="${JRE_URL:-https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jre/hotspot/normal/eclipse}"

echo "[build-artifacts] building application distributions ..."
"$ROOT/gradlew" --quiet \
    :exc-launcher:installDist \
    :exc-read:installDist \
    :exc-gateway:installDist \
    :exc-bench:installDist

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

echo "[build-artifacts] downloading Temurin 21 JRE ..."
mkdir -p "$STAGE/jre"
curl -fsSL "$JRE_URL" -o "$STAGE/jre.tgz"
tar -xzf "$STAGE/jre.tgz" -C "$STAGE/jre" --strip-components=1
rm -f "$STAGE/jre.tgz"

echo "[build-artifacts] staging distributions ..."
cp -R "$ROOT/exc-launcher/build/install/exc-launcher" "$STAGE/launcher"
cp -R "$ROOT/exc-read/build/install/exc-read" "$STAGE/read"
cp -R "$ROOT/exc-gateway/build/install/exc-gateway" "$STAGE/gateway"
cp -R "$ROOT/exc-bench/build/install/exc-bench" "$STAGE/bench"
cp -R "$DEPLOY_DIR/bin" "$STAGE/bin"
chmod +x "$STAGE/bin/"*.sh

echo "[build-artifacts] packing $OUT ..."
tar -czf "$OUT" -C "$STAGE" jre launcher read gateway bench bin

echo "[build-artifacts] done: $OUT"
