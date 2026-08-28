#!/bin/bash
set -euo pipefail
exec > /var/log/excoredum-provision.log 2>&1

command -v aws >/dev/null 2>&1 || dnf install -y awscli

mkdir -p /opt/excoredum /data
for i in $(seq 1 30); do
    aws s3 cp "${s3_url}" /tmp/excoredum-runtime.tgz && break
    echo "artifact not ready, retrying in 10s ..."
    sleep 10
done
test -f /tmp/excoredum-runtime.tgz || { echo "artifact download failed"; exit 1; }
tar -xzf /tmp/excoredum-runtime.tgz -C /opt/excoredum
rm -f /tmp/excoredum-runtime.tgz
chmod +x /opt/excoredum/bin/*.sh

cat > /etc/systemd/system/excoredum.service <<'UNIT'
[Unit]
Description=excoredum cluster node ${node_id}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=EXC_NODE_ID=${node_id}
Environment=EXC_MEMBERS=${members}
Environment=EXC_HOST=${host}
Environment=EXC_CLEAN_START=${clean_start}
Environment=EXC_BASE_DIR=/data
Environment=EXC_METRICS_INTERVAL_MS=${metrics_interval_ms}
ExecStart=/opt/excoredum/bin/node.sh
Restart=on-failure
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now excoredum
