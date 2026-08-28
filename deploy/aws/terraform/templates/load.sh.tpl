#!/bin/bash
set -euo pipefail
exec > /var/log/excoredum-provision.log 2>&1

command -v aws >/dev/null 2>&1 || dnf install -y awscli

mkdir -p /opt/excoredum
for i in $(seq 1 30); do
    aws s3 cp "${s3_url}" /tmp/excoredum-runtime.tgz && break
    echo "artifact not ready, retrying in 10s ..."
    sleep 10
done
test -f /tmp/excoredum-runtime.tgz || { echo "artifact download failed"; exit 1; }
tar -xzf /tmp/excoredum-runtime.tgz -C /opt/excoredum
rm -f /tmp/excoredum-runtime.tgz
chmod +x /opt/excoredum/bin/*.sh

# The load runner is a one-shot: it is installed here but started manually once
# the cluster, read replica, and gateway are healthy.
cat > /etc/systemd/system/excoredum.service <<'UNIT'
[Unit]
Description=excoredum write-side load
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
Environment=EXC_ENDPOINTS=${ingress_endpoints}
Environment=EXC_OPS=${ops}
Environment=EXC_USERS=${users}
Environment=EXC_CLIENT_ID=${client_id}
ExecStart=/opt/excoredum/bin/load.sh
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
echo "load runner installed; start it with: systemctl start excoredum"
