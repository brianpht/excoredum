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

# The verifier is a one-shot: it is installed here but started manually after
# the load run has completed.
cat > /etc/systemd/system/excoredum.service <<'UNIT'
[Unit]
Description=excoredum read-side verification
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
Environment=EXC_QUERY=${query_channel}
Environment=EXC_OPS=${ops}
Environment=EXC_USERS=${users}
ExecStart=/opt/excoredum/bin/verify.sh
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
echo "verify runner installed; start it with: systemctl start excoredum"
