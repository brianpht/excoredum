#!/bin/bash
set -euo pipefail
exec > /var/log/excoredum-provision.log 2>&1

command -v aws >/dev/null 2>&1 || dnf install -y awscli

mkdir -p /opt/excoredum /etc/excoredum
for i in $(seq 1 30); do
    aws s3 cp "${s3_url}" /tmp/excoredum-runtime.tgz && break
    echo "artifact not ready, retrying in 10s ..."
    sleep 10
done
test -f /tmp/excoredum-runtime.tgz || { echo "artifact download failed"; exit 1; }
tar -xzf /tmp/excoredum-runtime.tgz -C /opt/excoredum
rm -f /tmp/excoredum-runtime.tgz
chmod +x /opt/excoredum/bin/*.sh

cat > /etc/excoredum/gateway.properties <<'PROPS'
gateway.http.host=0.0.0.0
gateway.http.port=8080
gateway.read.requestChannel=${query_channel}
gateway.read.requestStreamId=300
gateway.read.responseStreamId=301
gateway.write.clientId=42
gateway.write.ingressEndpoints=${ingress_endpoints}
gateway.admin.uids=1,2,811
gateway.symbols=${symbols}
gateway.currencies=${currencies}
gateway.marketPump.intervalMs=0
PROPS

cat > /etc/systemd/system/excoredum.service <<'UNIT'
[Unit]
Description=excoredum gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=EXC_CONFIG=/etc/excoredum/gateway.properties
ExecStart=/opt/excoredum/bin/gateway.sh
Restart=on-failure
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now excoredum
