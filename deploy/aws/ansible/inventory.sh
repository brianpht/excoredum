#!/usr/bin/env bash
#
# inventory.sh - generate the Ansible inventory from `terraform output -json`.
# Run from anywhere; writes hosts.ini next to this script. The addressing
# strings (cluster_members, ingress_endpoints, archive_channels,
# query_channel) are written into the [all:vars] section of hosts.ini.
#
# Usage:
#   ./deploy/aws/ansible/inventory.sh
#
# Requires: terraform (already applied), python3.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TF_DIR="$ROOT/deploy/aws/terraform"
ANSIBLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

tfout="$ANSIBLE_DIR/.tfout.json"
hosts="$ANSIBLE_DIR/hosts.ini"

terraform -chdir="$TF_DIR" output -json > "$tfout"

python3 - "$tfout" "$hosts" <<'PY'
import json
import sys

tf = json.load(open(sys.argv[1]))
hosts_path = sys.argv[2]


def v(name):
    return tf[name]["value"]


node_priv = v("node_private_ips")
node_pub = v("node_public_ips")

lines = []
lines.append("[nodes]")
for i, (priv, pub) in enumerate(zip(node_priv, node_pub)):
    lines.append(f"node-{i} ansible_host={pub} node_id={i} host={priv}")
lines.append("")
lines.append("[read]")
lines.append(f"read-0 ansible_host={v('read_public_ip')} host={v('read_ip')}")
lines.append("")
lines.append("[gateway]")
lines.append(f"gateway-0 ansible_host={v('gateway_public_ip')} host={v('gateway_ip')}")
lines.append("")
lines.append("[load]")
lines.append(f"load-0 ansible_host={v('load_public_ip')}")
lines.append("")
lines.append("[verify]")
lines.append(f"verify-0 ansible_host={v('verify_public_ip')}")
lines.append("")
lines.append("[all:children]")
lines.append("nodes")
lines.append("read")
lines.append("gateway")
lines.append("load")
lines.append("verify")
lines.append("")
lines.append("[all:vars]")
lines.append(f'cluster_members="{v("cluster_members")}"')
lines.append(f'ingress_endpoints="{v("ingress_endpoints")}"')
lines.append(f'archive_channels="{v("archive_channels")}"')
lines.append(f'query_channel="{v("query_channel")}"')

open(hosts_path, "w").write("\n".join(lines) + "\n")
print(f"wrote {hosts_path}")
PY

rm -f "$tfout"
