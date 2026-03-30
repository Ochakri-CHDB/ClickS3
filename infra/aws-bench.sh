#!/usr/bin/env bash
set -euo pipefail

#############################################################################
# ClickS3 AWS Benchmark — Deploy 3 MinIO + 3 Bench nodes, run, destroy
#
# Usage:
#   ./infra/aws-bench.sh [--region eu-west-1] [--duration 5m] [--az a]
#
# Requires: aws CLI v2, ssh-keygen, python3
# All resources are tagged clicks3-bench-{RUN_ID} and auto-destroyed at end.
#############################################################################

REGION="${REGION:-eu-west-1}"
AZ_SUFFIX="${AZ_SUFFIX:-a}"
DURATION="${DURATION:-5m}"
MINIO_INSTANCE_TYPE="${MINIO_INSTANCE_TYPE:-c5d.xlarge}"
BENCH_INSTANCE_TYPE="${BENCH_INSTANCE_TYPE:-c5.xlarge}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASS="${MINIO_PASS:-minioadmin}"
BUCKET="${BUCKET:-clicks3-test}"
AMI=""  # auto-detected
RUN_ID="clicks3-$(date +%s)"
KEY_NAME="${RUN_ID}-key"
KEY_FILE="/tmp/${KEY_NAME}.pem"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Resource IDs for cleanup
VPC_ID="" ; SUBNET_ID="" ; IGW_ID="" ; RTB_ID="" ; SG_ID=""
MINIO_IDS=() ; BENCH_IDS=()
MINIO_IPS=() ; BENCH_IPS=()
MINIO_PRIV_IPS=()

log()  { echo -e "\033[1;34m[clicks3]\033[0m $*"; }
ok()   { echo -e "\033[1;32m[  OK  ]\033[0m $*"; }
err()  { echo -e "\033[1;31m[ERROR]\033[0m $*"; }
die()  { err "$*"; cleanup; exit 1; }

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --region)   REGION="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --az)       AZ_SUFFIX="$2"; shift 2 ;;
    --minio-type) MINIO_INSTANCE_TYPE="$2"; shift 2 ;;
    --bench-type) BENCH_INSTANCE_TYPE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

AZ="${REGION}${AZ_SUFFIX}"

#############################################################################
# CLEANUP — destroys all resources, always called at exit
#############################################################################
cleanup() {
  log "Destroying all AWS resources (${RUN_ID})..."

  # Terminate instances
  local all_ids=("${MINIO_IDS[@]}" "${BENCH_IDS[@]}")
  if [[ ${#all_ids[@]} -gt 0 ]]; then
    log "Terminating ${#all_ids[@]} instances..."
    aws ec2 terminate-instances --region "$REGION" \
      --instance-ids "${all_ids[@]}" >/dev/null 2>&1 || true
    aws ec2 wait instance-terminated --region "$REGION" \
      --instance-ids "${all_ids[@]}" 2>/dev/null || true
    ok "Instances terminated"
  fi

  # Delete key pair
  if [[ -n "$KEY_NAME" ]]; then
    aws ec2 delete-key-pair --region "$REGION" --key-name "$KEY_NAME" 2>/dev/null || true
    rm -f "$KEY_FILE"
  fi

  # Detach and delete IGW
  if [[ -n "$IGW_ID" && -n "$VPC_ID" ]]; then
    aws ec2 detach-internet-gateway --region "$REGION" \
      --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID" 2>/dev/null || true
    aws ec2 delete-internet-gateway --region "$REGION" \
      --internet-gateway-id "$IGW_ID" 2>/dev/null || true
  fi

  # Delete subnet
  if [[ -n "$SUBNET_ID" ]]; then
    aws ec2 delete-subnet --region "$REGION" --subnet-id "$SUBNET_ID" 2>/dev/null || true
  fi

  # Delete security group
  if [[ -n "$SG_ID" ]]; then
    aws ec2 delete-security-group --region "$REGION" --group-id "$SG_ID" 2>/dev/null || true
  fi

  # Delete route table (non-main)
  if [[ -n "$RTB_ID" ]]; then
    aws ec2 delete-route-table --region "$REGION" --route-table-id "$RTB_ID" 2>/dev/null || true
  fi

  # Delete VPC
  if [[ -n "$VPC_ID" ]]; then
    aws ec2 delete-vpc --region "$REGION" --vpc-id "$VPC_ID" 2>/dev/null || true
  fi

  ok "All resources destroyed."
}

trap cleanup EXIT

#############################################################################
# 1. Find latest Amazon Linux 2023 AMI
#############################################################################
find_ami() {
  log "Finding latest Amazon Linux 2023 AMI in ${REGION}..."
  AMI=$(aws ec2 describe-images --region "$REGION" \
    --owners amazon \
    --filters "Name=name,Values=al2023-ami-2023*-x86_64" \
              "Name=state,Values=available" \
    --query 'sort_by(Images, &CreationDate)[-1].ImageId' \
    --output text)
  ok "AMI: $AMI"
}

#############################################################################
# 2. Create VPC, Subnet, IGW, Security Group
#############################################################################
create_network() {
  log "Creating VPC..."
  VPC_ID=$(aws ec2 create-vpc --region "$REGION" \
    --cidr-block 10.0.0.0/16 \
    --tag-specifications "ResourceType=vpc,Tags=[{Key=Name,Value=${RUN_ID}}]" \
    --query 'Vpc.VpcId' --output text)
  aws ec2 modify-vpc-attribute --region "$REGION" --vpc-id "$VPC_ID" --enable-dns-support
  aws ec2 modify-vpc-attribute --region "$REGION" --vpc-id "$VPC_ID" --enable-dns-hostnames

  log "Creating subnet in ${AZ}..."
  SUBNET_ID=$(aws ec2 create-subnet --region "$REGION" \
    --vpc-id "$VPC_ID" --cidr-block 10.0.1.0/24 \
    --availability-zone "$AZ" \
    --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=${RUN_ID}}]" \
    --query 'Subnet.SubnetId' --output text)
  aws ec2 modify-subnet-attribute --region "$REGION" \
    --subnet-id "$SUBNET_ID" --map-public-ip-on-launch

  log "Creating internet gateway..."
  IGW_ID=$(aws ec2 create-internet-gateway --region "$REGION" \
    --tag-specifications "ResourceType=internet-gateway,Tags=[{Key=Name,Value=${RUN_ID}}]" \
    --query 'InternetGateway.InternetGatewayId' --output text)
  aws ec2 attach-internet-gateway --region "$REGION" \
    --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID"

  RTB_ID=$(aws ec2 create-route-table --region "$REGION" \
    --vpc-id "$VPC_ID" \
    --query 'RouteTable.RouteTableId' --output text)
  aws ec2 create-route --region "$REGION" \
    --route-table-id "$RTB_ID" --destination-cidr-block 0.0.0.0/0 \
    --gateway-id "$IGW_ID" >/dev/null
  aws ec2 associate-route-table --region "$REGION" \
    --route-table-id "$RTB_ID" --subnet-id "$SUBNET_ID" >/dev/null

  log "Creating security group..."
  SG_ID=$(aws ec2 create-security-group --region "$REGION" \
    --group-name "${RUN_ID}-sg" --description "ClickS3 bench" \
    --vpc-id "$VPC_ID" \
    --query 'GroupId' --output text)
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG_ID" \
    --protocol tcp --port 22 --cidr 0.0.0.0/0 >/dev/null
  # All traffic within VPC
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG_ID" \
    --protocol -1 --cidr 10.0.0.0/16 >/dev/null

  ok "Network: VPC=$VPC_ID  Subnet=$SUBNET_ID  SG=$SG_ID"
}

#############################################################################
# 3. Create key pair
#############################################################################
create_keypair() {
  log "Creating key pair ${KEY_NAME}..."
  aws ec2 create-key-pair --region "$REGION" \
    --key-name "$KEY_NAME" --query 'KeyMaterial' --output text > "$KEY_FILE"
  chmod 600 "$KEY_FILE"
  ok "Key: $KEY_FILE"
}

#############################################################################
# 4. Launch EC2 instances
#############################################################################
launch_instances() {
  log "Launching 3 MinIO nodes (${MINIO_INSTANCE_TYPE})..."
  local minio_out
  minio_out=$(aws ec2 run-instances --region "$REGION" \
    --image-id "$AMI" --instance-type "$MINIO_INSTANCE_TYPE" \
    --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET_ID" --count 3 \
    --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${RUN_ID}-minio},{Key=Role,Value=minio}]" \
    --query 'Instances[*].InstanceId' --output text)
  read -ra MINIO_IDS <<< "$minio_out"

  log "Launching 3 Bench nodes (${BENCH_INSTANCE_TYPE})..."
  local bench_out
  bench_out=$(aws ec2 run-instances --region "$REGION" \
    --image-id "$AMI" --instance-type "$BENCH_INSTANCE_TYPE" \
    --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET_ID" --count 3 \
    --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${RUN_ID}-bench},{Key=Role,Value=bench}]" \
    --query 'Instances[*].InstanceId' --output text)
  read -ra BENCH_IDS <<< "$bench_out"

  local all_ids=("${MINIO_IDS[@]}" "${BENCH_IDS[@]}")
  log "Waiting for ${#all_ids[@]} instances to be running..."
  aws ec2 wait instance-running --region "$REGION" --instance-ids "${all_ids[@]}"

  # Fetch IPs
  for id in "${MINIO_IDS[@]}"; do
    local ip priv_ip
    ip=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$id" \
      --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    priv_ip=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$id" \
      --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
    MINIO_IPS+=("$ip")
    MINIO_PRIV_IPS+=("$priv_ip")
  done

  for id in "${BENCH_IDS[@]}"; do
    local ip
    ip=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$id" \
      --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    BENCH_IPS+=("$ip")
  done

  ok "MinIO nodes: ${MINIO_IPS[*]}"
  ok "MinIO private: ${MINIO_PRIV_IPS[*]}"
  ok "Bench nodes: ${BENCH_IPS[*]}"
}

#############################################################################
# 5. Wait for SSH
#############################################################################
wait_ssh() {
  local ip="$1" max_wait=120 elapsed=0
  while ! ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${ip}" "true" 2>/dev/null; do
    sleep 5
    elapsed=$((elapsed + 5))
    if [[ $elapsed -ge $max_wait ]]; then
      die "SSH timeout for $ip"
    fi
  done
}

wait_all_ssh() {
  log "Waiting for SSH on all nodes..."
  for ip in "${MINIO_IPS[@]}" "${BENCH_IPS[@]}"; do
    wait_ssh "$ip" &
  done
  wait
  ok "All nodes reachable via SSH"
}

#############################################################################
# 6. Install MinIO cluster
#############################################################################
setup_minio() {
  log "Installing MinIO (3 servers, each with NVMe SSD)..."

  # Install MinIO on all 3 nodes, each runs independently
  # All 3 listen on :9000, bench nodes round-robin across them
  for i in 0 1 2; do
    local ip="${MINIO_IPS[$i]}"
    log "  Setting up MinIO on ${ip} (server $((i+1))/3)..."

    ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${ip}" bash -s -- \
      "${MINIO_USER}" "${MINIO_PASS}" <<'SETUP_EOF'
set -e
MUSER="$1"; MPASS="$2"

sudo dnf install -y wget xfsprogs >/dev/null 2>&1
wget -q https://dl.min.io/server/minio/release/linux-amd64/minio -O /tmp/minio
chmod +x /tmp/minio
sudo mv /tmp/minio /usr/local/bin/minio
wget -q https://dl.min.io/client/mc/release/linux-amd64/mc -O /tmp/mc
chmod +x /tmp/mc
sudo mv /tmp/mc /usr/local/bin/mc

# Mount NVMe SSD (c5d.xlarge: /dev/nvme1n1 = ~93 GB)
NVME_DEV=""
for dev in /dev/nvme1n1 /dev/nvme2n1 /dev/xvdb; do
  if [ -b "$dev" ]; then NVME_DEV="$dev"; break; fi
done

sudo mkdir -p /mnt/ssd
if [ -n "$NVME_DEV" ]; then
  # Unmount if already mounted, then format
  EXISTING_MOUNT=$(findmnt -n -o TARGET "$NVME_DEV" 2>/dev/null || true)
  if [ -n "$EXISTING_MOUNT" ]; then
    sudo umount "$EXISTING_MOUNT" 2>/dev/null || true
  fi
  sudo mkfs.xfs -f "$NVME_DEV" 2>/dev/null || true
  sudo mount "$NVME_DEV" /mnt/ssd 2>/dev/null || true
fi
sudo mkdir -p /mnt/ssd/data
sudo chown -R ec2-user:ec2-user /mnt/ssd

# Single server mode with NVMe SSD
cat > /tmp/minio-env <<ENVFILE
MINIO_ROOT_USER=${MUSER}
MINIO_ROOT_PASSWORD=${MPASS}
ENVFILE

sudo tee /etc/systemd/system/minio.service > /dev/null <<SVC
[Unit]
Description=MinIO
After=network-online.target
[Service]
User=ec2-user
Group=ec2-user
EnvironmentFile=/tmp/minio-env
ExecStart=/usr/local/bin/minio server /mnt/ssd/data --address :9000 --console-address :9001
Restart=always
LimitNOFILE=65536
[Install]
WantedBy=multi-user.target
SVC

sudo systemctl daemon-reload
sudo systemctl enable minio
sudo systemctl start minio
echo "MinIO service started on $(hostname)"
SETUP_EOF
  done

  log "Waiting for MinIO servers to be ready..."
  sleep 10

  # Health check on each MinIO server
  for i in 0 1 2; do
    local ip="${MINIO_IPS[$i]}"
    local priv="${MINIO_PRIV_IPS[$i]}"
    log "  Checking MinIO server $((i+1)) (${priv})..."

    ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${ip}" bash -s -- \
      "${priv}" "${MINIO_USER}" "${MINIO_PASS}" "${BUCKET}" <<'MC_EOF'
PRIV_IP="$1"; MU="$2"; MP="$3"; BK="$4"

for attempt in $(seq 1 20); do
  if curl -sf -o /dev/null "http://${PRIV_IP}:9000/minio/health/live" 2>/dev/null; then
    echo "  MinIO responding on ${PRIV_IP}"
    break
  fi
  echo "  Waiting... attempt $attempt/20"
  sleep 3
done

mc alias set clicks3 "http://${PRIV_IP}:9000" "$MU" "$MP"
mc mb "clicks3/${BK}" --ignore-existing 2>/dev/null || true
mc admin info clicks3
MC_EOF
  done

  ok "MinIO servers ready (3x independent, NVMe SSD), bucket '${BUCKET}' created"
}

#############################################################################
# 7. Build and deploy clicks3
#############################################################################
deploy_clicks3() {
  log "Cross-compiling clicks3 for linux/amd64..."
  cd "$PROJECT_DIR"
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-s -w" -o /tmp/clicks3-linux .
  ok "Binary built: $(ls -lh /tmp/clicks3-linux | awk '{print $5}')"

  log "Deploying clicks3 to bench nodes..."
  for ip in "${BENCH_IPS[@]}"; do
    scp $SSH_OPTS -i "$KEY_FILE" /tmp/clicks3-linux "ec2-user@${ip}:/tmp/clicks3" &
  done
  wait

  for ip in "${BENCH_IPS[@]}"; do
    ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${ip}" "chmod +x /tmp/clicks3"
  done
  ok "clicks3 deployed to all bench nodes"
}

#############################################################################
# 8. Run benchmark
#############################################################################
run_benchmark() {
  log "Running benchmark on 3 nodes (duration: ${DURATION})..."
  log "Each bench node → different MinIO server (load distributed)"
  echo ""

  local pids=()
  for i in 0 1 2; do
    local ip="${BENCH_IPS[$i]}"
    local minio_ip="${MINIO_PRIV_IPS[$i]}"
    local minio_endpoint="http://${minio_ip}:9000"
    local node_role="node$((i+1))"
    local node_id="bench-$((i+1))-${ip}"

    log "  Starting ${node_role} on ${ip} → MinIO ${minio_ip}..."
    ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${ip}" bash -s <<RUN_EOF &
set -e
/tmp/clicks3 \
  --endpoint ${minio_endpoint} \
  --access-key ${MINIO_USER} \
  --secret-key ${MINIO_PASS} \
  --bucket ${BUCKET} \
  --prefix mergetree/ \
  --role ${node_role} \
  --node-id ${node_id} \
  --duration ${DURATION} \
  --warmup 30s \
  --output /tmp/report.json \
  --path-style=true \
  2>&1 | tee /tmp/clicks3.log
RUN_EOF
    pids+=($!)
  done

  # Wait for all benchmarks
  local failed=0
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      failed=$((failed + 1))
    fi
  done

  echo ""
  if [[ $failed -gt 0 ]]; then
    err "$failed node(s) reported failures"
  else
    ok "All 3 nodes completed benchmark"
  fi
}

#############################################################################
# 9. Collect reports
#############################################################################
collect_reports() {
  log "Collecting reports..."
  mkdir -p /tmp/clicks3-reports-${RUN_ID}

  for i in 0 1 2; do
    local ip="${BENCH_IPS[$i]}"
    scp $SSH_OPTS -i "$KEY_FILE" \
      "ec2-user@${ip}:/tmp/report.json" \
      "/tmp/clicks3-reports-${RUN_ID}/report-node$((i+1)).json" 2>/dev/null || true
    scp $SSH_OPTS -i "$KEY_FILE" \
      "ec2-user@${ip}:/tmp/clicks3.log" \
      "/tmp/clicks3-reports-${RUN_ID}/log-node$((i+1)).txt" 2>/dev/null || true
  done

  ok "Reports saved to /tmp/clicks3-reports-${RUN_ID}/"

  # Merge reports
  if [[ -f "${SCRIPT_DIR}/merge-reports.py" ]]; then
    log "Merging reports..."
    python3 "${SCRIPT_DIR}/merge-reports.py" /tmp/clicks3-reports-${RUN_ID}/report-node*.json
  fi
}

#############################################################################
# MAIN
#############################################################################
main() {
  echo ""
  echo "╔══════════════════════════════════════════════════════════════╗"
  echo "║     ClickS3 — AWS Distributed Benchmark                    ║"
  echo "║     3 MinIO nodes + 3 Bench nodes (auto-destroy)           ║"
  echo "╚══════════════════════════════════════════════════════════════╝"
  echo ""
  echo "  Region:     ${REGION}"
  echo "  AZ:         ${AZ}"
  echo "  MinIO type: ${MINIO_INSTANCE_TYPE} × 3"
  echo "  Bench type: ${BENCH_INSTANCE_TYPE} × 3"
  echo "  Duration:   ${DURATION}"
  echo "  Run ID:     ${RUN_ID}"
  echo ""

  find_ami
  create_keypair
  create_network
  launch_instances
  wait_all_ssh
  setup_minio
  deploy_clicks3
  run_benchmark
  collect_reports

  echo ""
  echo "╔══════════════════════════════════════════════════════════════╗"
  echo "║     Benchmark complete — destroying infrastructure...      ║"
  echo "╚══════════════════════════════════════════════════════════════╝"
  echo ""
  echo "  Reports: /tmp/clicks3-reports-${RUN_ID}/"
  echo ""
  # cleanup is called automatically via trap EXIT
}

main "$@"
