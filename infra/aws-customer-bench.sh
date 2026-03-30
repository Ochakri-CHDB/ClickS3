#!/usr/bin/env bash
set -eo pipefail

#############################################################################
# ClickS3 — Configuration Benchmark
#
# Launches 3 bench + 3 MinIO nodes matching reference hardware ratios,
# runs the benchmark, collects results, and auto-destroys everything.
#
# Modes:
#   --mode direct       Each bench-N talks directly to MinIO-N (1:1 mapping)
#   --mode haproxy      HAProxy on each bench node, sticky to all 3 MinIO
#   --mode distributed  3-node MinIO cluster with erasure coding (EC:3)
#
# Storage:
#   --storage nvme      NVMe SSD instance store (c5d instances)
#   --storage ssd       EBS gp3 SSD (2×500GB per node, 3000 IOPS each)
#   --storage hdd       EBS st1 HDD (2×1TB per node)
#
# Usage:
#   ./infra/aws-customer-bench.sh --mode direct --storage hdd --duration 5m
#   ./infra/aws-customer-bench.sh --mode haproxy --storage ssd --duration 5m
#   ./infra/aws-customer-bench.sh --mode distributed --storage ssd --duration 5m
#
# Requires: aws CLI v2, go, python3
#############################################################################

REGION="${REGION:-eu-west-1}"
AZ_SUFFIX="${AZ_SUFFIX:-a}"
DURATION="${DURATION:-5m}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASS="${MINIO_PASS:-minioadmin}"
BUCKET="${BUCKET:-clicks3-test}"
MODE="${MODE:-direct}"        # direct | haproxy | distributed
STORAGE="${STORAGE:-nvme}"    # nvme | ssd | hdd

# Instance types — matching reference ratio (CH: 224 cores / MinIO: 64 cores ≈ 3.5:1)
BENCH_TYPE="${BENCH_TYPE:-c5.9xlarge}"     # 36 vCPU, 72 GB — bench (ClickHouse proxy)
MINIO_TYPE=""                              # auto-selected based on storage

HDD_VOL_SIZE=1000   # GB per HDD volume (st1 min 500)
SSD_VOL_SIZE=500    # GB per SSD volume (gp3)
SSD_VOL_IOPS=3000
SSD_VOL_THROUGHPUT=250  # MB/s per volume

AMI=""
RUN_ID="cust-$(date +%s)"
KEY_NAME="${RUN_ID}-key"
KEY_FILE="/tmp/${KEY_NAME}.pem"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

VPC_ID="" ; SUBNET_ID="" ; IGW_ID="" ; RTB_ID="" ; SG_ID=""
MINIO_IDS=() ; BENCH_IDS=()
MINIO_PUB_IPS=() ; MINIO_PRIV_IPS=() ; BENCH_PUB_IPS=()

log()  { echo -e "\033[1;34m[clicks3]\033[0m $*"; }
ok()   { echo -e "\033[1;32m[  OK  ]\033[0m $*"; }
err()  { echo -e "\033[1;31m[ERROR]\033[0m $*"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region)      REGION="$2"; shift 2 ;;
    --duration)    DURATION="$2"; shift 2 ;;
    --az)          AZ_SUFFIX="$2"; shift 2 ;;
    --mode)        MODE="$2"; shift 2 ;;
    --storage)     STORAGE="$2"; shift 2 ;;
    --bench-type)  BENCH_TYPE="$2"; shift 2 ;;
    --minio-type)  MINIO_TYPE="$2"; shift 2 ;;
    *) echo "Unknown: $1"; exit 1 ;;
  esac
done

AZ="${REGION}${AZ_SUFFIX}"

# Auto-select MinIO instance type based on storage
if [[ -z "$MINIO_TYPE" ]]; then
  case "$STORAGE" in
    nvme) MINIO_TYPE="c5d.4xlarge" ;;  # 16 vCPU + NVMe instance store
    *)    MINIO_TYPE="c5.4xlarge"  ;;  # 16 vCPU + attached EBS
  esac
fi

case "$STORAGE" in
  hdd)  STORAGE_LABEL="HDD (EBS st1, 2×${HDD_VOL_SIZE}GB)" ;;
  ssd)  STORAGE_LABEL="SSD (EBS gp3, 2×${SSD_VOL_SIZE}GB, ${SSD_VOL_IOPS} IOPS, ${SSD_VOL_THROUGHPUT} MB/s)" ;;
  nvme) STORAGE_LABEL="NVMe SSD (instance store)" ;;
  *)    echo "Invalid --storage: $STORAGE"; exit 1 ;;
esac

case "$MODE" in
  direct)      MODE_LABEL="Direct 1:1 (Bench-N → MinIO-N)" ;;
  haproxy)     MODE_LABEL="HAProxy (sticky LB → 3 MinIO)" ;;
  distributed) MODE_LABEL="Distributed MinIO cluster (EC:3, 3×2 drives)" ;;
  *)           echo "Invalid --mode: $MODE"; exit 1 ;;
esac

BENCH_LABEL=$(echo "$BENCH_TYPE" | sed 's/c5[d]*\.\(.*\)/\1/')
MINIO_LABEL=$(echo "$MINIO_TYPE" | sed 's/c5[d]*\.\(.*\)/\1/')
RESULTS_DIR="${PROJECT_DIR}/results-bench-${MODE}-${STORAGE}-b${BENCH_LABEL}-m${MINIO_LABEL}-$(date +%Y-%m-%d)"

#############################################################################
cleanup() {
  log "Destroying all AWS resources (${RUN_ID})..."
  local all_ids=("${MINIO_IDS[@]}" "${BENCH_IDS[@]}")
  if [[ ${#all_ids[@]} -gt 0 ]]; then
    log "Terminating ${#all_ids[@]} instances..."
    aws ec2 terminate-instances --region "$REGION" \
      --instance-ids "${all_ids[@]}" >/dev/null 2>&1 || true
    aws ec2 wait instance-terminated --region "$REGION" \
      --instance-ids "${all_ids[@]}" 2>/dev/null || true
    ok "Instances terminated"
  fi
  [[ -n "$KEY_NAME" ]] && {
    aws ec2 delete-key-pair --region "$REGION" --key-name "$KEY_NAME" 2>/dev/null || true
    rm -f "$KEY_FILE"
  }
  if [[ -n "$IGW_ID" && -n "$VPC_ID" ]]; then
    aws ec2 detach-internet-gateway --region "$REGION" \
      --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID" 2>/dev/null || true
    aws ec2 delete-internet-gateway --region "$REGION" \
      --internet-gateway-id "$IGW_ID" 2>/dev/null || true
  fi
  [[ -n "$SUBNET_ID" ]] && \
    aws ec2 delete-subnet --region "$REGION" --subnet-id "$SUBNET_ID" 2>/dev/null || true
  [[ -n "$SG_ID" ]] && \
    aws ec2 delete-security-group --region "$REGION" --group-id "$SG_ID" 2>/dev/null || true
  [[ -n "$RTB_ID" ]] && \
    aws ec2 delete-route-table --region "$REGION" --route-table-id "$RTB_ID" 2>/dev/null || true
  [[ -n "$VPC_ID" ]] && \
    aws ec2 delete-vpc --region "$REGION" --vpc-id "$VPC_ID" 2>/dev/null || true
  ok "All resources destroyed."
}
trap cleanup EXIT

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

create_network() {
  log "Creating VPC + subnet + IGW..."
  VPC_ID=$(aws ec2 create-vpc --region "$REGION" \
    --cidr-block 10.0.0.0/16 \
    --tag-specifications "ResourceType=vpc,Tags=[{Key=Name,Value=${RUN_ID}}]" \
    --query 'Vpc.VpcId' --output text)
  aws ec2 modify-vpc-attribute --region "$REGION" --vpc-id "$VPC_ID" --enable-dns-support
  aws ec2 modify-vpc-attribute --region "$REGION" --vpc-id "$VPC_ID" --enable-dns-hostnames

  SUBNET_ID=$(aws ec2 create-subnet --region "$REGION" \
    --vpc-id "$VPC_ID" --cidr-block 10.0.1.0/24 \
    --availability-zone "$AZ" \
    --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=${RUN_ID}}]" \
    --query 'Subnet.SubnetId' --output text)
  aws ec2 modify-subnet-attribute --region "$REGION" \
    --subnet-id "$SUBNET_ID" --map-public-ip-on-launch

  IGW_ID=$(aws ec2 create-internet-gateway --region "$REGION" \
    --tag-specifications "ResourceType=internet-gateway,Tags=[{Key=Name,Value=${RUN_ID}}]" \
    --query 'InternetGateway.InternetGatewayId' --output text)
  aws ec2 attach-internet-gateway --region "$REGION" \
    --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID"

  RTB_ID=$(aws ec2 create-route-table --region "$REGION" \
    --vpc-id "$VPC_ID" --query 'RouteTable.RouteTableId' --output text)
  aws ec2 create-route --region "$REGION" \
    --route-table-id "$RTB_ID" --destination-cidr-block 0.0.0.0/0 \
    --gateway-id "$IGW_ID" >/dev/null
  aws ec2 associate-route-table --region "$REGION" \
    --route-table-id "$RTB_ID" --subnet-id "$SUBNET_ID" >/dev/null

  SG_ID=$(aws ec2 create-security-group --region "$REGION" \
    --group-name "${RUN_ID}-sg" --description "ClickS3 bench" \
    --vpc-id "$VPC_ID" --query 'GroupId' --output text)
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG_ID" \
    --protocol tcp --port 22 --cidr 0.0.0.0/0 >/dev/null
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG_ID" \
    --protocol -1 --cidr 10.0.0.0/16 >/dev/null
  ok "Network: VPC=$VPC_ID  Subnet=$SUBNET_ID  SG=$SG_ID"
}

create_keypair() {
  log "Creating key pair..."
  aws ec2 create-key-pair --region "$REGION" \
    --key-name "$KEY_NAME" --query 'KeyMaterial' --output text > "$KEY_FILE"
  chmod 600 "$KEY_FILE"
  ok "Key: $KEY_FILE"
}

#############################################################################
launch_instances() {
  log "Launching 3 bench nodes (${BENCH_TYPE}) + 3 MinIO nodes (${MINIO_TYPE})..."

  local bench_bdm='[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]'

  # MinIO block device mapping depends on storage type
  local minio_bdm="$bench_bdm"
  case "$STORAGE" in
    hdd)
      minio_bdm='[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}},{"DeviceName":"/dev/xvdb","Ebs":{"VolumeSize":'"${HDD_VOL_SIZE}"',"VolumeType":"st1","DeleteOnTermination":true}},{"DeviceName":"/dev/xvdc","Ebs":{"VolumeSize":'"${HDD_VOL_SIZE}"',"VolumeType":"st1","DeleteOnTermination":true}}]'
      ;;
    ssd)
      minio_bdm='[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}},{"DeviceName":"/dev/xvdb","Ebs":{"VolumeSize":'"${SSD_VOL_SIZE}"',"VolumeType":"gp3","Iops":'"${SSD_VOL_IOPS}"',"Throughput":'"${SSD_VOL_THROUGHPUT}"',"DeleteOnTermination":true}},{"DeviceName":"/dev/xvdc","Ebs":{"VolumeSize":'"${SSD_VOL_SIZE}"',"VolumeType":"gp3","Iops":'"${SSD_VOL_IOPS}"',"Throughput":'"${SSD_VOL_THROUGHPUT}"',"DeleteOnTermination":true}}]'
      ;;
  esac

  local mids
  mids=$(aws ec2 run-instances --region "$REGION" \
    --image-id "$AMI" --instance-type "$MINIO_TYPE" \
    --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET_ID" --count 3 \
    --block-device-mappings "$minio_bdm" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${RUN_ID}-minio},{Key=Role,Value=minio}]" \
    --query 'Instances[*].InstanceId' --output text)
  read -ra MINIO_IDS <<< "$mids"

  local bids
  bids=$(aws ec2 run-instances --region "$REGION" \
    --image-id "$AMI" --instance-type "$BENCH_TYPE" \
    --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET_ID" --count 3 \
    --block-device-mappings "$bench_bdm" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${RUN_ID}-bench},{Key=Role,Value=bench}]" \
    --query 'Instances[*].InstanceId' --output text)
  read -ra BENCH_IDS <<< "$bids"

  local all_ids=("${MINIO_IDS[@]}" "${BENCH_IDS[@]}")
  log "Waiting for ${#all_ids[@]} instances..."
  aws ec2 wait instance-running --region "$REGION" --instance-ids "${all_ids[@]}"

  for id in "${MINIO_IDS[@]}"; do
    local pub priv
    pub=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$id" \
      --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    priv=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$id" \
      --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
    MINIO_PUB_IPS+=("$pub")
    MINIO_PRIV_IPS+=("$priv")
  done

  for id in "${BENCH_IDS[@]}"; do
    local pub
    pub=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$id" \
      --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    BENCH_PUB_IPS+=("$pub")
  done

  ok "MinIO nodes: ${MINIO_PRIV_IPS[*]}"
  ok "Bench nodes: ${BENCH_PUB_IPS[*]}"
}

#############################################################################
wait_ssh() {
  local ip="$1" max_wait=180 elapsed=0
  while ! ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${ip}" "true" 2>/dev/null; do
    sleep 5
    elapsed=$((elapsed + 5))
    [[ $elapsed -ge $max_wait ]] && { err "SSH timeout for $ip"; return 1; }
  done
}

wait_all_ssh() {
  log "Waiting for SSH on all 6 nodes..."
  for ip in "${MINIO_PUB_IPS[@]}" "${BENCH_PUB_IPS[@]}"; do
    wait_ssh "$ip" &
  done
  wait
  ok "All nodes reachable via SSH"
}

#############################################################################
# Prepare MinIO node: install binaries + format/mount storage
prep_minio_node() {
  local ip="$1"
  ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${ip}" bash <<'PREP_EOF'
set -e
sudo dnf install -y wget xfsprogs >/dev/null 2>&1
wget -q https://dl.min.io/server/minio/release/linux-amd64/minio -O /tmp/minio
chmod +x /tmp/minio
sudo mv /tmp/minio /usr/local/bin/minio
wget -q https://dl.min.io/client/mc/release/linux-amd64/mc -O /tmp/mc
chmod +x /tmp/mc
sudo mv /tmp/mc /usr/local/bin/mc

# Mount data drives — detect Nitro NVMe vs legacy device names
# On Nitro instances (c5/c5d): root=/dev/nvme0n1, EBS or instance store = nvme1n1, nvme2n1...
# Distinguish instance store vs EBS via nvme model string
sudo mkdir -p /mnt/data/data1 /mnt/data/data2

# Collect non-root NVMe devices
DATA_DEVS=()
for d in /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1; do
  [ -b "$d" ] && DATA_DEVS+=("$d")
done

if [ ${#DATA_DEVS[@]} -ge 2 ]; then
  # Two data devices — mount separately as data1 and data2
  sudo mkfs.xfs -f "${DATA_DEVS[0]}" 2>/dev/null || true
  sudo mount "${DATA_DEVS[0]}" /mnt/data/data1 2>/dev/null || true
  sudo mkfs.xfs -f "${DATA_DEVS[1]}" 2>/dev/null || true
  sudo mount "${DATA_DEVS[1]}" /mnt/data/data2 2>/dev/null || true
  echo "Mounted ${DATA_DEVS[0]} → data1, ${DATA_DEVS[1]} → data2"
elif [ ${#DATA_DEVS[@]} -eq 1 ]; then
  # Single data device — mount at /mnt/data, create subdirs
  sudo mkfs.xfs -f "${DATA_DEVS[0]}" 2>/dev/null || true
  sudo mount "${DATA_DEVS[0]}" /mnt/data 2>/dev/null || true
  sudo mkdir -p /mnt/data/data1 /mnt/data/data2
  echo "Mounted ${DATA_DEVS[0]} at /mnt/data (single drive, data1+data2 as subdirs)"
else
  # Legacy device names (non-Nitro)
  for pair in "xvdb:data1" "xvdc:data2"; do
    dev="/dev/${pair%%:*}"
    dir="/mnt/data/${pair##*:}"
    if [ -b "$dev" ]; then
      EXISTING=$(findmnt -n -o TARGET "$dev" 2>/dev/null || true)
      [ -n "$EXISTING" ] && sudo umount "$EXISTING" 2>/dev/null || true
      sudo mkfs.xfs -f "$dev" 2>/dev/null || true
      sudo mount "$dev" "$dir" 2>/dev/null || true
      echo "Mounted $dev at $dir"
    fi
  done
fi

sudo chown -R ec2-user:ec2-user /mnt/data

echo "Node prepared — data1: $(df -h /mnt/data/data1 2>/dev/null | tail -1 | awk '{print $2}' || echo '?'), data2: $(df -h /mnt/data/data2 2>/dev/null | tail -1 | awk '{print $2}' || echo '?')"
PREP_EOF
}

# Start standalone MinIO (modes: direct, haproxy)
start_minio_standalone() {
  local pub="$1" muser="$2" mpass="$3"
  ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${pub}" bash -s -- "$muser" "$mpass" <<'START_EOF'
set -e
MUSER="$1"; MPASS="$2"

cat > /tmp/minio-env <<ENVFILE
MINIO_ROOT_USER=${MUSER}
MINIO_ROOT_PASSWORD=${MPASS}
ENVFILE

# Use data1 as the single data directory for standalone
sudo tee /etc/systemd/system/minio.service > /dev/null <<SVC
[Unit]
Description=MinIO Standalone
After=network-online.target
[Service]
User=ec2-user
Group=ec2-user
EnvironmentFile=/tmp/minio-env
ExecStart=/usr/local/bin/minio server /mnt/data/data1 --address :9000 --console-address :9001
Restart=always
RestartSec=3
LimitNOFILE=65536
[Install]
WantedBy=multi-user.target
SVC

sudo systemctl daemon-reload
sudo systemctl enable minio
sudo systemctl start minio
echo "MinIO standalone started"
START_EOF
}

# Start distributed MinIO (mode: distributed) — 3 nodes × 2 drives = EC:3
start_minio_distributed() {
  local pub="$1" ip1="$2" ip2="$3" ip3="$4" muser="$5" mpass="$6"
  ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${pub}" bash -s -- \
    "$ip1" "$ip2" "$ip3" "$muser" "$mpass" <<'START_EOF'
set -e
IP1="$1"; IP2="$2"; IP3="$3"; MUSER="$4"; MPASS="$5"

sudo tee -a /etc/hosts > /dev/null <<HOSTS
${IP1} minio1
${IP2} minio2
${IP3} minio3
HOSTS

cat > /tmp/minio-env <<ENVFILE
MINIO_ROOT_USER=${MUSER}
MINIO_ROOT_PASSWORD=${MPASS}
ENVFILE

sudo tee /etc/systemd/system/minio.service > /dev/null <<SVC
[Unit]
Description=MinIO Distributed
After=network-online.target
Wants=network-online.target
[Service]
User=ec2-user
Group=ec2-user
EnvironmentFile=/tmp/minio-env
ExecStart=/usr/local/bin/minio server \
  http://minio1:9000/mnt/data/data{1...2} \
  http://minio2:9000/mnt/data/data{1...2} \
  http://minio3:9000/mnt/data/data{1...2} \
  --address :9000 --console-address :9001
Restart=always
RestartSec=5
LimitNOFILE=65536
[Install]
WantedBy=multi-user.target
SVC

sudo systemctl daemon-reload
sudo systemctl enable minio
sudo systemctl start minio
echo "MinIO distributed started"
START_EOF
}

#############################################################################
setup_minio() {
  log "Preparing MinIO binaries on all 3 nodes..."
  for i in 0 1 2; do
    prep_minio_node "${MINIO_PUB_IPS[$i]}" &
  done
  wait
  ok "All 3 MinIO nodes prepared"

  if [[ "$MODE" == "distributed" ]]; then
    log "Starting distributed MinIO cluster (3 nodes × 2 drives = EC:3)..."
    local ip1="${MINIO_PRIV_IPS[0]}" ip2="${MINIO_PRIV_IPS[1]}" ip3="${MINIO_PRIV_IPS[2]}"
    for i in 0 1 2; do
      start_minio_distributed "${MINIO_PUB_IPS[$i]}" "$ip1" "$ip2" "$ip3" "$MINIO_USER" "$MINIO_PASS" &
    done
    wait
    ok "Distributed cluster starting..."
    sleep 30
  else
    log "Starting 3 standalone MinIO servers..."
    for i in 0 1 2; do
      start_minio_standalone "${MINIO_PUB_IPS[$i]}" "$MINIO_USER" "$MINIO_PASS" &
    done
    wait
    ok "All 3 standalone MinIO servers started"
    sleep 15
  fi

  local health_path="live"
  [[ "$MODE" == "distributed" ]] && health_path="cluster"

  for i in 0 1 2; do
    local pub="${MINIO_PUB_IPS[$i]}" priv="${MINIO_PRIV_IPS[$i]}"
    log "  Health check MinIO-$((i+1)) (${priv})..."
    ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${pub}" bash -s -- \
      "$priv" "$MINIO_USER" "$MINIO_PASS" "$BUCKET" "$health_path" <<'HC_EOF'
PRIV_IP="$1"; MU="$2"; MP="$3"; BK="$4"; HP="$5"
for attempt in $(seq 1 40); do
  if curl -sf -o /dev/null "http://${PRIV_IP}:9000/minio/health/${HP}" 2>/dev/null; then
    echo "  Healthy on attempt $attempt"
    break
  fi
  [ "$attempt" -eq 40 ] && echo "  WARNING: health check timed out"
  sleep 5
done
mc alias set clicks3 "http://${PRIV_IP}:9000" "$MU" "$MP" >/dev/null 2>&1
mc mb "clicks3/${BK}" --ignore-existing 2>/dev/null || echo "  Continuing (bucket may exist)"
echo "  MinIO ready: ${PRIV_IP}"
HC_EOF
  done
  ok "All MinIO servers ready (mode: ${MODE}, storage: ${STORAGE})"
}

#############################################################################
setup_haproxy() {
  [[ "$MODE" != "haproxy" ]] && return

  local ip1="${MINIO_PRIV_IPS[0]}" ip2="${MINIO_PRIV_IPS[1]}" ip3="${MINIO_PRIV_IPS[2]}"
  log "Setting up HAProxy on bench nodes → [${ip1}, ${ip2}, ${ip3}]..."

  for bip in "${BENCH_PUB_IPS[@]}"; do
    ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${bip}" bash -s -- \
      "$ip1" "$ip2" "$ip3" <<'HAPROXY_EOF'
set -e
IP1="$1"; IP2="$2"; IP3="$3"
sudo dnf install -y haproxy >/dev/null 2>&1

sudo tee /etc/haproxy/haproxy.cfg > /dev/null <<HACFG
global
    maxconn 10000
    log stdout format raw local0

defaults
    mode http
    timeout connect 10s
    timeout client  60s
    timeout server  60s
    retries 3
    option httpchk GET /minio/health/live
    http-check expect status 200

frontend s3_frontend
    bind *:9090
    default_backend minio_backend

backend minio_backend
    balance source
    hash-type consistent
    server minio1 ${IP1}:9000 check inter 5s fall 3 rise 2
    server minio2 ${IP2}:9000 check inter 5s fall 3 rise 2
    server minio3 ${IP3}:9000 check inter 5s fall 3 rise 2

listen stats
    bind *:8404
    stats enable
    stats uri /stats
    stats refresh 5s
HACFG

sudo systemctl enable haproxy
sudo systemctl restart haproxy
echo "HAProxy started: localhost:9090 → [${IP1}, ${IP2}, ${IP3}]:9000"
HAPROXY_EOF
  done
  ok "HAProxy configured on all bench nodes"
  sleep 3
}

#############################################################################
deploy_clicks3() {
  log "Cross-compiling clicks3 for linux/amd64..."
  cd "$PROJECT_DIR"
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-s -w" -o /tmp/clicks3-linux .
  ok "Binary: $(ls -lh /tmp/clicks3-linux | awk '{print $5}')"

  log "Deploying to 3 bench nodes..."
  for ip in "${BENCH_PUB_IPS[@]}"; do
    scp $SSH_OPTS -i "$KEY_FILE" /tmp/clicks3-linux "ec2-user@${ip}:/tmp/clicks3" &
  done
  wait
  for ip in "${BENCH_PUB_IPS[@]}"; do
    ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${ip}" "chmod +x /tmp/clicks3"
  done
  ok "clicks3 deployed to all bench nodes"
}

#############################################################################
run_benchmark() {
  log "Running benchmark (mode: ${MODE}, storage: ${STORAGE}, duration: ${DURATION})..."
  echo ""

  local pids=()
  for i in 0 1 2; do
    local bip="${BENCH_PUB_IPS[$i]}"
    local endpoint
    local node_id="${MODE}-${STORAGE}-node$((i+1))"

    case "$MODE" in
      direct)
        endpoint="http://${MINIO_PRIV_IPS[$i]}:9000"
        ;;
      haproxy)
        endpoint="http://127.0.0.1:9090"
        ;;
      distributed)
        endpoint="http://${MINIO_PRIV_IPS[$i]}:9000"
        ;;
    esac

    log "  Node $((i+1)): ${bip} → ${endpoint}"
    ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${bip}" bash -s <<RUN_EOF &
set -e
/tmp/clicks3 \
  --endpoint "${endpoint}" \
  --access-key "${MINIO_USER}" \
  --secret-key "${MINIO_PASS}" \
  --bucket "${BUCKET}" \
  --prefix mergetree/ \
  --role "node$((i+1))" \
  --node-id "${node_id}" \
  --duration "${DURATION}" \
  --warmup 15s \
  --output /tmp/report.json \
  --path-style=true \
  2>&1 | tee /tmp/clicks3.log
RUN_EOF
    pids+=($!)
  done

  local failed=0
  for pid in "${pids[@]}"; do
    wait "$pid" || failed=$((failed + 1))
  done

  echo ""
  if [[ $failed -gt 0 ]]; then
    err "$failed node(s) reported failures"
  else
    ok "All 3 nodes completed benchmark"
  fi
}

#############################################################################
collect_reports() {
  mkdir -p "$RESULTS_DIR"
  log "Collecting reports → ${RESULTS_DIR}/"

  for i in 0 1 2; do
    local ip="${BENCH_PUB_IPS[$i]}"
    scp $SSH_OPTS -i "$KEY_FILE" \
      "ec2-user@${ip}:/tmp/report.json" \
      "${RESULTS_DIR}/report-node$((i+1)).json" 2>/dev/null || true
    scp $SSH_OPTS -i "$KEY_FILE" \
      "ec2-user@${ip}:/tmp/clicks3.log" \
      "${RESULTS_DIR}/log-node$((i+1)).txt" 2>/dev/null || true
  done

  if [[ -f "${SCRIPT_DIR}/merge-reports.py" ]]; then
    log "Merging reports..."
    python3 "${SCRIPT_DIR}/merge-reports.py" "${RESULTS_DIR}"/report-node*.json 2>/dev/null || true
  fi

  ok "Reports saved to ${RESULTS_DIR}/"
}

#############################################################################
generate_report() {
  local rf="${RESULTS_DIR}/RESULTS.md"
  log "Generating report..."

  cat > "$rf" <<HEADER
# ClickS3 Benchmark — ${MODE_LABEL}

**Date:** $(date +%Y-%m-%d)
**Run ID:** ${RUN_ID}
**Duration:** ${DURATION}

## Configuration

| Component | Instance Type | Count | vCPU | Storage |
|-----------|--------------|-------|------|---------|
| Bench (ClickHouse proxy) | ${BENCH_TYPE} | 3 | $(echo "$BENCH_TYPE" | grep -oP '\d+xlarge' | head -1 || echo "?") | — |
| MinIO | ${MINIO_TYPE} | 3 | $(echo "$MINIO_TYPE" | grep -oP '\d+xlarge' | head -1 || echo "?") | ${STORAGE_LABEL} |

## Architecture

HEADER

  case "$MODE" in
    direct)
      cat >> "$rf" <<'ARCH'
```
Bench-1 ──────── MinIO-1 (standalone)
Bench-2 ──────── MinIO-2 (standalone)
Bench-3 ──────── MinIO-3 (standalone)

Direct 1:1 — each bench node talks to its own MinIO.
No data sharing, no replication, no erasure coding.
```
ARCH
      ;;
    haproxy)
      cat >> "$rf" <<'ARCH'
```
Bench-1 ─┐              ┌─ MinIO-1 (standalone)
Bench-2 ──┼── HAProxy ──┤  MinIO-2 (standalone)
Bench-3 ─┘  (source)    └─ MinIO-3 (standalone)

HAProxy with balance source (sticky per client IP).
No erasure coding, no replication.
```
ARCH
      ;;
    distributed)
      cat >> "$rf" <<'ARCH'
```
Bench-1 ─┐                 ┌─ MinIO-1 (data1, data2)
Bench-2 ──┼── same bucket ──┤  MinIO-2 (data1, data2)
Bench-3 ─┘                 └─ MinIO-3 (data1, data2)

Distributed MinIO cluster — Erasure coded (EC:3)
3 nodes × 2 drives = 6 drives total
```
ARCH
      ;;
  esac

  echo "" >> "$rf"
  echo "## Results" >> "$rf"
  echo "" >> "$rf"
  echo "| Node | Verdict | Checks | PUT_s P50 | PUT_s P99 | GET_r P50 | GET_r P99 | HEAD P50 | PUT MB/s | GET MB/s |" >> "$rf"
  echo "|------|---------|--------|-----------|-----------|-----------|-----------|----------|----------|----------|" >> "$rf"

  for n in 1 2 3; do
    local nf="${RESULTS_DIR}/report-node${n}.json"
    if [[ -f "$nf" ]]; then
      local metrics
      metrics=$(python3 - "$nf" <<'PYEOF'
import json, sys
with open(sys.argv[1]) as f: data = json.load(f)
verdict = data.get("verdict", "?")
total = passed = 0
for s in data.get("scenarios", []):
    for c in s.get("checks", []):
        total += 1
        if c.get("passed"): passed += 1
ps50=ps99=gr50=gr99=h50=pmbps=gmbps="-"
for s in data.get("scenarios", []):
    for ok, st in s.get("stats", {}).items():
        if st.get("count", 0) == 0: continue
        op = st.get("op_type", ok)
        p50 = st.get("p50_ms", 0); p99 = st.get("p99_ms", 0)
        mbps = st.get("throughput_mbps", 0)
        if op == "PUT_small" and ps50 == "-":
            ps50 = f"{p50:.1f}"; ps99 = f"{p99:.1f}"
            if mbps > 0: pmbps = f"{mbps:.1f}"
        elif op == "GET_range" and gr50 == "-":
            gr50 = f"{p50:.1f}"; gr99 = f"{p99:.1f}"
            if mbps > 0: gmbps = f"{mbps:.1f}"
        elif op == "HEAD" and h50 == "-":
            h50 = f"{p50:.1f}"
print(f"{verdict}|{passed}/{total}|{ps50}|{ps99}|{gr50}|{gr99}|{h50}|{pmbps}|{gmbps}")
PYEOF
      ) || continue
      local v ck ps50 ps99 gr50 gr99 h50 pm gm
      IFS='|' read -r v ck ps50 ps99 gr50 gr99 h50 pm gm <<< "$metrics"
      echo "| ${n} | **${v}** | ${ck} | ${ps50} | ${ps99} | ${gr50} | ${gr99} | ${h50} | ${pm} | ${gm} |" >> "$rf"
    fi
  done

  echo "" >> "$rf"

  # Add failed checks detail
  echo "### Failed Checks Detail" >> "$rf"
  echo "" >> "$rf"
  for n in 1 2 3; do
    local nf="${RESULTS_DIR}/report-node${n}.json"
    if [[ -f "$nf" ]]; then
      echo "**Node ${n}:**" >> "$rf"
      python3 - "$nf" >> "$rf" <<'PYEOF'
import json, sys
with open(sys.argv[1]) as f: data = json.load(f)
for s in data.get("scenarios", []):
    for c in s.get("checks", []):
        if not c.get("passed"):
            print(f"- ❌ {c.get('name', '?')}: {c.get('detail', 'no detail')}")
PYEOF
      echo "" >> "$rf"
    fi
  done

  echo "---" >> "$rf"
  echo "Generated by ClickS3 v0.1.0 on $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$rf"

  ok "Report: ${rf}"
}

#############################################################################
main() {
  echo ""
  echo "╔══════════════════════════════════════════════════════════════════╗"
  echo "║  ClickS3 — Configuration Benchmark                            ║"
  echo "╠══════════════════════════════════════════════════════════════════╣"
  echo "║  Mode:    ${MODE_LABEL}"
  echo "║  Storage: ${STORAGE_LABEL}"
  echo "║  Bench:   ${BENCH_TYPE} × 3  (36 vCPU, 72 GB)"
  echo "║  MinIO:   ${MINIO_TYPE} × 3  (16 vCPU, 32 GB)"
  echo "║  Region:  ${REGION} (AZ: ${AZ})"
  echo "║  Duration: ${DURATION}"
  echo "╚══════════════════════════════════════════════════════════════════╝"
  echo ""

  case "$MODE" in
    direct)
      echo "  Architecture:"
      echo "    Bench-1 ──────── MinIO-1 (standalone, ${STORAGE})"
      echo "    Bench-2 ──────── MinIO-2 (standalone, ${STORAGE})"
      echo "    Bench-3 ──────── MinIO-3 (standalone, ${STORAGE})"
      echo "    Direct 1:1 — no shared storage"
      ;;
    haproxy)
      echo "  Architecture:"
      echo "    Bench-1 ─┐              ┌─ MinIO-1 (standalone, ${STORAGE})"
      echo "    Bench-2 ──┼── HAProxy ──┤  MinIO-2 (standalone, ${STORAGE})"
      echo "    Bench-3 ─┘  (source)    └─ MinIO-3 (standalone, ${STORAGE})"
      echo "    Sticky LB — no shared storage"
      ;;
    distributed)
      echo "  Architecture:"
      echo "    Bench-1 ─┐                 ┌─ MinIO-1 (data1,data2) [${STORAGE}]"
      echo "    Bench-2 ──┼── same bucket ──┤  MinIO-2 (data1,data2) [${STORAGE}]"
      echo "    Bench-3 ─┘                 └─ MinIO-3 (data1,data2) [${STORAGE}]"
      echo "    Erasure coded (EC:3) — shared bucket"
      ;;
  esac
  echo ""

  mkdir -p "$RESULTS_DIR"

  find_ami
  create_keypair
  create_network
  launch_instances
  wait_all_ssh
  setup_minio
  setup_haproxy
  deploy_clicks3
  run_benchmark
  collect_reports
  generate_report

  echo ""
  echo "╔══════════════════════════════════════════════════════════════════╗"
  echo "║   Benchmark complete — destroying infrastructure              ║"
  echo "╚══════════════════════════════════════════════════════════════════╝"
  echo ""
  echo "  Results: ${RESULTS_DIR}/"
  echo "  Report:  ${RESULTS_DIR}/RESULTS.md"
  echo ""
}

main "$@"
