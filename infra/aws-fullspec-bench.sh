#!/usr/bin/env bash
set -eo pipefail

#############################################################################
# ClickS3 — Full-Spec Benchmark (matching reference hardware)
#
# Reference hardware:
#   ClickHouse: 224 cores / 448 threads, 768 GB RAM, 4×30TB NVMe, 2×25Gb NIC
#   MinIO:      64 cores / 128 threads, 1024 GB RAM, 24×24TB HDD, 2×25Gb NIC
#
# AWS equivalents:
#   Bench:  r6i.24xlarge  (96 vCPU,  768 GiB, 37.5 Gbps net, 30 Gbps EBS)
#   MinIO:  r6i.32xlarge  (128 vCPU, 1024 GiB, 50 Gbps net, 40 Gbps EBS)
#
# Drives: 24 EBS volumes per MinIO node (matching reference 24-drive layout)
#   --storage hdd  →  24× 1TB st1  (40 MB/s baseline each)
#   --storage ssd  →  24× 200GB gp3 (3000 IOPS, 125 MB/s each)
#
# Modes:
#   --mode direct   Each bench-N talks to MinIO-N (1:1)
#   --mode haproxy  HAProxy on bench → all 3 MinIO (balance source)
#
# Usage:
#   ./infra/aws-fullspec-bench.sh --mode direct  --storage hdd --duration 5m
#   ./infra/aws-fullspec-bench.sh --mode haproxy --storage ssd --duration 5m
#
# Requires: aws CLI v2, go, python3
#############################################################################

# Unset proxy vars that can interfere with S3 requests
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy NO_PROXY no_proxy
: "${AWS_PROFILE:?AWS_PROFILE must be set (e.g. export AWS_PROFILE=your-profile)}"

REGION="${REGION:-eu-west-1}"
AZ_SUFFIX="${AZ_SUFFIX:-a}"
DURATION="${DURATION:-5m}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASS="${MINIO_PASS:-minioadmin}"
BUCKET="${BUCKET:-clicks3-test}"
MODE="${MODE:-direct}"
STORAGE="${STORAGE:-hdd}"

BENCH_TYPE="${BENCH_TYPE:-r6i.24xlarge}"     # 96 vCPU, 768 GiB — closest to reference CH
MINIO_TYPE="${MINIO_TYPE:-r6i.32xlarge}"     # 128 vCPU, 1024 GiB — reference MinIO spec
DATA_DRIVES=24                               # match reference 24-drive layout

HDD_VOL_SIZE=600    # GB per HDD volume (st1, 24×600GB×3nodes = 43.2 TiB < 50 TiB quota)
SSD_VOL_SIZE=200    # GB per SSD volume (gp3)
SSD_VOL_IOPS=3000   # per volume (default)
SSD_VOL_THROUGHPUT=125  # MB/s per volume (default)

AMI=""
RUN_ID="fullspec-$(date +%s)"
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
    --region)       REGION="$2"; shift 2 ;;
    --duration)     DURATION="$2"; shift 2 ;;
    --az)           AZ_SUFFIX="$2"; shift 2 ;;
    --mode)         MODE="$2"; shift 2 ;;
    --storage)      STORAGE="$2"; shift 2 ;;
    --bench-type)   BENCH_TYPE="$2"; shift 2 ;;
    --minio-type)   MINIO_TYPE="$2"; shift 2 ;;
    --data-drives)  DATA_DRIVES="$2"; shift 2 ;;
    *) echo "Unknown: $1"; exit 1 ;;
  esac
done

AZ="${REGION}${AZ_SUFFIX}"

case "$STORAGE" in
  hdd)  STORAGE_LABEL="HDD (EBS st1, ${DATA_DRIVES}×${HDD_VOL_SIZE}GB per node)" ;;
  ssd)  STORAGE_LABEL="SSD (EBS gp3, ${DATA_DRIVES}×${SSD_VOL_SIZE}GB, ${SSD_VOL_IOPS} IOPS each)" ;;
  *)    echo "Invalid --storage: $STORAGE (hdd|ssd)"; exit 1 ;;
esac

case "$MODE" in
  direct)  MODE_LABEL="Direct 1:1 (Bench-N → MinIO-N, ${DATA_DRIVES} drives)" ;;
  haproxy) MODE_LABEL="HAProxy (sticky LB → 3 MinIO, ${DATA_DRIVES} drives each)" ;;
  *)       echo "Invalid --mode: $MODE (direct|haproxy)"; exit 1 ;;
esac

RESULTS_DIR="${PROJECT_DIR}/results-fullspec-${MODE}-${STORAGE}-$(date +%Y-%m-%d-%H%M)"

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
    --group-name "${RUN_ID}-sg" --description "ClickS3 fullspec bench" \
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
# Generate block device mappings for N data drives
generate_minio_bdm() {
  local bdm='[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}'

  # Device letters: b through y = 24 drives max
  local letters=(b c d e f g h i j k l m n o p q r s t u v w x y)

  for (( i=0; i<DATA_DRIVES && i<24; i++ )); do
    local dev="/dev/xvd${letters[$i]}"
    case "$STORAGE" in
      hdd)
        bdm="${bdm},{\"DeviceName\":\"${dev}\",\"Ebs\":{\"VolumeSize\":${HDD_VOL_SIZE},\"VolumeType\":\"st1\",\"DeleteOnTermination\":true}}"
        ;;
      ssd)
        bdm="${bdm},{\"DeviceName\":\"${dev}\",\"Ebs\":{\"VolumeSize\":${SSD_VOL_SIZE},\"VolumeType\":\"gp3\",\"Iops\":${SSD_VOL_IOPS},\"Throughput\":${SSD_VOL_THROUGHPUT},\"DeleteOnTermination\":true}}"
        ;;
    esac
  done

  bdm="${bdm}]"
  echo "$bdm"
}

launch_instances() {
  log "Launching 3 bench nodes (${BENCH_TYPE}) + 3 MinIO nodes (${MINIO_TYPE})..."
  log "  MinIO: ${DATA_DRIVES} data drives each (${STORAGE})"

  local bench_bdm='[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]'
  local minio_bdm
  minio_bdm=$(generate_minio_bdm)

  log "  Block device mapping: $(echo "$minio_bdm" | wc -c) bytes"

  local mids
  mids=$(aws ec2 run-instances --region "$REGION" \
    --image-id "$AMI" --instance-type "$MINIO_TYPE" \
    --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET_ID" --count 3 \
    --block-device-mappings "$minio_bdm" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${RUN_ID}-minio},{Key=Role,Value=minio}]" \
    --query 'Instances[*].InstanceId' --output text)
  read -ra MINIO_IDS <<< "$mids"
  ok "MinIO instances: ${MINIO_IDS[*]}"

  local bids
  bids=$(aws ec2 run-instances --region "$REGION" \
    --image-id "$AMI" --instance-type "$BENCH_TYPE" \
    --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET_ID" --count 3 \
    --block-device-mappings "$bench_bdm" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${RUN_ID}-bench},{Key=Role,Value=bench}]" \
    --query 'Instances[*].InstanceId' --output text)
  read -ra BENCH_IDS <<< "$bids"
  ok "Bench instances: ${BENCH_IDS[*]}"

  local all_ids=("${MINIO_IDS[@]}" "${BENCH_IDS[@]}")
  log "Waiting for ${#all_ids[@]} instances to start..."
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

  ok "MinIO private IPs: ${MINIO_PRIV_IPS[*]}"
  ok "Bench public IPs:  ${BENCH_PUB_IPS[*]}"
}

#############################################################################
wait_ssh() {
  local ip="$1" max_wait=300 elapsed=0
  while ! ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${ip}" "true" 2>/dev/null; do
    sleep 5
    elapsed=$((elapsed + 5))
    [[ $elapsed -ge $max_wait ]] && { err "SSH timeout for $ip after ${max_wait}s"; return 1; }
  done
}

wait_all_ssh() {
  log "Waiting for SSH on all 6 nodes (large instances may take longer)..."
  for ip in "${MINIO_PUB_IPS[@]}" "${BENCH_PUB_IPS[@]}"; do
    wait_ssh "$ip" &
  done
  wait
  ok "All 6 nodes reachable via SSH"
}

#############################################################################
# Mount all data drives on MinIO node
prep_minio_node() {
  local ip="$1" num_drives="$2"
  ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${ip}" bash -s -- "$num_drives" <<'PREP_EOF'
set -e
NUM_DRIVES="$1"

sudo dnf install -y wget xfsprogs nvme-cli >/dev/null 2>&1

wget -q https://dl.min.io/server/minio/release/linux-amd64/minio -O /tmp/minio
chmod +x /tmp/minio
sudo mv /tmp/minio /usr/local/bin/minio

wget -q https://dl.min.io/client/mc/release/linux-amd64/mc -O /tmp/mc
chmod +x /tmp/mc
sudo mv /tmp/mc /usr/local/bin/mc

# On Nitro instances (r6i), EBS volumes appear as /dev/nvmeXn1
# Root = nvme0n1, data volumes = nvme1n1 through nvme24n1
# We identify data volumes by excluding the root device

ROOT_DEV=$(findmnt -n -o SOURCE / | sed 's/p[0-9]*$//')
echo "Root device: $ROOT_DEV"

MOUNTED=0
for i in $(seq 1 40); do
  DEV="/dev/nvme${i}n1"
  [ -b "$DEV" ] || continue

  # Skip root device
  REAL_ROOT=$(readlink -f "$ROOT_DEV" 2>/dev/null || echo "$ROOT_DEV")
  REAL_DEV=$(readlink -f "$DEV" 2>/dev/null || echo "$DEV")
  [ "$REAL_DEV" = "$REAL_ROOT" ] && continue

  MOUNTED=$((MOUNTED + 1))
  DIR="/mnt/data/disk${MOUNTED}"
  sudo mkdir -p "$DIR"
  sudo mkfs.xfs -f "$DEV" 2>/dev/null || true
  sudo mount "$DEV" "$DIR" 2>/dev/null || true
  echo "  Mounted $DEV → $DIR ($(lsblk -dn -o SIZE "$DEV" 2>/dev/null || echo '?'))"

  [ "$MOUNTED" -ge "$NUM_DRIVES" ] && break
done

sudo chown -R ec2-user:ec2-user /mnt/data

# Increase file descriptor limits
sudo tee /etc/security/limits.d/minio.conf > /dev/null <<LIMITS
ec2-user soft nofile 1048576
ec2-user hard nofile 1048576
LIMITS

# Kernel tuning for high-IOPS workload
sudo sysctl -w net.core.somaxconn=65535 >/dev/null 2>&1
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535 >/dev/null 2>&1
sudo sysctl -w net.core.netdev_max_backlog=65535 >/dev/null 2>&1
sudo sysctl -w vm.dirty_ratio=30 >/dev/null 2>&1
sudo sysctl -w vm.dirty_background_ratio=5 >/dev/null 2>&1

echo ""
echo "Node prepared: $MOUNTED drives mounted"
df -h /mnt/data/disk* 2>/dev/null | head -30
PREP_EOF
}

# Start MinIO with all N drives (standalone mode per node)
start_minio_standalone() {
  local pub="$1" muser="$2" mpass="$3" num_drives="$4"
  ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${pub}" bash -s -- "$muser" "$mpass" "$num_drives" <<'START_EOF'
set -e
MUSER="$1"; MPASS="$2"; NUM_DRIVES="$3"

cat > /tmp/minio-env <<ENVFILE
MINIO_ROOT_USER=${MUSER}
MINIO_ROOT_PASSWORD=${MPASS}
ENVFILE

# Build drive list: /mnt/data/disk{1...N}
DRIVE_SPEC="/mnt/data/disk{1...${NUM_DRIVES}}"

sudo tee /etc/systemd/system/minio.service > /dev/null <<SVC
[Unit]
Description=MinIO Standalone (${NUM_DRIVES} drives)
After=network-online.target
[Service]
User=ec2-user
Group=ec2-user
EnvironmentFile=/tmp/minio-env
ExecStart=/usr/local/bin/minio server ${DRIVE_SPEC} --address :9000 --console-address :9001
Restart=always
RestartSec=3
LimitNOFILE=1048576
[Install]
WantedBy=multi-user.target
SVC

sudo systemctl daemon-reload
sudo systemctl enable minio
sudo systemctl start minio
echo "MinIO standalone started with ${NUM_DRIVES} drives (erasure within single node)"
START_EOF
}

#############################################################################
setup_minio() {
  log "Preparing MinIO on all 3 nodes (${DATA_DRIVES} drives each)..."
  for i in 0 1 2; do
    prep_minio_node "${MINIO_PUB_IPS[$i]}" "$DATA_DRIVES" &
  done
  wait
  ok "All 3 MinIO nodes prepared"

  log "Starting 3 standalone MinIO servers (${DATA_DRIVES} drives each)..."
  for i in 0 1 2; do
    start_minio_standalone "${MINIO_PUB_IPS[$i]}" "$MINIO_USER" "$MINIO_PASS" "$DATA_DRIVES" &
  done
  wait
  ok "MinIO servers starting..."

  # HDD with 24 drives needs more startup time
  local wait_time=30
  [[ "$STORAGE" == "hdd" ]] && wait_time=60
  log "Waiting ${wait_time}s for MinIO initialization (${STORAGE}, ${DATA_DRIVES} drives)..."
  sleep "$wait_time"

  for i in 0 1 2; do
    local pub="${MINIO_PUB_IPS[$i]}" priv="${MINIO_PRIV_IPS[$i]}"
    log "  Health check MinIO-$((i+1)) (${priv}, ${DATA_DRIVES} drives)..."
    ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${pub}" bash -s -- \
      "$priv" "$MINIO_USER" "$MINIO_PASS" "$BUCKET" <<'HC_EOF'
PRIV_IP="$1"; MU="$2"; MP="$3"; BK="$4"
for attempt in $(seq 1 60); do
  if curl -sf -o /dev/null "http://${PRIV_IP}:9000/minio/health/live" 2>/dev/null; then
    echo "  Healthy on attempt $attempt"
    break
  fi
  [ "$attempt" -eq 60 ] && echo "  WARNING: health check timed out after 5 min"
  sleep 5
done
mc alias set clicks3 "http://${PRIV_IP}:9000" "$MU" "$MP" >/dev/null 2>&1
mc mb "clicks3/${BK}" --ignore-existing 2>/dev/null || echo "  Bucket may already exist"
echo "  MinIO ready: ${PRIV_IP}"
# Show drive status
mc admin info clicks3 2>/dev/null | head -20 || true
HC_EOF
  done
  ok "All MinIO servers ready (${DATA_DRIVES} drives per node, ${STORAGE})"
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
    maxconn 20000
    log stdout format raw local0

defaults
    mode http
    timeout connect 10s
    timeout client  120s
    timeout server  120s
    retries 3
    option httpchk GET /minio/health/live
    http-check expect status 200

frontend s3_frontend
    bind *:9090
    default_backend minio_backend

backend minio_backend
    balance source
    hash-type consistent
    server minio1 ${IP1}:9000 check inter 5s fall 3 rise 2 maxconn 10000
    server minio2 ${IP2}:9000 check inter 5s fall 3 rise 2 maxconn 10000
    server minio3 ${IP3}:9000 check inter 5s fall 3 rise 2 maxconn 10000

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
  ok "HAProxy configured on all bench nodes (maxconn=20000)"
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
  log "Running FULL benchmark (mode: ${MODE}, storage: ${STORAGE}, duration: ${DURATION})..."
  log "  Scenarios: compat + mixed + failures + iops (all criteria)"
  echo ""

  local pids=()
  for i in 0 1 2; do
    local bip="${BENCH_PUB_IPS[$i]}"
    local endpoint
    local node_id="fullspec-${MODE}-${STORAGE}-node$((i+1))"

    case "$MODE" in
      direct)  endpoint="http://${MINIO_PRIV_IPS[$i]}:9000" ;;
      haproxy) endpoint="http://127.0.0.1:9090" ;;
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
  --scenarios all \
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
# ClickS3 Full-Spec Benchmark — ${MODE_LABEL}

**Date:** $(date +%Y-%m-%d %H:%M)
**Run ID:** ${RUN_ID}
**Duration:** ${DURATION}

## Hardware Configuration (matching reference spec)

| Component | Instance Type | Count | vCPU | RAM | Storage | Network |
|-----------|--------------|-------|------|-----|---------|---------|
| Bench (CH proxy) | ${BENCH_TYPE} | 3 | 96 | 768 GiB | — | 37.5 Gbps |
| MinIO | ${MINIO_TYPE} | 3 | 128 (64 cores) | 1024 GiB | ${STORAGE_LABEL} | 50 Gbps |

### Reference vs AWS mapping

| Spec | Reference | AWS |
|------|----------|-----|
| CH CPU | 224 cores / 448 threads | 96 vCPU (r6i.24xlarge) |
| CH RAM | 768 GB | 768 GiB |
| CH NIC | 2×25 Gbps | 37.5 Gbps |
| MinIO CPU | 64 cores / 128 threads | 128 vCPU = 64 cores HT (r6i.32xlarge) |
| MinIO RAM | 1024 GB | 1024 GiB |
| MinIO Drives | 24× 24TB HDD | 24× ${STORAGE} EBS |
| MinIO NIC | 2×25 Gbps | 50 Gbps |

## Architecture

HEADER

  case "$MODE" in
    direct)
      cat >> "$rf" <<ARCH
\`\`\`
Bench-1 (96 vCPU) ──────── MinIO-1 (128 vCPU, ${DATA_DRIVES} drives ${STORAGE})
Bench-2 (96 vCPU) ──────── MinIO-2 (128 vCPU, ${DATA_DRIVES} drives ${STORAGE})
Bench-3 (96 vCPU) ──────── MinIO-3 (128 vCPU, ${DATA_DRIVES} drives ${STORAGE})

Direct 1:1 — each bench node talks to its own MinIO.
Each MinIO has erasure coding within its ${DATA_DRIVES} drives.
\`\`\`
ARCH
      ;;
    haproxy)
      cat >> "$rf" <<ARCH
\`\`\`
Bench-1 (96 vCPU) ─┐                  ┌─ MinIO-1 (128 vCPU, ${DATA_DRIVES} drives ${STORAGE})
Bench-2 (96 vCPU) ──┼── HAProxy(LB) ──┤  MinIO-2 (128 vCPU, ${DATA_DRIVES} drives ${STORAGE})
Bench-3 (96 vCPU) ─┘  (source hash)   └─ MinIO-3 (128 vCPU, ${DATA_DRIVES} drives ${STORAGE})

HAProxy with balance source (sticky per client IP).
Each MinIO has erasure coding within its ${DATA_DRIVES} drives.
\`\`\`
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
    for ok_key, st in s.get("stats", {}).items():
        if st.get("count", 0) == 0: continue
        op = st.get("op_type", ok_key)
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

  # IOPS results
  echo "## Peak IOPS" >> "$rf"
  echo "" >> "$rf"
  for n in 1 2 3; do
    local nf="${RESULTS_DIR}/report-node${n}.json"
    if [[ -f "$nf" ]]; then
      echo "**Node ${n}:**" >> "$rf"
      python3 - "$nf" >> "$rf" <<'PYEOF'
import json, sys
with open(sys.argv[1]) as f: data = json.load(f)
for s in data.get("scenarios", []):
    if "IOPS" not in s.get("name", ""): continue
    for key, st in s.get("stats", {}).items():
        if st.get("ops_per_sec", 0) > 0:
            print(f"- **{st.get('op_type', key)}**: {st['ops_per_sec']:.0f} IOPS (P50={st.get('p50_ms',0):.1f}ms, P99={st.get('p99_ms',0):.1f}ms)")
PYEOF
      echo "" >> "$rf"
    fi
  done

  # Failed checks detail
  echo "## Failed Checks Detail" >> "$rf"
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
            print(f"- {c.get('name', '?')}: {c.get('detail', 'no detail')}")
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
  echo "╔══════════════════════════════════════════════════════════════════════╗"
  echo "║  ClickS3 — Full-Spec Benchmark (Reference Hardware Match)         ║"
  echo "╠══════════════════════════════════════════════════════════════════════╣"
  echo "║  Mode:     ${MODE_LABEL}"
  echo "║  Storage:  ${STORAGE_LABEL}"
  echo "║  Bench:    ${BENCH_TYPE} × 3  (96 vCPU, 768 GiB each)"
  echo "║  MinIO:    ${MINIO_TYPE} × 3  (128 vCPU, 1024 GiB each)"
  echo "║  Drives:   ${DATA_DRIVES} per MinIO node"
  echo "║  Region:   ${REGION} (AZ: ${AZ})"
  echo "║  Duration: ${DURATION}"
  echo "║  Scenarios: ALL (compat + insert + merge + select + mixed + failures + iops)"
  echo "╚══════════════════════════════════════════════════════════════════════╝"
  echo ""

  case "$MODE" in
    direct)
      echo "  Architecture:"
      echo "    Bench-1 (96 vCPU) ──────── MinIO-1 (128 vCPU, ${DATA_DRIVES}× ${STORAGE})"
      echo "    Bench-2 (96 vCPU) ──────── MinIO-2 (128 vCPU, ${DATA_DRIVES}× ${STORAGE})"
      echo "    Bench-3 (96 vCPU) ──────── MinIO-3 (128 vCPU, ${DATA_DRIVES}× ${STORAGE})"
      echo "    Direct 1:1 — erasure coding within each node"
      ;;
    haproxy)
      echo "  Architecture:"
      echo "    Bench-1 ─┐                    ┌─ MinIO-1 (128 vCPU, ${DATA_DRIVES}× ${STORAGE})"
      echo "    Bench-2 ──┼── HAProxy (LB) ──┤  MinIO-2 (128 vCPU, ${DATA_DRIVES}× ${STORAGE})"
      echo "    Bench-3 ─┘   (balance src)    └─ MinIO-3 (128 vCPU, ${DATA_DRIVES}× ${STORAGE})"
      echo "    Sticky LB — erasure coding within each node"
      ;;
  esac

  # Cost estimate
  echo ""
  echo "  Estimated cost: ~\$70/hr (3×r6i.24xlarge + 3×r6i.32xlarge + EBS)"
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
  echo "╔══════════════════════════════════════════════════════════════════════╗"
  echo "║   Benchmark complete — destroying infrastructure                  ║"
  echo "╚══════════════════════════════════════════════════════════════════════╝"
  echo ""
  echo "  Results: ${RESULTS_DIR}/"
  echo "  Report:  ${RESULTS_DIR}/RESULTS.md"
  echo ""
}

main "$@"
