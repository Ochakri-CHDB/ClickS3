#!/usr/bin/env bash
set -eo pipefail

#############################################################################
# ClickS3 AWS Matrix Benchmark
#
# Modes:
#   --mode distributed   3-node MinIO cluster, erasure coded (3×2 drives = EC:3)
#   --mode standalone    3 standalone MinIO nodes + HAProxy LB (no erasure)
#
# Storage:
#   --storage nvme       NVMe SSD instance store (c5d instances) — default
#   --storage ssd        EBS gp3 SSD 500GB (c5 instances + attached gp3 volume)
#   --storage hdd        EBS st1 HDD 1TB (c5 instances + attached st1 volume)
#
# Runs 9 benchmark combinations crossing 3 bench sizes × 3 MinIO sizes.
# All 18 instances launched in parallel, benchmarks run sequentially.
# All resources destroyed at the end.
#
# Usage:
#   ./infra/aws-matrix-bench.sh --mode distributed --storage hdd --duration 3m
#
# Requires: aws CLI v2, go, python3
#############################################################################

REGION="${REGION:-eu-west-1}"
AZ_SUFFIX="${AZ_SUFFIX:-a}"
DURATION="${DURATION:-3m}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASS="${MINIO_PASS:-minioadmin}"
BUCKET="${BUCKET:-clicks3-test}"
MODE="${MODE:-distributed}"  # distributed | standalone | direct
STORAGE="${STORAGE:-nvme}"   # nvme | ssd | hdd
COMBO=""                     # e.g. "L:M" — run only this bench:minio combo instead of full 9
HDD_SIZE_GB="${HDD_SIZE_GB:-1000}"  # st1 volume size (GB) for HDD mode
AMI=""
RUN_ID="matrix-$(date +%s)"
KEY_NAME="${RUN_ID}-key"
KEY_FILE="/tmp/${KEY_NAME}.pem"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

SIZE_LABELS=("S" "M" "L")
SIZE_VCPU=("8" "16" "36")
BENCH_TYPES=("c5.2xlarge" "c5.4xlarge" "c5.9xlarge")
MINIO_TYPES=("c5d.2xlarge" "c5d.4xlarge" "c5d.9xlarge")

VPC_ID="" ; SUBNET_ID="" ; IGW_ID="" ; RTB_ID="" ; SG_ID=""
ALL_INSTANCE_IDS=""

MINIO_PUB_IPS=()    # 9 entries: [S0 S1 S2 M0 M1 M2 L0 L1 L2]
MINIO_PRIV_IPS=()
BENCH_PUB_IPS=()

log()  { echo -e "\033[1;34m[matrix]\033[0m $*"; }
ok()   { echo -e "\033[1;32m[  OK  ]\033[0m $*"; }
err()  { echo -e "\033[1;31m[ERROR]\033[0m $*"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region)   REGION="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --az)       AZ_SUFFIX="$2"; shift 2 ;;
    --mode)     MODE="$2"; shift 2 ;;
    --storage)  STORAGE="$2"; shift 2 ;;
    --combo)    COMBO="$2"; shift 2 ;;
    *) echo "Unknown: $1"; exit 1 ;;
  esac
done

AZ="${REGION}${AZ_SUFFIX}"

# Storage mode: select MinIO instance types and labels
case "$STORAGE" in
  hdd)
    MINIO_TYPES=("c5.2xlarge" "c5.4xlarge" "c5.9xlarge")
    STORAGE_LABEL="hdd-st1"
    ;;
  ssd)
    MINIO_TYPES=("c5.2xlarge" "c5.4xlarge" "c5.9xlarge")
    STORAGE_LABEL="ssd-gp3"
    ;;
  *)
    STORAGE_LABEL="nvme"
    ;;
esac

RESULTS_DIR="${PROJECT_DIR}/results-matrix-${MODE}-${STORAGE_LABEL}-$(date +%Y-%m-%d)"

#############################################################################
cleanup() {
  log "Destroying all AWS resources (${RUN_ID})..."
  if [[ -n "$ALL_INSTANCE_IDS" ]]; then
    log "Terminating instances..."
    aws ec2 terminate-instances --region "$REGION" \
      --instance-ids $ALL_INSTANCE_IDS >/dev/null 2>&1 || true
    aws ec2 wait instance-terminated --region "$REGION" \
      --instance-ids $ALL_INSTANCE_IDS 2>/dev/null || true
    ok "Instances terminated"
  fi
  if [[ -n "$KEY_NAME" ]]; then
    aws ec2 delete-key-pair --region "$REGION" --key-name "$KEY_NAME" 2>/dev/null || true
    rm -f "$KEY_FILE"
  fi
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
  log "Creating VPC..."
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
    --group-name "${RUN_ID}-sg" --description "ClickS3 matrix bench" \
    --vpc-id "$VPC_ID" --query 'GroupId' --output text)
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG_ID" \
    --protocol tcp --port 22 --cidr 0.0.0.0/0 >/dev/null
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG_ID" \
    --protocol -1 --cidr 10.0.0.0/16 >/dev/null
  ok "Network: VPC=$VPC_ID  Subnet=$SUBNET_ID  SG=$SG_ID"
}

create_keypair() {
  log "Creating key pair ${KEY_NAME}..."
  aws ec2 create-key-pair --region "$REGION" \
    --key-name "$KEY_NAME" --query 'KeyMaterial' --output text > "$KEY_FILE"
  chmod 600 "$KEY_FILE"
  ok "Key: $KEY_FILE"
}

#############################################################################
launch_all_instances() {
  log "Launching 18 instances (3 sizes × 2 roles × 3 nodes)..."

  for si in 0 1 2; do
    local sz="${SIZE_LABELS[$si]}"
    local mtype="${MINIO_TYPES[$si]}"
    local btype="${BENCH_TYPES[$si]}"
    local bdm='[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]'

    # Storage mode: attach appropriate EBS volume or rely on instance store
    local minio_bdm="$bdm"
    if [[ "$STORAGE" == "hdd" ]]; then
      minio_bdm='[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}},{"DeviceName":"/dev/xvdb","Ebs":{"VolumeSize":'"${HDD_SIZE_GB}"',"VolumeType":"st1","DeleteOnTermination":true}}]'
    elif [[ "$STORAGE" == "ssd" ]]; then
      minio_bdm='[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}},{"DeviceName":"/dev/xvdb","Ebs":{"VolumeSize":500,"VolumeType":"gp3","Iops":3000,"Throughput":125,"DeleteOnTermination":true}}]'
    fi

    log "  MinIO-${sz}: ${mtype} × 3 (${MODE}, ${STORAGE_LABEL})"
    local mids
    mids=$(aws ec2 run-instances --region "$REGION" \
      --image-id "$AMI" --instance-type "$mtype" \
      --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
      --subnet-id "$SUBNET_ID" --count 3 \
      --block-device-mappings "$minio_bdm" \
      --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${RUN_ID}-minio-${sz}},{Key=Role,Value=minio-${sz}}]" \
      --query 'Instances[*].InstanceId' --output text)
    ALL_INSTANCE_IDS+=" $mids"
    eval "MINIO_IDS_${si}=\"$mids\""

    log "  Bench-${sz}: ${btype} × 3"
    local bids
    bids=$(aws ec2 run-instances --region "$REGION" \
      --image-id "$AMI" --instance-type "$btype" \
      --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
      --subnet-id "$SUBNET_ID" --count 3 \
      --block-device-mappings "$bdm" \
      --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${RUN_ID}-bench-${sz}},{Key=Role,Value=bench-${sz}}]" \
      --query 'Instances[*].InstanceId' --output text)
    ALL_INSTANCE_IDS+=" $bids"
    eval "BENCH_IDS_${si}=\"$bids\""
  done

  ALL_INSTANCE_IDS=$(echo "$ALL_INSTANCE_IDS" | xargs)
  log "Waiting for all 18 instances..."
  aws ec2 wait instance-running --region "$REGION" --instance-ids $ALL_INSTANCE_IDS

  for si in 0 1 2; do
    local mids_var="MINIO_IDS_${si}"
    local bids_var="BENCH_IDS_${si}"
    local sz="${SIZE_LABELS[$si]}"

    for id in ${!mids_var}; do
      local pub priv
      pub=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
      priv=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
      MINIO_PUB_IPS+=("$pub")
      MINIO_PRIV_IPS+=("$priv")
    done

    for id in ${!bids_var}; do
      local pub
      pub=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
      BENCH_PUB_IPS+=("$pub")
    done

    local base=$((si * 3))
    ok "MinIO-${sz}: [${MINIO_PRIV_IPS[$base]}, ${MINIO_PRIV_IPS[$((base+1))]}, ${MINIO_PRIV_IPS[$((base+2))]}]"
    ok "Bench-${sz}: [${BENCH_PUB_IPS[$base]}, ${BENCH_PUB_IPS[$((base+1))]}, ${BENCH_PUB_IPS[$((base+2))]}]"
  done
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
  log "Waiting for SSH on all 18 nodes..."
  for ip in "${MINIO_PUB_IPS[@]}" "${BENCH_PUB_IPS[@]}"; do
    wait_ssh "$ip" &
  done
  wait
  ok "All nodes reachable via SSH"
}

#############################################################################
# Prepare a MinIO node: install binary + format NVMe
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

# Find data drive: NVMe instance store or EBS volume (st1/gp3)
DATA_DEV=""
for dev in /dev/nvme1n1 /dev/nvme2n1 /dev/xvdb; do
  [ -b "$dev" ] && { DATA_DEV="$dev"; break; }
done

sudo mkdir -p /mnt/data
if [ -n "$DATA_DEV" ]; then
  EXISTING=$(findmnt -n -o TARGET "$DATA_DEV" 2>/dev/null || true)
  [ -n "$EXISTING" ] && sudo umount "$EXISTING" 2>/dev/null || true
  sudo mkfs.xfs -f "$DATA_DEV" 2>/dev/null || true
  sudo mount "$DATA_DEV" /mnt/data 2>/dev/null || true
fi

sudo mkdir -p /mnt/data/data /mnt/data/data1 /mnt/data/data2
sudo chown -R ec2-user:ec2-user /mnt/data

# Identify storage type for logging
DRIVE_TYPE="unknown"
if [ -n "$DATA_DEV" ]; then
  ROTATIONAL=$(cat /sys/block/$(basename "$DATA_DEV")/queue/rotational 2>/dev/null || echo "?")
  if [ "$ROTATIONAL" = "1" ]; then
    DRIVE_TYPE="HDD"
  elif [ "$ROTATIONAL" = "0" ]; then
    DRIVE_TYPE="SSD/NVMe"
  fi
fi
echo "Node prepared: ${DATA_DEV} (${DRIVE_TYPE}) mounted at /mnt/data"
PREP_EOF
}

#############################################################################
# MODE: standalone — each MinIO runs independently on 1 drive, no erasure
start_minio_standalone() {
  local pub="$1" muser="$2" mpass="$3"
  ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${pub}" bash -s -- "$muser" "$mpass" <<'START_EOF'
set -e
MUSER="$1"; MPASS="$2"

cat > /tmp/minio-env <<ENVFILE
MINIO_ROOT_USER=${MUSER}
MINIO_ROOT_PASSWORD=${MPASS}
ENVFILE

sudo tee /etc/systemd/system/minio.service > /dev/null <<SVC
[Unit]
Description=MinIO Standalone
After=network-online.target
[Service]
User=ec2-user
Group=ec2-user
EnvironmentFile=/tmp/minio-env
ExecStart=/usr/local/bin/minio server /mnt/data/data --address :9000 --console-address :9001
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

# MODE: distributed — 3-node cluster with erasure coding
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
setup_all_minio() {
  log "Preparing MinIO binaries and NVMe on all 9 nodes..."
  for i in $(seq 0 8); do
    prep_minio_node "${MINIO_PUB_IPS[$i]}" &
  done
  wait
  ok "All 9 MinIO nodes prepared"

  if [[ "$MODE" == "standalone" || "$MODE" == "direct" ]]; then
    log "Starting ${NUM_MINIO} standalone MinIO servers (no erasure coding)..."
    for i in $(seq 0 $((NUM_MINIO - 1))); do
      start_minio_standalone "${MINIO_PUB_IPS[$i]}" "$MINIO_USER" "$MINIO_PASS" &
    done
    wait
    ok "All ${NUM_MINIO} standalone MinIO servers started"
    sleep 15
  else
    log "Starting 3 distributed MinIO clusters (erasure coded, 3×2=6 drives)..."
    for si in 0 1 2; do
      local base=$((si * 3))
      local sz="${SIZE_LABELS[$si]}"
      local ip1="${MINIO_PRIV_IPS[$base]}"
      local ip2="${MINIO_PRIV_IPS[$((base+1))]}"
      local ip3="${MINIO_PRIV_IPS[$((base+2))]}"
      log "  MinIO-${sz} cluster: ${ip1}, ${ip2}, ${ip3}"
      for ni in 0 1 2; do
        start_minio_distributed "${MINIO_PUB_IPS[$((base+ni))]}" "$ip1" "$ip2" "$ip3" "$MINIO_USER" "$MINIO_PASS" &
      done
    done
    wait
    ok "All 3 clusters starting..."
    sleep 30
  fi

  # Health check + create bucket on node 1 of each cluster/size
  for si in 0 1 2; do
    local base=$((si * 3))
    local sz="${SIZE_LABELS[$si]}"

    for ni in 0 1 2; do
      local pub="${MINIO_PUB_IPS[$((base+ni))]}"
      local priv="${MINIO_PRIV_IPS[$((base+ni))]}"
      local health_path="live"
      [[ "$MODE" == "distributed" ]] && health_path="cluster"

      log "  Health check MinIO-${sz} node $((ni+1)) (${priv})..."
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
mc mb "clicks3/${BK}" --ignore-existing 2>/dev/null || true
echo "  MinIO ready: ${PRIV_IP}"
HC_EOF
    done
  done

  ok "All MinIO servers ready (mode: ${MODE})"
}

#############################################################################
# Install HAProxy on each bench node — load-balances to all 3 MinIO nodes of a given size
# Uses balance source (sticky by client IP) for consistency
setup_haproxy_on_bench() {
  local msi="$1"  # minio size index for this combo
  local minio_base=$((msi * 3))
  local ip1="${MINIO_PRIV_IPS[$minio_base]}"
  local ip2="${MINIO_PRIV_IPS[$((minio_base+1))]}"
  local ip3="${MINIO_PRIV_IPS[$((minio_base+2))]}"
  local msz="${SIZE_LABELS[$msi]}"

  log "  Setting up HAProxy on bench nodes → MinIO-${msz} [${ip1}, ${ip2}, ${ip3}]..."

  for bip in "$@"; do
    [[ "$bip" == "$msi" ]] && continue  # skip first arg (msi)
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
}

#############################################################################
deploy_clicks3() {
  log "Cross-compiling clicks3 for linux/amd64..."
  cd "$PROJECT_DIR"
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-s -w" -o /tmp/clicks3-linux .
  ok "Binary: $(ls -lh /tmp/clicks3-linux | awk '{print $5}')"

  log "Deploying to all 9 bench nodes..."
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
clean_bucket() {
  local msi="$1"
  local base=$((msi * 3))

  if [[ "$MODE" == "standalone" ]]; then
    # Clean all 3 standalone MinIO nodes
    for ni in 0 1 2; do
      local pub="${MINIO_PUB_IPS[$((base+ni))]}"
      local priv="${MINIO_PRIV_IPS[$((base+ni))]}"
      ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${pub}" bash -s -- \
        "$priv" "$MINIO_USER" "$MINIO_PASS" "$BUCKET" <<'CLEAN_EOF' &
mc alias set clicks3 "http://$1:9000" "$2" "$3" >/dev/null 2>&1
mc rm --recursive --force "clicks3/$4/" >/dev/null 2>&1 || true
CLEAN_EOF
    done
    wait
  else
    # Distributed: clean via node 1
    local pub="${MINIO_PUB_IPS[$base]}"
    local priv="${MINIO_PRIV_IPS[$base]}"
    ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${pub}" bash -s -- \
      "$priv" "$MINIO_USER" "$MINIO_PASS" "$BUCKET" <<'CLEAN_EOF'
mc alias set clicks3 "http://$1:9000" "$2" "$3" >/dev/null 2>&1
mc rm --recursive --force "clicks3/$4/" >/dev/null 2>&1 || true
CLEAN_EOF
  fi
  log "  Bucket cleaned"
}

#############################################################################
run_combo() {
  local bsi="$1" msi="$2" combo="$3"
  local bsz="${SIZE_LABELS[$bsi]}" msz="${SIZE_LABELS[$msi]}"
  local combo_dir="${RESULTS_DIR}/bench-${bsz}_minio-${msz}"
  mkdir -p "$combo_dir"

  local bench_base=$((bsi * 3))
  local minio_base=$((msi * 3))

  local mode_label="direct 1:1"
  [[ "$MODE" == "standalone" ]] && mode_label="HAProxy → standalone"
  [[ "$MODE" == "distributed" ]] && mode_label="direct → distributed cluster"
  [[ "$MODE" == "direct" ]] && mode_label="direct 1:1 (each bench → its own MinIO)"

  echo ""
  echo "  ┌──────────────────────────────────────────────────────────────"
  echo "  │  COMBO ${combo}/9: Bench-${bsz} (${BENCH_TYPES[$bsi]}, ${SIZE_VCPU[$bsi]} vCPU)"
  echo "  │              × MinIO-${msz} (${MINIO_TYPES[$msi]}, ${SIZE_VCPU[$msi]} vCPU)"
  echo "  │  Mode: ${mode_label}"
  echo "  │  Duration: ${DURATION}"
  echo "  └──────────────────────────────────────────────────────────────"

  clean_bucket "$msi"

  if [[ "$MODE" == "standalone" ]]; then
    # Setup HAProxy on all 3 bench nodes for this combo's MinIO size
    local bench_ips=()
    for ni in 0 1 2; do
      bench_ips+=("${BENCH_PUB_IPS[$((bench_base + ni))]}")
    done
    setup_haproxy_on_bench "$msi" "${bench_ips[@]}"
    sleep 3
  fi

  local pids=()
  for ni in 0 1 2; do
    local bip="${BENCH_PUB_IPS[$((bench_base + ni))]}"
    local endpoint
    local node_id="B${bsz}-M${msz}-node$((ni+1))"

    if [[ "$MODE" == "standalone" ]]; then
      # Bench → localhost:9090 (HAProxy) → MinIO cluster
      endpoint="http://127.0.0.1:9090"
    elif [[ "$MODE" == "direct" ]]; then
      # Bench-N → MinIO-N (1:1 direct mapping, no shared storage)
      endpoint="http://${MINIO_PRIV_IPS[$((minio_base + ni))]}:9000"
    else
      # Distributed: Bench → direct to MinIO cluster node (shared storage via EC)
      endpoint="http://${MINIO_PRIV_IPS[$((minio_base + ni))]}:9000"
    fi

    log "  Node $((ni+1)): ${bip} → ${endpoint}"
    ssh $SSH_OPTS -i "$KEY_FILE" "ec2-user@${bip}" bash -s <<RUN_EOF &
set -e
/tmp/clicks3 \
  --endpoint "${endpoint}" \
  --access-key "${MINIO_USER}" \
  --secret-key "${MINIO_PASS}" \
  --bucket "${BUCKET}" \
  --prefix mergetree/ \
  --role "node$((ni+1))" \
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

  for ni in 0 1 2; do
    local bip="${BENCH_PUB_IPS[$((bench_base + ni))]}"
    scp $SSH_OPTS -i "$KEY_FILE" \
      "ec2-user@${bip}:/tmp/report.json" \
      "${combo_dir}/report-node$((ni+1)).json" 2>/dev/null || true
    scp $SSH_OPTS -i "$KEY_FILE" \
      "ec2-user@${bip}:/tmp/clicks3.log" \
      "${combo_dir}/log-node$((ni+1)).txt" 2>/dev/null || true
  done

  if [[ $failed -gt 0 ]]; then
    err "  Combo ${combo}: ${failed}/3 nodes failed"
  else
    ok "  Combo ${combo} complete: Bench-${bsz} × MinIO-${msz}"
  fi
}

#############################################################################
extract_metrics() {
  local json_file="$1"
  python3 - "$json_file" <<'PYEOF'
import json, sys
try:
    with open(sys.argv[1]) as f:
        data = json.load(f)
except:
    print("?|0/0|-|-|-|-|-|-|-|-|-|-|-|-")
    sys.exit(0)

verdict = data.get("verdict", "?")
scenarios = data.get("scenarios", [])
total_checks = passed_checks = 0
for s in scenarios:
    for c in s.get("checks", []):
        total_checks += 1
        if c.get("passed"):
            passed_checks += 1

ps50 = ps99 = pl50 = pl99 = gr50 = gr99 = gf50 = gf99 = h50 = h99 = pmbps = gmbps = "-"
for s in scenarios:
    for op_key, st in s.get("stats", {}).items():
        if st.get("count", 0) == 0:
            continue
        op = st.get("op_type", op_key)
        p50 = st.get("p50_ms", 0)
        p99 = st.get("p99_ms", 0)
        mbps = st.get("throughput_mbps", 0)
        if op == "PUT_small" and ps50 == "-":
            ps50, ps99 = f"{p50:.1f}", f"{p99:.1f}"
            if mbps > 0: pmbps = f"{mbps:.1f}"
        elif op == "PUT_large" and pl50 == "-":
            pl50, pl99 = f"{p50:.1f}", f"{p99:.1f}"
            if mbps > 0: pmbps = f"{mbps:.1f}"
        elif op == "GET_range" and gr50 == "-":
            gr50, gr99 = f"{p50:.1f}", f"{p99:.1f}"
            if mbps > 0: gmbps = f"{mbps:.1f}"
        elif op == "GET_full_small" and gf50 == "-":
            gf50, gf99 = f"{p50:.1f}", f"{p99:.1f}"
        elif op == "HEAD" and h50 == "-":
            h50, h99 = f"{p50:.1f}", f"{p99:.1f}"

print(f"{verdict}|{passed_checks}/{total_checks}|{ps50}|{ps99}|{pl50}|{pl99}|{gr50}|{gr99}|{gf50}|{gf99}|{h50}|{h99}|{pmbps}|{gmbps}")
PYEOF
}

#############################################################################
generate_matrix_report() {
  log "Generating matrix comparison report..."

  local mode_title mode_desc storage_desc
  case "$STORAGE" in
    hdd) storage_desc="EBS st1 HDD (${HDD_SIZE_GB} GB, throughput-optimized)" ;;
    ssd) storage_desc="EBS gp3 SSD (500 GB, 3000 IOPS)" ;;
    *)   storage_desc="NVMe SSD (instance store)" ;;
  esac

  if [[ "$MODE" == "standalone" ]]; then
    mode_title="Standalone MinIO + HAProxy — ${storage_desc}"
    mode_desc="- **MinIO mode:** Standalone (1 drive per server, no erasure coding, no replication)
- **Storage:** ${storage_desc}
- **Load balancer:** HAProxy on each bench node (\`balance source\`, sticky by client IP)
- **No write amplification:** Each PUT writes once to one server"
  else
    mode_title="Distributed MinIO Cluster — ${storage_desc}"
    mode_desc="- **MinIO mode:** Distributed cluster (3 nodes × 2 drives = 6 drives)
- **Storage:** ${storage_desc}
- **Erasure coding:** EC:3 (3 data + 3 parity shards)
- **Bench → MinIO:** Each bench node enters via a different cluster node
- **Data sharing:** All 3 bench nodes read/write the SAME bucket"
  fi

  local rf="${RESULTS_DIR}/MATRIX-RESULTS.md"
  cat > "$rf" <<HEADER
# ClickS3 Matrix Benchmark — ${mode_title} — AWS (${REGION})

**Date:** $(date +%Y-%m-%d)
**Run ID:** ${RUN_ID}
**Duration:** ${DURATION} per scenario
**Mode:** ${MODE}

## Architecture

${mode_desc}

---

## Instance Sizes

| Size | Bench Instance | MinIO Instance | Storage | vCPU | RAM |
|------|---------------|----------------|---------|------|-----|
| S | ${BENCH_TYPES[0]} | ${MINIO_TYPES[0]} | ${storage_desc} | 8 | 16 GB |
| M | ${BENCH_TYPES[1]} | ${MINIO_TYPES[1]} | ${storage_desc} | 16 | 32 GB |
| L | ${BENCH_TYPES[2]} | ${MINIO_TYPES[2]} | ${storage_desc} | 36 | 72 GB |

---

## Results Matrix (3 nodes per combination)

### Verdict Matrix

| Bench \\ MinIO | MinIO-S (8 vCPU) | MinIO-M (16 vCPU) | MinIO-L (36 vCPU) |
|---------------|------------------|--------------------|--------------------|
HEADER

  for bsi in 0 1 2; do
    local bsz="${SIZE_LABELS[$bsi]}"
    local line="| Bench-${bsz} (${SIZE_VCPU[$bsi]} vCPU) |"
    for msi in 0 1 2; do
      local msz="${SIZE_LABELS[$msi]}"
      local cdir="${RESULTS_DIR}/bench-${bsz}_minio-${msz}"
      if [[ -f "${cdir}/report-node1.json" ]]; then
        local m v ck
        m=$(extract_metrics "${cdir}/report-node1.json")
        v=$(echo "$m" | cut -d'|' -f1)
        ck=$(echo "$m" | cut -d'|' -f2)
        line+=" **${v}** (${ck}) |"
      else
        line+=" N/A |"
      fi
    done
    echo "$line" >> "$rf"
  done

  for metric_label in "GET Range Latency P50 (ms)" "PUT Small Latency P50 (ms)" "HEAD Latency P50 (ms)" "PUT Throughput (MB/s)" "GET Throughput (MB/s)"; do
    local field_num
    case "$metric_label" in
      "GET Range"*) field_num=7 ;;
      "PUT Small"*) field_num=3 ;;
      "HEAD"*)      field_num=11 ;;
      "PUT Thro"*)  field_num=13 ;;
      "GET Thro"*)  field_num=14 ;;
    esac

    cat >> "$rf" <<TABLE_HDR

### ${metric_label}

| Bench \\ MinIO | MinIO-S | MinIO-M | MinIO-L |
|---------------|---------|---------|---------|
TABLE_HDR

    for bsi in 0 1 2; do
      local bsz="${SIZE_LABELS[$bsi]}"
      local line="| Bench-${bsz} |"
      for msi in 0 1 2; do
        local msz="${SIZE_LABELS[$msi]}"
        local cdir="${RESULTS_DIR}/bench-${bsz}_minio-${msz}"
        if [[ -f "${cdir}/report-node1.json" ]]; then
          local m val
          m=$(extract_metrics "${cdir}/report-node1.json")
          val=$(echo "$m" | cut -d'|' -f${field_num})
          line+=" ${val} |"
        else
          line+=" - |"
        fi
      done
      echo "$line" >> "$rf"
    done
  done

  cat >> "$rf" <<'DETAIL_HDR'

---

### Detailed Per-Combination Results

DETAIL_HDR

  for bsi in 0 1 2; do
    for msi in 0 1 2; do
      local bsz="${SIZE_LABELS[$bsi]}" msz="${SIZE_LABELS[$msi]}"
      local cdir="${RESULTS_DIR}/bench-${bsz}_minio-${msz}"
      echo "#### Bench-${bsz} (${BENCH_TYPES[$bsi]}) x MinIO-${msz} (${MINIO_TYPES[$msi]})" >> "$rf"
      echo "" >> "$rf"

      if [[ ! -f "${cdir}/report-node1.json" ]]; then
        echo "*No results*" >> "$rf"
        echo "" >> "$rf"
        continue
      fi

      echo "| Node | Verdict | Checks | PUT_s P50 | PUT_l P50 | GET_r P50 | GET_f P50 | HEAD P50 | PUT MB/s | GET MB/s |" >> "$rf"
      echo "|------|---------|--------|-----------|-----------|-----------|-----------|----------|----------|----------|" >> "$rf"

      for n in 1 2 3; do
        local nf="${cdir}/report-node${n}.json"
        if [[ -f "$nf" ]]; then
          local m v ck ps pl gr gf h pm gm
          m=$(extract_metrics "$nf")
          v=$(echo "$m" | cut -d'|' -f1); ck=$(echo "$m" | cut -d'|' -f2)
          ps=$(echo "$m" | cut -d'|' -f3); pl=$(echo "$m" | cut -d'|' -f5)
          gr=$(echo "$m" | cut -d'|' -f7); gf=$(echo "$m" | cut -d'|' -f9)
          h=$(echo "$m" | cut -d'|' -f11); pm=$(echo "$m" | cut -d'|' -f13)
          gm=$(echo "$m" | cut -d'|' -f14)
          echo "| ${n} | ${v} | ${ck} | ${ps} | ${pl} | ${gr} | ${gf} | ${h} | ${pm} | ${gm} |" >> "$rf"
        fi
      done
      echo "" >> "$rf"
    done
  done

  echo "---" >> "$rf"
  echo "" >> "$rf"
  echo "Generated by ClickS3 v0.1.0 on $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$rf"
  ok "Matrix report: ${rf}"
}

#############################################################################
main() {
  local mode_banner storage_banner
  if [[ "$MODE" == "standalone" ]]; then
    mode_banner="Standalone MinIO + HAProxy LB (no erasure)"
  else
    mode_banner="Distributed MinIO cluster (erasure coded, EC:3)"
  fi
  case "$STORAGE" in
    hdd) storage_banner="EBS st1 HDD (${HDD_SIZE_GB} GB)" ;;
    ssd) storage_banner="EBS gp3 SSD (500 GB, 3000 IOPS)" ;;
    *)   storage_banner="NVMe SSD (instance store)" ;;
  esac

  echo ""
  echo "╔══════════════════════════════════════════════════════════════╗"
  echo "║  ClickS3 — AWS Matrix Benchmark                           ║"
  echo "║  Mode:    ${mode_banner}"
  echo "║  Storage: ${storage_banner}"
  echo "╚══════════════════════════════════════════════════════════════╝"
  echo ""

  if [[ "$MODE" == "standalone" ]]; then
    echo "  Architecture:"
    echo "    Bench-1 ─┐              ┌─ MinIO-1 (standalone, ${STORAGE})"
    echo "    Bench-2 ──┼── HAProxy ──┤  MinIO-2 (standalone, ${STORAGE})"
    echo "    Bench-3 ─┘  (source)    └─ MinIO-3 (standalone, ${STORAGE})"
    echo "                 No erasure, no replication"
  else
    echo "  Architecture:"
    echo "    Bench-1 ─┐                 ┌─ MinIO-1 (data1, data2) [${STORAGE}]"
    echo "    Bench-2 ──┼── same bucket ──┤  MinIO-2 (data1, data2) [${STORAGE}]"
    echo "    Bench-3 ─┘                 └─ MinIO-3 (data1, data2) [${STORAGE}]"
    echo "                     Erasure coded (EC:3)"
  fi

  echo ""
  echo "  Region:     ${REGION} (AZ: ${AZ})"
  echo "  Duration:   ${DURATION} per scenario"
  echo "  Run ID:     ${RUN_ID}"
  echo ""
  echo "  ┌──────────────────────────────────────────────────────────────┐"
  echo "  │  Size │ Bench Instance   │ MinIO Instance   │ Storage    │"
  echo "  ├──────────────────────────────────────────────────────────────┤"
  echo "  │  S    │ ${BENCH_TYPES[0]}   │ ${MINIO_TYPES[0]}  │ ${storage_banner} │"
  echo "  │  M    │ ${BENCH_TYPES[1]}   │ ${MINIO_TYPES[1]}  │ ${storage_banner} │"
  echo "  │  L    │ ${BENCH_TYPES[2]}   │ ${MINIO_TYPES[2]}  │ ${storage_banner} │"
  echo "  └──────────────────────────────────────────────────────────────┘"
  echo ""

  mkdir -p "$RESULTS_DIR"

  find_ami
  create_keypair
  create_network
  launch_all_instances
  wait_all_ssh
  setup_all_minio
  deploy_clicks3

  local combo=0
  for bsi in 0 1 2; do
    for msi in 0 1 2; do
      combo=$((combo + 1))
      run_combo "$bsi" "$msi" "$combo"
    done
  done

  generate_matrix_report

  echo ""
  echo "╔══════════════════════════════════════════════════════════════╗"
  echo "║   Matrix benchmark complete — destroying infrastructure    ║"
  echo "╚══════════════════════════════════════════════════════════════╝"
  echo ""
  echo "  Results: ${RESULTS_DIR}/"
  echo "  Report:  ${RESULTS_DIR}/MATRIX-RESULTS.md"
  echo ""
}

main "$@"
