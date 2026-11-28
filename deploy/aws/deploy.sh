#!/usr/bin/env bash
# ==============================================================================
# deploy.sh — Aegis-FDS AWS 원클릭 프로덕션 빌드 및 배포 자동화 스크립트
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT"

echo "=================================================================="
echo ">>> [1/6] 환경 변수(.env) 및 보안 인증 파일 사전 검증"
echo "=================================================================="
if [ ! -f ".env" ]; then
    echo "[ERROR] 프로젝트 루트에 .env 파일이 없습니다!"
    echo "       .env.example을 복사하여 필수 키를 채운 뒤 다시 실행하세요."
    echo "       cp .env.example .env"
    exit 1
fi

check_var() {
    local key="$1"
    if ! grep -q "^${key}=" .env; then
        echo "[ERROR] .env 파일에 ${key} 항목이 정의되어 있지 않습니다!"
        exit 1
    fi
    local val
    val="$(grep "^${key}=" .env | cut -d'=' -f2- | tr -d '\r' | tr -d ' ')"
    if [ -z "$val" ]; then
        echo "[ERROR] .env 파일의 ${key} 값이 비어 있습니다!"
        exit 1
    fi
    if [[ "$val" =~ ^YOUR_ ]]; then
        echo "[ERROR] .env 파일의 ${key} 항목에 플레이스홀더('${val}')가 남아 있습니다. 실제 비밀키/비밀번호로 변경해 주세요!"
        exit 1
    fi
}
check_var "GATEWAY_API_KEY"
check_var "ADMIN_API_KEY"
check_var "CLICKHOUSE_PASSWORD"
check_var "ADMIN_CONSOLE_PASSWORD"
check_var "ALCHEMY_WSS_URL"
check_var "MONGO_URI"
echo "환경 변수 필수 보안 검증 완료."

# Basic Auth (.htpasswd) 생성 (Nginx /admin 이중 보안 잠금)
HTPASSWD_FILE="$SCRIPT_DIR/.htpasswd"
if [ ! -f "$HTPASSWD_FILE" ]; then
    ADMIN_USER="admin"
    ADMIN_PASS="$(awk -F= '/^ADMIN_CONSOLE_PASSWORD/{print $2}' .env | tr -d '\r ')"
    echo "Nginx /admin 2중 보안 Basic Auth 생성 중 (사용자: $ADMIN_USER)..."
    htpasswd -bc "$HTPASSWD_FILE" "$ADMIN_USER" "$ADMIN_PASS"
fi

echo "=================================================================="
echo ">>> [2/6] 웹 프론트엔드 프로덕션 정적 애셋 빌드 (npm run build)"
echo "=================================================================="
cd "$PROJECT_ROOT/web"
if [ ! -d "node_modules" ]; then
    echo "node_modules 설치 중..."
    npm ci
fi

export VITE_API_BASE="/"
export VITE_PERIOD_PROFILE="${VITE_PERIOD_PROFILE:-demo}"
npm run build
echo "프론트엔드 프로덕션 빌드 완료 (web/dist 생성)."

cd "$PROJECT_ROOT"

echo "=================================================================="
echo ">>> [3/6] Docker 컨테이너 이미지 빌드 및 스택 구동"
echo "=================================================================="
docker compose -f docker-compose.aws.yml build
docker compose -f docker-compose.aws.yml up -d

echo "=================================================================="
echo ">>> [4/6] 코어 인프라 컨테이너 Healthcheck 대기 (최대 60초)"
echo "=================================================================="
wait_healthy() {
    local container="$1"
    echo -n "[$container] 헬스체크 대기 중..."
    local count=0
    until [ "$(docker inspect -f '{{.State.Health.Status}}' "$container" 2>/dev/null || true)" == "healthy" ]; do
        sleep 2
        count=$((count + 2))
        echo -n "."
        if [ $count -ge 60 ]; then
            echo -e "\n[WARNING] $container 헬스체크 제한시간(60초) 초과. 상태 확인 필요."
            return 1
        fi
    done
    echo " 정상(Healthy)!"
}

wait_healthy "aegis-redis"
wait_healthy "aegis-mongo"
wait_healthy "aegis-kafka"

echo "=================================================================="
echo ">>> [5/6] Kafka 필수 토픽 생성 및 직원 API 화이트리스트 시딩"
echo "=================================================================="
create_topic() {
    local topic="$1"
    docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create --if-not-exists \
        --topic "$topic" --partitions 1 --replication-factor 1 > /dev/null 2>&1 || true
}
create_topic "transfer.events.v1"
create_topic "transfer.rejected.v1"
create_topic "fds.holds.v1"
create_topic "fds.alerts.v1"
create_topic "fds.freezes.v1"
echo "Kafka 5대 코어 토픽 준비 완료."

# 직원 API를 통한 화이트리스트 안전 시딩 (Redis 직접 쓰기 금지 원칙 준수)
if [ -f "whitelist_seed.csv" ]; then
    echo "직원 API (POST /api/v1/admin/whitelist) 호출로 화이트리스트 등록 중..."
    python3 seed_whitelist_api.py whitelist_seed.csv --url "http://localhost:8000/api/v1/admin/whitelist" || true
fi

echo "=================================================================="
echo ">>> [6/6] 배포 완료 및 접속 정보 안내"
echo "=================================================================="
PUBLIC_IP=$(curl -s https://api.ipify.org || echo "YOUR-EC2-PUBLIC-IP")

echo "배포 완료!"
echo "------------------------------------------------------------------"
echo "  [사용자 송금 시뮬레이터] : http://${PUBLIC_IP}:8080/ 또는 http://${PUBLIC_IP}/"
echo "  [보안 관리자 관제 콘솔]   : http://${PUBLIC_IP}:8080/admin/ (Basic Auth 적용)"
echo "  [게이트웨이 API 헬스]     : http://${PUBLIC_IP}:8080/api/v1/health"
echo "------------------------------------------------------------------"
echo "※ 보안 주의: AWS 보안 그룹에서 80, 443, 8080 포트를 반드시 '내 IP (My IP/32)'로만 제한하세요."
echo "=================================================================="
