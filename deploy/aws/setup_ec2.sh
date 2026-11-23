#!/usr/bin/env bash
# ==============================================================================
# setup_ec2.sh — Aegis-FDS AWS EC2 초기 인프라 프로비저닝 스크립트
# 대상 OS: Ubuntu 22.04 LTS (x86_64)
# 권장 인스턴스: t3.xlarge (4 vCPU, 16GB RAM) 이상, gp3 50GB+ EBS
# ==============================================================================

set -euo pipefail

echo "=================================================================="
echo ">>> [1/5] 시스템 패키지 업데이트 및 기본 도구 / Python 환경 설치"
echo "=================================================================="
sudo apt-get update -y
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    git \
    jq \
    htop \
    net-tools \
    apache2-utils \
    python3 \
    python3-pip \
    python3-venv

# 온체인 수집기(collector_alchemy.py) 호스트 실행용 라이브러리 (Ubuntu 22.04 Python 3.10 호환 버전 고정 설치, 실패 시 중단)
pip3 install "websockets==16.0" "requests==2.34.2"

echo "=================================================================="
echo ">>> [2/5] Swap 메모리(4GB) 구성 (Spark/Kafka OOM 방지)"
echo "=================================================================="
if [ ! -f /swapfile ]; then
    sudo fallocate -l 4G /swapfile
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    echo "Swap 4GB 활성화 완료."
else
    echo "Swap 파일이 이미 존재합니다. 건너뜁니다."
fi

echo "=================================================================="
echo ">>> [3/5] Docker CE 및 Docker Compose v2 공식 저장소 설치"
echo "=================================================================="
sudo install -m 0755 -d /etc/apt/keyrings
if [ ! -f /etc/apt/keyrings/docker.gpg ]; then
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg
fi

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 현재 사용자(ubuntu)를 docker 그룹에 추가
sudo usermod -aG docker "$USER"

echo "=================================================================="
echo ">>> [4/5] Node.js 20.x LTS 설치 (웹 프론트엔드 프로덕션 빌드용)"
echo "=================================================================="
if ! command -v node &> /dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
    sudo apt-get install -y nodejs
fi

echo "=================================================================="
echo ">>> [5/5] 네트워크 보안 정책 확인 (AWS 보안 그룹 전용 제어)"
echo "=================================================================="
# SSH(22번)는 개방하지 않으며 AWS Systems Manager(SSM) Session Manager를 사용합니다.
# 호스트 UFW로 인한 포트 차단 충돌을 방지하기 위해 UFW를 비활성화하고,
# 네트워크 접근 통제는 CloudFormation이 생성한 AWS Security Group(내 IP 전용 80/8080 허용)으로 단일화합니다.
if command -v ufw &> /dev/null; then
    sudo ufw disable 2>/dev/null || true
fi
echo "[보안 정책] 호스트 UFW를 비활성화하고, AWS 보안 그룹(Security Group)으로 인바운드를 안전하게 통제합니다."

echo "=================================================================="
echo ">>> [완료] AWS EC2 초기 인프라 설정이 완료되었습니다."
echo ">>> Docker 그룹 반영을 위해 세션을 재접속하거나 'newgrp docker'를 실행하세요."
echo "=================================================================="
