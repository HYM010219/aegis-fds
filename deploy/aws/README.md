# Aegis-FDS AWS EC2 배포 및 인프라 운영 가이드 (v2.0 보안 강화본)

본 문서는 가상자산 이상거래 탐지 시스템(Aegis-FDS)을 **AWS EC2 클라우드 인프라**에 안전하게 배포하고 운영하기 위한 인프라·DevOps 실무 가이드입니다.

---

## 🔒 1. 보안 핵심 원칙 및 방화벽 정책 (CRITICAL)

관제 콘솔(`/admin/`)은 브라우저에 관리자 키를 노출하지 않기 위해 리버스 프록시(Nginx)가 `X-Admin-Key`를 내부에서 주입하는 구조입니다.  
따라서 **웹 포트가 외부에 열리면 IP 주소를 아는 누구나 지갑 동결·해제·화이트리스트 조작을 수행할 수 있습니다.** 다음 보안 수칙을 반드시 준수합니다.

### 1.1 AWS 보안 그룹(Security Group) 인바운드 규칙
| 포트 (Port) | 프로토콜 | 허용 대상 (Source) | 비고 |
| :--- | :--- | :--- | :--- |
| **80 / 443 / 8080** | TCP | **내 IP (`My IP/32`) 전용** | **절대 `0.0.0.0/0` 개방 금지**. 장소 이동 시 30초 갱신 |
| **22 (SSH)** | TCP | **차단 (또는 내 IP 전용)** | **AWS Systems Manager(SSM) Session Manager 접속 권장** |
| **8000, 27017, 6379, 9092, 8123** | TCP | **전부 차단** | Docker 내부 브릿지망(`aegis-net`)으로만 통신 |

### 1.2 Nginx Basic Auth (2중 잠금)
- `/admin/` 관제 콘솔 및 `/api/v1/admin/` 관리자 API 엔드포인트에 **HTTP Basic Auth 2차 인증**이 기본 적용됩니다.
- 초기 관리자 계정: `admin` / 비밀번호: `.env`의 `ADMIN_CONSOLE_PASSWORD`에 설정한 비밀번호

---

## 🏗️ 2. 아키텍처 모델: PoC 단일 노드 vs To-Be 2-Tier 분리

* **PoC 배포 (현 단계)**: **단일 EC2 인스턴스 (`m5.2xlarge`: 8 vCPU, 32GB RAM 권장)**
  - Spark(2GB), ClickHouse(4GB), Kafka, Mongo, Redis, Gateway, Nginx를 단일 호스트 내부 네트워크로 안전하게 통합 구동.
  - 단일 호스트 환경이므로 문서/발표 시 "2-Tier 물리 망분리" 대신 **"PoC 단일 호스트 (Docker 내부 격리), To-Be 앱·데이터 노드 물리 분리"**로 명확히 기술함.
* **To-Be 프로덕션 (상용화 단계)**:
  - App Node (Nginx, Gateway, Vite) vs Data Node (Kafka, Spark, Mongo, Redis, ClickHouse) 분리 및 VPC 피어링.

---

## 📦 3. 인프라 프로비저닝 & 코드 전송 (IaC & Bundle)

### 3.1 CloudFormation 원클릭 인프라 생성 (`aegis-poc.yaml`)
AWS 콘솔 -> CloudFormation -> 스택 생성 -> `deploy/aws/aegis-poc.yaml` 템플릿 업로드:
- **AllowedCidr1**: 관리자 현재 공인 IP (예: `1.2.3.4/32`)
- **InstanceType**: `m5.2xlarge` (권장: 8 vCPU, 32GB RAM)
- **VolumeSizeGiB**: `100` (EBS gp3 100GB)
- VPC, 서브넷, 인터넷 게이트웨이, 보안 그룹(80/443/8080 허용, SSH 22 포트 원천 차단), SSM 인스턴스 프로파일, 전용 S3 배포 버킷이 3분 내 자동 생성됩니다.

### 3.2 Git Bundle 전송 (GitHub Push 우회)
`local-lab` 브랜치는 원격 저장소에 Push하지 않은 로컬 작업물이므로, S3 버킷을 통해 EC2로 안전하게 이식합니다.
```bash
# 1. 로컬 PC (C:\Users\Baik\aegis-fds-main)에서 번들 파일 생성
git bundle create aegis.bundle local-lab

# 2. CloudFormation Outputs에 표시된 S3 버킷으로 업로드
aws s3 cp aegis.bundle s3://<DeployBucketName>/aegis.bundle

# 3. EC2 인스턴스 (SSM 접속)에서 다운로드 및 복원
aws s3 cp s3://<DeployBucketName>/aegis.bundle ./aegis.bundle
git clone aegis.bundle aegis-fds
cd aegis-fds
git checkout local-lab
```

---

## 🚀 4. 원클릭 배포 3단계 실행 절차

```bash
# [1단계] .env 파일 생성 및 필수 비밀키 입력
cp .env.example .env
nano .env

# [필수 확인 키]
# GATEWAY_API_KEY=...
# ADMIN_API_KEY=...
# CLICKHOUSE_PASSWORD=...
# ADMIN_CONSOLE_PASSWORD=<YOUR_ADMIN_PASSWORD>
# ALCHEMY_WSS_URL=wss://eth-mainnet.g.alchemy.com/v2/...  # Key 1 URL
# ALCHEMY_KEY_1=...                                      # Key 1 (일 00:00 ~ 일 23:30)
# ALCHEMY_KEY_2=...                                      # Key 2 (일 23:30 ~ 화 00:00)
# ALCHEMY_KEY_3=...                                      # Backup Key 3
# ALCHEMY_KEY_4=...                                      # Backup Key 4

# [2단계] 초기 인프라 프로비저닝 (Docker, 4GB Swap, Python 수집기 의존성 설치)
chmod +x deploy/aws/*.sh
./deploy/aws/setup_ec2.sh
newgrp docker

# [3단계] 프로덕션 원클릭 배포 실행
# (Nginx Basic Auth 생성 -> FE 빌드 -> 컨테이너 기동 -> 토픽 생성 -> 직원 API 화이트리스트 시딩)
./deploy/aws/deploy.sh

# [4단계] 온체인 수집기 systemd 등록 및 기동 (스모크 테스트 전에 실행)
sudo cp deploy/aws/aegis-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now aegis-collector

# [5단계] 리소스 로깅 스크립트 백그라운드 구동 (5분 간격)
nohup bash deploy/aws/record_stats.sh > ~/soak_stats_runner.log 2>&1 &
```

---

## ⚙️ 5. 48시간 침지 테스트(Soak Test) 운영 도구

### 5.1 온체인 수집기 동작 실시간 확인
```bash
# 수집기 동작 실시간 로그 확인
tail -f /home/ubuntu/collector.log

# 수집기 systemd 상태 확인
sudo systemctl status aegis-collector
```

### 5.2 리소스 로깅 현황 확인 (5분 간격 docker stats)
침지 테스트 기간 중 Kafka/ClickHouse/Spark의 CPU 및 메모리 사용 추이를 5분 간격으로 영구 기록합니다.
```bash
# 기록 확인 (~/soak_stats.csv)
tail -f ~/soak_stats.csv
```

### 5.3 Alchemy API 키 교체 (`swap_key.sh`, 수집기만 재시작, 수 초 공백 발생, 시각 기록)
CU 한도 소진 방지를 위해 일요일 밤(23:30) Key 2로 교체합니다:
```bash
# 사용법: bash deploy/aws/swap_key.sh <키번호>
bash deploy/aws/swap_key.sh 2

# 교체 이력 확인 (~/soak_events.log)
cat ~/soak_events.log
```
- `.env`를 자동 백업(`cp -p`)하고 `ALCHEMY_WSS_URL`을 갱신합니다.
- 수집기 프로세스/서비스만 재기동(약 2~3초 수집 공백 발생, 이벤트 로그에 시각 기록)되며, Kafka/Spark/ClickHouse/Mongo/Redis 등 데이터 파이프라인 컨테이너는 일체 영향받지 않습니다(무중단 유지).

---

## 🛠️ 6. 침지 테스트 일정 계획

| 일정 | 테스트 항목 | 주요 내용 및 목적 | 비고 |
| :--- | :--- | :--- | :--- |
| **토요일 오후~밤** | **인프라 배포 & 스모크 테스트** | • CloudFormation 스택 생성 (`aegis-poc.yaml`)<br>• 번들 전송 및 deploy.sh 실행<br>• **수집기 systemd 등록 후** 스모크 테스트(콘솔, API, 블록 수신 실측) | • 배포와 침지 동일 환경 유지 |
| **일 00:00 ~ 일 23:30** | **메인넷 48h 침지 1일차 (주말)** | • Key 1(신규 30M CU)로 실시간 온체인 수집<br>• 주말 트래픽 패턴 하 탐지/보류/동결 모니터링<br>• `record_stats.sh` 5분 간격 통계 수집 | • 일요일 23:30 `swap_key.sh 2` 실행 |
| **일 23:30 ~ 화 00:00** | **메인넷 48h 침지 2일차 (평일)** | • Key 2(팀원 신규 30M CU)로 교체(수집기만 수 초 재시작, 파이프라인 무중단)<br>• 월요일 평일 트래픽 패턴 비교<br>• Kafka 유휴 CPU 스파이크 재현 여부 실측 | • 화요일 00:00 침지 자동 종료 타이머 설정 |
| **차주 화요일 이후** | **봇 시험 및 리플레이 심층 검증** | • bot3 / 시드 B 단독 및 혼합 검증<br>• A2 / EK 리플레이 정밀 검증 (로컬/EC2) | • 침지 테스트 완료 후 데이터 오염 없이 별도 수행 |
