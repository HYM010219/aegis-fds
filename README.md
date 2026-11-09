# AEGIS-FDS

USDT 송금 데이터의 이상거래를 실시간으로 탐지하고 지갑을 동결하는 프로젝트.

## 폴더

| 폴더 | 내용 |
|---|---|
| `web/` | 관제 웹 |
| `fds/` | 탐지: 게이트웨이, Publisher, 경보 소비자(`app/`), Spark 룰(`spark/`), 블랙리스트·화이트리스트(`seed/`) |
| `collector/` | 봇, 데이터 수집기 |

## 실행

1. `.env.example`을 복사해 `.env`를 만들고 `MONGO_URI`를 넣는다.
2. `docker compose up -d --build`
3. 새 환경이면 Kafka 토픽 `transfer.events.v1`, `fds.alerts.v1`을 직접 만든다 (자동 생성 꺼짐).

## 공용 파일

`docker-compose.yml`과 데이터 규격은 여러 폴더가 함께 쓴다. 바꿀 때는 팀에 먼저 알린다.
