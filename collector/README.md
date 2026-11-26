# collector

봇(시뮬레이터)과 데이터 수집기 코드를 이 폴더에 둔다.

- 보내는 곳: `POST /api/v1/transfers` (게이트웨이, 포트 8000)
- 필드: `tx_hash`, `log_index`, `from`, `to`, `amount`, `block`, `timestamp`
- `timestamp`는 현재 시각과 30초 이내여야 한다. 벗어나면 422로 거절된다.

## 수집기 이미지 (k3s)

`collector_alchemy.py`만 담는 이미지다 (`bots/rate_control.py` 포함). 빌드 위치는 저장소 루트이고, 넣을 파일은 `collector/Dockerfile.dockerignore`가 정한다.

```bash
docker build -f collector/Dockerfile -t aegis-collector:latest .
```

`deploy/k3s/import_images.sh`가 이 빌드까지 하고 k3s에 넣는다. 배포는 `deploy/k3s/16-collector-deployment.yaml`.

| 환경 변수 | 필수 | 내용 |
|---|---|---|
| `ALCHEMY_WSS_URL` | 예 | Alchemy WebSocket 주소 (Secret `aegis-env`). 없으면 시작하지 않는다 |
| `GATEWAY_API_KEY` | 예 | 게이트웨이 키 (Secret `aegis-env`) |
| `FASTAPI_URL` | 아니오 | 보낼 주소. 기본 `http://localhost:8000/api/v1/transfers`, k3s는 `http://gateway:8000/api/v1/transfers` |
