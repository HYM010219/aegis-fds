# collector

봇(시뮬레이터)과 데이터 수집기 코드를 이 폴더에 둔다.

- 보내는 곳: `POST /api/v1/transfers` (게이트웨이, 포트 8000)
- 필드: `tx_hash`, `log_index`, `from`, `to`, `amount`, `block`, `timestamp`
- `timestamp`는 현재 시각과 30초 이내여야 한다. 벗어나면 422로 거절된다.
