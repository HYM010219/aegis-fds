# 프론트 원상복구와 이상거래 주입 1회 확인

> 이 문서는 로컬 작업 폴더에서 수행한 시험 기록이다. 이후 확인한 GitHub integration `14be4af0`에는 팀원의 서버 STR 연동·의견 저장이 이미 있으므로 최신 FE를 이 문서의 로컬 상태로 되돌리지 않는다. 현재 전달 범위와 응답 계약 차이는 같은 폴더의 `README.md`, `API_GUIDE.md`를 우선한다.

2026-11-24 KST. 사용자 요청에 따라 이번에 추가한 프론트 변경을 모두 철회했다. 별도 Spark 집계 탭·화면, BOT2/BOT3 패널, 사건 목록 버튼·문구, STR UI 변경을 운영 프론트에 두지 않는다. 이 문서는 이전 문서의 신규 UI 안내보다 우선한다.

## 복구 결과

- 이번 작업에서 수정한 `web/package.json`, `web/src/admin`의 앱·API·훅·메타·경보·사건 상세·그래프·거래 화면, 사용자 앱·API·CSS를 Git HEAD 내용으로 복구했다. 해당 diff를 확인했고 사용자 선행 변경이 있던 `web/vite.config.js`는 보존했다.
- 새로 추가했던 `web/src/admin/pages/Streaming.jsx`, `web/src/user/DemoPanel.jsx`, `web/.env.demo`를 제거했다.
- 최종 `git diff --name-only -- web` 출력은 기존 `web/vite.config.js` 하나뿐이다.
- 기존 프론트의 원래 `STR 초안 → 내려받기 (.txt)` 동선이 복구됐다. 서버 STR 조회·스냅샷 표시를 추가한 프론트 코드는 복구 대상에 포함되어 제거됐다.
- 기존 화면은 L1~L8만 지원하므로, 백엔드의 기본 경보 목록·통계·검색·사건 연결·감사로그에서는 검증용 R01/R04 관측 사건을 제외한다. 검증 API는 `include_observations=true`를 명시하면 조회할 수 있다. 기존 DB의 시험 기록은 삭제하지 않았다.
- 철회 전 FE 코드는 `docs/verification/frontend_before_restore_20261124.zip`에 보관했다. 프론트 빌드나 런타임에서 사용하지 않는다.
- `npm.cmd run build` 통과. 기존 큰 JS 번들 경고는 남는다.
- 이제 원래 `npm.cmd run dev`를 사용한다. `npm run demo`는 추가했던 명령이어서 복구와 함께 제거됐다. 원래 `.env.local`의 읽기 전용 설정도 변경하지 않았다.

## 이상거래 주입 1회

화면에 새 버튼을 추가하지 않고 백엔드 검증 스크립트로 BOT3_L8을 한 번 실행했다. 같은 run_id 재호출은 저장된 실행 조회로 끝나며 거래를 다시 주입하지 않는다.

```powershell
Get-Content -Raw -Encoding UTF8 scripts/verify_demo.py | docker compose exec -T gateway python - --execute --scenario BOT3_L8
```

실행 ID: `verify-e3a5edde-4678-49bc-a583-6d211d8fc8be`

사건 번호: `6b04ff100591fcf002525476`

| 단계 | 확인 결과 |
|---|---|
| Gateway·Redis 입구 판단 | 요청 7건 모두 HTTP 200. 이 입력은 입구 보류 조건을 충족하지 않음 |
| 판정 전 요청 기록 | `stream_requests` 7건 |
| 거래 결과 기록 | 승인 거래 7건 |
| Kafka 발행 | Outbox ACK 기준 `transfer.requests.v1` 7건, `transfer.events.v1` 7건 PUBLISHED |
| 요청 집계 별도 백엔드 경로 | requests-v2-1m에서 총 7건·40,770 USDT. 화면을 따로 만들지 않음 |
| 기존 L8 탐지 | spark_mongo_detector, batch 18724, L8 사건 생성 |
| 순환 근거 | A→B 10,000 / B→C 9,500 / C→A 9,000 USDT, 회수 비율 90% |
| 그래프 | 노드 7개·간선 7개·탐지 경로 3개 |
| 탐지 후 조치 | 대상 A가 Redis 내부 동결 목록에 등록됨 |
| 기존 사건 상세·서버 STR API | 같은 스냅샷과 순환 금액·비율 확인 |
| 원래 FE 호환 | 기본 경보 목록에 새 L8 사건 노출, 검증용 R 관측 사건은 기본 경보·감사로그에서 제외 |

이번 동작은 **입구에서 거절 → 처리**가 아니다. 먼저 통과한 모의 요청을 Kafka가 전달하고, Spark가 순환을 탐지한 다음 내부 지갑 상태를 동결한 것이다. 실제 자산 이동이나 온체인 동결은 없다. 별도 입구 거절은 이전 BOT3 R01/R04 시험 결과로 구분한다.

L8은 기존 구조대로 Kafka를 계기로 Mongo 거래를 조회하는 Spark 탐지기다. Delta 기반 요청 집계 경로와 하나의 처리 단계인 것처럼 설명하지 않는다.

스냅샷 ID: `9c815771b630920225acfc58972326247ca432ad94cd22e234d685fa9c8ef0f3`

## 증거와 재확인

- 주입 및 그래프·서버 STR 대조 출력: [frontend_restore_roundtrip_20261124.jsonl](verification/frontend_restore_roundtrip_20261124.jsonl)
- Kafka·요청 집계·Redis·사건·기본 API 호환 출력: [original_frontend_roundtrip_20261124.json](verification/original_frontend_roundtrip_20261124.json)
- 읽기 전용 재확인 코드: `scripts/verify_original_frontend_roundtrip.py`

```powershell
Get-Content -Raw -Encoding UTF8 scripts/verify_original_frontend_roundtrip.py | docker compose exec -T gateway python - --run-id verify-e3a5edde-4678-49bc-a583-6d211d8fc8be
```

기존 사건 화면: `http://127.0.0.1:8080/admin/#/cases/6b04ff100591fcf002525476`

브라우저 시각·클릭·원래 프론트가 생성하는 TXT의 새 다운로드 검수는 하지 않았다. 이번 확인은 원상복구 diff·프론트 빌드와 실제 백엔드 API·저장 데이터 기반이다. 백엔드 코드·시험 코드·과거 리포트는 보존했다.
