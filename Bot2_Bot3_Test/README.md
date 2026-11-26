# BOT2·BOT3 테스트 전달 자료

## 2026-11-25 로컬 봇 테스트 및 후속 변경

이 절은 아래의 기존 11/24 별도 테스트 환경 안내와 구분한다. 팀장님의 Spark 재계산·STR 구현(`14be4af`)을 기반으로 로컬에서 봇을 주입한 결과와 후속 변경을 공유한다. 기존 DE·FE 작성 이력을 유지하며, 새로운 탐지 엔진을 구현했다고 주장하지 않는다.

- 확인한 L2 사건: `6b069cf0d1ff81df2d52e65e`, 지갑 `BOT-L2-72ba82-A`, Spark 배치 `24229`.
- 당시 기준은 50,000 USDT였다. 승인 송금 20,000 + 20,000 + 15,000 = 55,000 USDT, 3건, 상대 3곳이다.
- 화면·STR 기록에서 판정 값과 Spark 재계산 값의 일치를 확인했다. 1분 내역은 17:30에 2건/40,000 USDT, 17:31에 1건/15,000 USDT였다. 이는 사용자가 제공한 로컬 화면·STR 기록이며 아래 오프라인 검사와 구분한다.
- 이후 합의한 R05·L2 공통 기준 100,000 USDT를 코드·Compose·화면 기준에 반영했다. 현재 L2 봇은 40,000 + 40,000 + 30,000 = 110,000 USDT다. 과거 55,000 사례를 새 100,000 기준의 성공 증거로 사용하지 않는다.
- R01의 5번째 200 승인 후 보류는 11/09 합의된 기존 동작이며 이번 신규 변경이 아니다.

실행 도구:

```bash
python bots/run_bots.py --dry-run --only L2 L6 L7
python bots/run_bots.py --once --only L2 L6 L7
python replay_golden.py --input data/onchain/usdt_20261124.jsonl --inspect-only
python bots/stream_all.py --input data/onchain/usdt_20261124.jsonl --bot-ratio 0.5
```

송신 명령은 테스트용 Gateway와 관리자 API에 연결한다. 실제 자산 이동·온체인 출금 차단을 수행하는 것이 아니다. 녹화 JSONL, DB, `.env`, 실행 로그는 Git에 포함하지 않는다. AWS 녹화본을 사용하려면 별도 백업에서 입력 파일을 복사해야 한다.

실행한 오프라인 검사:

- `scripts/verify_amount_limits.py`: R05 경계와 L2 공통 기준 검사. 기본 100,000 및 150,000 설정 재정의 검사 통과.
- `scripts/verify_mix_rate.py`: 일정한 유입의 2:1 허용 비율, 오래된 유입 시 대기, 일시적 몰아 보내기 방지와 시나리오 대기 처리 검사 통과. HTTP는 모의 처리한다.
- `scripts/verify_replay_input.py`: 입력 파일 선택, 제거 이벤트·미소 금액 제외, 현재 시각·회차별 해시 생성 검사 통과. HTTP는 모의 처리한다.
- 프런트 `npm run build` 통과. 이 검사는 화면 전체의 실행 검수를 뜻하지 않는다.

알려진 제한 및 남은 검증:

- 최신 혼합 속도 제어는 실유입 2건당 봇 1건의 허용량을 최대 10건까지 적립하고 최소 0.05초 간격으로 소비한다. 오프라인 검사에서 실유입 100건에 봇 50건, 추가 10건의 묶음 유입에 봇 5건을 확인했다. 실유입이 2초 넘게 중단되면 멈추고, 재개 시 과거 누적량을 한꺼번에 내보내지 않는다. 용량 상한과 중단 시 초기화가 있어 모든 부하에서 정확히 50%를 보장하지 않으며 실제 혼합 실행의 처리량·지연은 별도 측정이 필요하다.
- 같은 L2 회차에서 A의 자동 동결 이후 B/C/D가 L5 검토 대상으로 함께 나타날 수 있다. 경보의 봇 비중과 전체 거래의 봇 비중은 다르다.
- 모의 주소의 출처 구분을 위한 로컬 표시 변경이 포함돼 있으나 화이트리스트·블랙리스트가 함께 적용된 주소의 최종 표시는 추가 확인이 필요하다.
- 100,000 기준 전체 흐름의 새 사건 검증, 실제 혼합 부하 측정, Spark 경보 전송 도중 실패·재시작 시 누락/중복 검증은 이 공유만으로 완료 처리하지 않는다.
- Kubernetes 새 계정 배포, Delta 추가, Kafka 다중 브로커 구성은 이번 봇 테스트 변경의 범위가 아니다. 기존 배포 파일 전체의 기동을 보장하지 않는다.

**FE는 [검증 체크리스트](FE_VERIFICATION_CHECKLIST.md)부터 읽고 확인한다.** 대상은 L8 순환거래·R01 고빈도 송금 두 개다. 성공 사건 번호, 비교할 값, 서버가 다를 때 조회되지 않는 이유, 담당별 선행 작업과 결과표를 한 문서에 정리했다. R01의 기존 화면 노출은 아직 선행 확인이 필요하며 두 규칙 모두 화면 검수 완료로 보고하지 않는다.

> GitHub 공유 기준: `integration`의 `14be4af024e08041a994b1cf0e4ed7e1dbedd968`을 확인했다. 최신 FE에는 서버 STR 조회·의견 저장이 이미 구현되어 있다. 그 코드를 되돌리거나 새 화면을 만들지 않는다. 이 폴더는 별도의 로컬 백엔드에서 수행한 주입 결과와 참고 스크립트이며, 최신 integration 전체를 재검증했다는 뜻은 아니다.

프론트 화면을 추가하거나 바꾸지 않고, 백엔드에서 데이터를 주입하고 FE 담당자가 기존 경보·사건·그래프·STR에 연결하기 위한 자료다. 별도 Spark 집계 탭·봇 실행 패널은 요청하지 않는다.

## 누가 무엇을 하는가

| 담당 | 할 일 |
|---|---|
| BE/인프라 담당 | 컨테이너 실행 유지, 주입 스크립트 실행, 접속 가능한 서버 주소·테스트 사건 ID·인증 방식 제공, 장애 점검 |
| FE 담당 | 이미 구현한 기존 화면의 서버 STR 연결·감사 기록을 보존하고, 전달받은 테스트 서버 응답과의 호환성 및 화면 결과 확인 |

FE는 Python 검증 스크립트를 PC에서 직접 실행할 필요가 없다. 먼저 BE의 실행 중인 서버로 연결해 아래 사건을 확인한다.

## 먼저 FE에게 전달할 것

1. 이 폴더 전체.
2. 백엔드 주소: `http://<BE_PC_IP>:8000` 또는 공동 접근 가능한 테스트 서버. `127.0.0.1`은 전달받은 사람 자신의 PC다. 다른 네트워크에서는 사설 PC 주소에 바로 접속할 수 없으므로 접근 경로를 먼저 준비한다.
3. 관리자 인증을 붙이는 서버 프록시 설정 방법. 키 실제 값은 문서/Git/브라우저 코드에 넣지 않는다.
4. 아래 최신 사건 ID와 예상 결과.

## 최신 시연 사례

사용자가 실행하여 성공 응답을 확인한 BOT3_L8 사례:

```text
run_id: verify-f40584f2-3cc1-4766-ad88-a715f7168e89
case_id: 6b05018f0591fcf002525477
snapshot_id: 04590f755fe1f5aa6b8a26166221c85fbbe784b057425e666a4b6ad39cf721ba
Spark batch: 18788
```

- 요청 7건 모두 HTTP 200, 이후 L8 탐지·사건 생성.
- 그래프: 노드 7개, 간선 7개, 탐지 경로 3개.
- A→B→C→A의 금액: 10,000→9,500→9,000 USDT, 회수 비율 90%.
- 합성 모의 출금이다. 실제 자산 이동이나 온체인 동결이 아니다.
- 기존 프론트에서 `/admin/#/cases/6b05018f0591fcf002525477`을 연다. 프론트 호스트 주소는 FE 자신의 실행 환경에 맞춘다.
- 이 사건은 BE가 사용한 MongoDB에 저장되어 있다. Git 폴더를 받아도 DB 데이터가 복사되는 것은 아니다.

과거 검증 사례: `verify-e3a5edde-4678-49bc-a583-6d211d8fc8be`, 사건 `6b04ff100591fcf002525476`. 컨테이너·Kafka·요청 집계·기존 L8·서버 STR 검증 원본은 저장소의 `docs/verification/original_frontend_roundtrip_20261124.json`에 있다.

## BE가 실행할 명령

저장소 루트 PowerShell에서 실행한다. `.env`와 변경된 백엔드·Compose 코드가 있는 환경이 필요하다. 이 폴더만으로 새 서버를 만들 수는 없다.

```powershell
docker compose -f docker-compose.yml -f docker-compose.demo.yml -f docker-compose.streaming.yml up -d
```

새 순환거래를 주입할 때만 실행한다. 약 45초의 계획 전송과 추가 탐지 대기가 있다.

```powershell
Get-Content -Raw -Encoding UTF8 Bot2_Bot3_Test/verify_demo.py | docker compose exec -T gateway python - --execute --scenario BOT3_L8
```

다른 시나리오는 `BOT2_HOTWALLET`, `BOT3_R01`, `BOT3_R04`다. 한 번에 하나씩 실행한다. 같은 시나리오 명령을 다시 실행하면 새 run_id로 추가 데이터가 주입된다.

| 시나리오 | Gateway 예상 결과 | 구분 |
|---|---|---|
| BOT2_HOTWALLET | 200 × 6 | 핫월렛 R01/R04 면제 |
| BOT3_R01 | 200 × 5, 429 × 1 | 5번째 승인 후 보류, 6번째 거절 |
| BOT3_R04 | 200 × 4, 429 × 2 | 분산 요청 입구 보류·거절 |
| BOT3_L8 | 200 × 7, 이후 L8 사건 | 승인 후 비동기 탐지·내부 동결 |

이미 저장된 최신 L8 사례를 읽기 전용으로 재확인:

```powershell
Get-Content -Raw -Encoding UTF8 Bot2_Bot3_Test/verify_original_frontend_roundtrip.py | docker compose exec -T gateway python - --run-id verify-f40584f2-3cc1-4766-ad88-a715f7168e89
```

## 파일 안내

- `API_GUIDE.md`: FE에게 전달할 API·인증·STR 연결 설명.
- `FE_VERIFICATION_CHECKLIST.md`: 화면과 보고서에서 무엇을 비교하는지, 두 사례의 기대값과 확인 결과표.
- `LATEST_L8_RESULT.json`: 사용자가 제공한 최신 실행 성공 출력.
- `verify_demo.py`: BE용 주입·검증 스크립트.
- `verify_original_frontend_roundtrip.py`: BE용 읽기 전용 전체 경로 대조.
- `FRONTEND_RESTORE_AND_ROUNDTRIP_20261124.md`: 원상복구와 이전 검증 결과.

이 폴더의 스크립트는 전달 시점의 사본이다. 원본은 `scripts/`에 있다. FE 화면 코드 변경은 포함하지 않는다.

## integration에 올릴 때

BE 서버를 함께 사용하는 FE에게는 이 전달 폴더와 서버 접속 정보가 우선이다. 팀원이 자체 서버를 실행하려면 수정한 `fds/` 백엔드 코드와 `docker-compose.demo.yml`, `docker-compose.streaming.yml`도 별도로 공유·리뷰해야 한다. 현재 로컬의 미커밋 코드는 GitHub integration에 자동으로 포함되지 않는다.

전달 폴더만 선택해 검토·커밋하려면 저장소 루트에서:

```powershell
git add -- Bot2_Bot3_Test
git diff --cached --name-only
```

스테이징된 목록에 다른 담당자의 파일이 섞이지 않았는지 확인한다. 이 전달 폴더의 GitHub 반영은 다른 폴더의 소스 수정이나 AWS 배포를 포함하지 않는다.
