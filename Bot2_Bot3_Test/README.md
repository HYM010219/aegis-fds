# BOT2·BOT3 테스트 전달 자료

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
