# FE 기존 화면 API 연결 안내

> **최신 integration 확인 결과:** `14be4af0`의 `web/src/admin/pages/CaseDetail.jsx`는 이미 `api.strDraft(caseId)`로 서버 초안을 조회하고 `report.text`를 표시·다운로드하며 분석관 의견 저장도 지원한다. 아래 내용은 로컬 검증 서버의 응답 계약이다. 최신 FE 구현을 덮어쓰거나 같은 기능을 다시 만들지 말고, BE와 응답 호환성을 확인한다. 최신 코드가 사용하는 `provenance`, `report_id`, `analyst_opinion`과 로컬의 `snapshot`은 같은 응답 형식이라고 가정하지 않는다.

화면 구성은 FE 담당 범위다. 별도 Spark 집계 페이지나 봇 실행 버튼은 추가하지 않는다. 테스트 데이터 주입과 컨테이너 운영은 BE가 맡는다.

## 접속·인증

BE가 알려준 서버를 Base URL로 사용한다. 관리자 API에는 `X-Admin-Key`가 필요하다. 기존 Vite 서버 프록시/운영 서버에서 키를 붙이며 브라우저 번들 또는 `VITE_*` 변수에 관리자 키를 넣지 않는다. 실제 키는 Git에 포함하지 않는다.

현재 기존 Vite 설정은 `VITE_API_BASE=/`일 때 서버 측 프록시에서 키를 붙인다. `VITE_PROXY_TARGET`을 BE 서버로 지정하는 설정은 FE 담당자가 자신의 환경에서 수행한다. `.env.local`의 `VITE_READONLY=1`이면 GET 이외 요청은 403 READ_ONLY_DEMO로 차단되므로 감사 기록·송금 테스트 전에 담당자가 확인한다. 이 전달 작업에서 해당 설정을 변경하지 않았다.

## API와 기존 화면

| 기능 | 메서드·경로 |
|---|---|
| 경보 목록 | `GET /api/v1/admin/cases` |
| 사건 상세 | `GET /api/v1/admin/cases/{case_id}` |
| 그래프 | `GET /api/v1/admin/cases/{case_id}/graph?depth=2` |
| 같은 스냅샷의 서버 STR | `GET /api/v1/admin/cases/{case_id}/str-draft?snapshot_id={snapshot_id}` |
| STR 감사 기록 | `POST /api/v1/admin/cases/{case_id}/decision` |

기본 경보 목록은 기존 프론트가 지원하는 L1~L8만 반환한다. R01/R04 요청 패턴 관측은 BE 검증용이며 기존 FE에 표시하도록 요구하지 않는다.

## 상세와 STR 연결 순서

1. 사건 상세 API 응답의 `evidence_snapshot.snapshot_id`와 근거 값을 사용한다.
2. 같은 ID를 `str-draft`의 `snapshot_id`에 전달한다.
3. 응답의 `snapshot.snapshot_id`가 요청한 ID와 같은지 확인한다.
4. 서버 응답 `text`를 기존 STR 팝업에 표시하고 같은 문자열을 UTF-8 TXT로 저장한다. 숫자를 FE에서 재계산하지 않는다.
5. 기존 감사 기록 API가 성공한 뒤 다운로드한다. 오류는 기존 화면 방식으로 표시한다.

서버 STR 응답 형식 예시(실제 본문 생략):

```json
{
  "case_id": "6b05018f0591fcf002525477",
  "snapshot": {
    "snapshot_id": "04590f755fe1f5aa6b8a26166221c85fbbe784b057425e666a4b6ad39cf721ba",
    "rule_id": "L8",
    "evidence": {"amounts": [10000, 9500, 9000], "return_ratio": 0.9}
  },
  "created_at": "UTC ISO 8601",
  "text": "서버에서 생성한 STR 초안 전체 문자열"
}
```

감사 기록 요청 예시:

```json
{
  "action": "STR_DRAFT",
  "actor": "현재 로그인한 담당자명",
  "note": "STR 다운로드 · snapshot 04590f755fe1f5aa6b8a26166221c85fbbe784b057425e666a4b6ad39cf721ba"
}
```

**로컬 작업 폴더의 프론트만 이전 상태로 복구했다. GitHub 최신 FE는 이미 서버 STR을 사용하므로 그대로 보존한다.** 위 `snapshot_id` 쿼리와 응답 예시는 로컬 검증 서버 전용이며, 최신 integration 서버의 응답 계약은 해당 서버의 구현·명세를 기준으로 한다. 로컬 백엔드를 최신 integration에 병합하는 작업은 이 자료 공유에 포함되지 않는다.

## 함께 확인할 완료 조건

- 최신 사건이 기존 경보 목록·상세에서 열린다.
- 그래프의 순환 경로 3개와 금액 10,000→9,500→9,000 USDT, 회수율 90%가 보인다.
- 상세·STR 팝업·다운로드 TXT가 같은 스냅샷과 같은 수치를 사용한다.
- 다운로드 시 STR_DRAFT 감사 기록이 남는다.
- 합성 모의 출금과 내부 동결을 실제 자산 이동·온체인 동결로 표시하지 않는다.

별도 UI를 만들거나 원래 송금 버튼에 자동 봇 주입을 연결한 상태가 아니다. 그러한 동선 변경은 별도 요구·담당자 합의가 필요하다.
