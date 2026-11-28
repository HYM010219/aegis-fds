# AWS K3s 복구 시연

대상은 App 단일 노드의 Gateway 파드 복구다. 노드·DB 장애, HPA 자동 확장, 수집기 백필, 무유실 적재를 검증하지 않는다. 시험은 실제 거래 API에 `BOT-LOAD-` 테스트 데이터를 남기며 기존 BOT 분류로 구분한다.

## 사전 준비 (시연 전에 한 번)

1. App/Data 배포와 DB 초기화를 끝낸다. Gateway 두 파드가 Ready이고 새 이미지가 App 노드 containerd에 있어야 한다. `recovery-job.yaml`의 이미지 태그를 실제 Gateway와 맞춘다.
2. Helm과 kube-prometheus-stack을 설치한다. 기존 릴리스가 있다면 같은 릴리스 `mon`을 갱신한다. chart 버전은 검증된 버전으로 고정해 운영한다.

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install mon prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace -f deploy/k3s/demo/monitoring-values.yaml --wait --timeout 10m
sudo k3s kubectl apply -f deploy/k3s/demo/pod-monitor.yaml
sudo k3s kubectl -n monitoring create configmap aegis-recovery-dashboard \
  --from-file=aegis-recovery.json=deploy/k3s/demo/grafana-dashboard.json \
  --dry-run=client -o yaml | sudo k3s kubectl apply -f -
sudo k3s kubectl -n monitoring label configmap aegis-recovery-dashboard grafana_dashboard=1 --overwrite
```

Monitoring은 추가 CPU/메모리를 사용한다. 앱과 함께 배치한 뒤 노드 여유 자원을 확인한다. K3s에서 접근할 수 없는 control-plane 전용 타깃은 monitoring-values에서 제외했다. HTTPS/API exporter 수집 장애와 실제 앱 장애를 구분한다.

Grafana는 사전에 SSM 포트포워딩 터널 등으로 열어둔다. 클러스터에서 포트포워딩할 경우:

```bash
sudo k3s kubectl -n monitoring port-forward svc/mon-grafana 3000:80 --address 127.0.0.1
```

이는 App 서버의 localhost에만 열리므로 사용자 PC에는 SSM 터널이 별도로 필요하다. AWS 보안 그룹에 Grafana 공개 포트를 추가하지 않는다. Grafana의 subpath 설정에 따라 `/grafana/`로 접속한다. Grafana 로그인은 설치된 Secret의 계정을 사용하고 비밀번호를 저장소에 남기지 않는다. 실제 화면은 AWS에서 확인해야 한다.

## 실제 시연 (한 번 실행)

Grafana에서 `Aegis Gateway recovery demo`를 열어둔 뒤 App 호스트에서 실행한다.

```bash
bash deploy/k3s/demo/run.sh
```

- 기본 90초, 목표 10 RPS, 15초 이후 정상 baseline을 확인한 뒤 Ready Gateway 하나를 정상 종료한다. 10 RPS는 검증 시작값이며 포화 부하라고 주장하지 않는다. 부하 수치는 job env에서 조절한다.
- 클러스터 내부 Job이 Gateway **Service DNS**로 POST한다. 특정 파드에 고정되는 port-forward를 사용하지 않는다.
- API 키는 Secret에서 Job으로 주입한다. 인증 실패·유효성 검사 실패·5xx·연결 실패를 성공으로 처리하지 않는다. HTTP 재시도를 하지 않아 일시 실패도 기록된다.
- 신규 UID 파드가 Ready이고 원래 Ready 수가 복구돼야 파드 복구로 판정한다. 복구 후 승인 응답이 있어야 요청 연속성 통과가 가능하다.
- 식별 헤더는 기본 비활성화(`POD_IDENTITY_HEADER=0`)다. run.sh가 시험 직전에 1로 켜고 롤아웃 완료를 기다리며, 종료·실패 시 EXIT 처리에서 0으로 되돌리고 롤아웃을 기다린다. 강제 종료(SIGKILL)나 API 장애로 정리가 실패하면 운영자가 비활성화를 확인해야 한다. 설정 전환 자체도 롤링 배포이므로 본 시험의 파드 종료와 구분한다.
- Nginx는 시험 중에도 파드 식별 헤더를 외부 응답에서 제거한다. 내부 Service로 직접 호출하는 실행기만 헤더를 사용한다. Nginx ConfigMap 변경은 실행 중인 Nginx에 자동 reload되지 않으므로 신규 배포 또는 Nginx 재시작에 포함한다.
- Gateway 응답의 `X-Gateway-Pod`만 읽어 요청별·파드별 처리 건수를 기록한다. 값은 Downward API의 POD_NAME에서 오며 클라이언트가 보낸 값을 반영하지 않는다. UID 헤더는 보내지 않는다. 쿠버네티스 API에서 읽은 UID는 삭제 사전조건과 신규 파드 판정에만 사용한다. 새 이미지와 manifest를 모두 반영해야 한다.
- 시험 요청마다 `Connection: close`를 명시하고 새 연결을 사용한다. 단일 연결을 재사용하면 Service의 연결 단위 분산 때문에 한쪽 파드로만 요청이 갈 수 있다. 본 실행기는 Nginx를 우회해 Service에 직접 접속하므로 외부 프록시 경로의 분산·연결 재사용까지 검증하지 않는다.
- 헤더 없는 오류 응답은 원인 미확인 실패다. 앱 내부 미처리 예외의 500과 프록시 오류 모두 가능하므로 파드 미도달로 단정하지 않는다. `/healthz`는 Gateway 경로이며 현재 Nginx에는 해당 경로의 프록시가 없어 외부 8080 주소의 `/healthz`로 분산을 판정하지 않는다.
- 장애 발생 전 최초 Ready 파드 모두에서 승인 응답을 관측해야 분산 점검을 통과한다. 한쪽만 응답하거나 식별 헤더가 없으면 장애를 발생시키지 않는다. 비율이 정확히 50:50이어야 하는 것은 아니다.
- 복구 구간에는 생존 파드의 승인 응답, 복구 후에는 신규 UID 파드의 승인 응답까지 각각 확인한다. `report.json`의 `pod_counts`, `baseline_distribution_pass`, `survivor_handled_during_recovery`, `replacement_handled_after_recovery`에 결과가 남는다. Grafana에는 처리 파드별 RPS 패널이 추가돼 있다.
- 전체 요청 수가 목표 부하의 80% 미만이거나 실행기 포화로 전송 기회를 건너뛰면 통과하지 않는다. 목표 RPS와 실제 RPS는 보고서에 별도로 남는다.
- 파드 조회·삭제는 별도 관찰 스레드에서 실행한다. API 응답이 느려져도 요청 생성 루프는 대기하지 않는다. API 자체 오류가 발생하면 시험 오류를 기록하고 중단한다.
- 요청 phase는 응답 종료 시점 기준이다. 장애 요청 전에 시작해 그 이후 끝난 요청은 `boundary`로 따로 분류한다. 보고서의 `phase_counts`에 baseline/boundary/recovering/recovered별 전체·승인·거부·실패 건수를 기록한다. 합격 판정 조건은 동일하며 boundary 실패도 전체 실패에 포함한다. `started_at`, `finished_at`과 시험의 `fault_requested_at`, `fault_acknowledged_at`도 기록한다. `recovery_seconds`는 삭제 요청 시작부터 새 UID Ready 관측까지로 API 처리·관측 지연도 포함한다.
- 응답 중간 단절(`IncompleteRead` 등)은 개별 요청 실패로 기록하고 다음 요청은 계속 보낸다. `p95_accepted_seconds`는 승인 응답만, `p95_seconds`는 실패를 포함한 전체 응답의 값이다. Grafana 지연 패널은 승인 응답만 사용한다.
- 기본 장애는 정상 종료(delete)다. 프로세스 강제 종료·노드 장애·과부하 OOM 시험과 구분한다. 5초 preStop은 endpoint 변경 전파를 기다리는 완충 시간이며 실패 0건을 보장하지 않는다.
- 종료 후 120초 동안 메트릭 최종값을 제공한다. 결과는 stdout JSON 및 실행 중 `/results/report.json`, `/results/requests.json`, `/results/pods.json`에 있다. Job 완료 후 파일 시스템은 exec로 읽을 수 없으며 stdout은 다음 실행 전 별도 저장한다.
- 샘플 누락 없이 결과를 보존하려면 Job 실행 중 `/results`를 `kubectl cp`로 보관한다. 다음 run.sh는 이전 demo Job을 삭제하므로 그 전에 로그를 저장한다.

```bash
sudo k3s kubectl -n aegis logs job/recovery-demo > recovery-demo-result.log
```

Role은 `aegis` 네임스페이스의 pods get/list/delete 권한이다. Kubernetes RBAC는 label 단위 delete 제한을 제공하지 않아 코드에서 Gateway selector와 UID 사전조건으로 대상을 좁힌다. 운영 상시 서비스로 두지 않고 시연 종료 후 Job/RoleBinding/Role/ServiceAccount를 제거한다. 자동 재실행은 backoffLimit=0으로 금지했다.

## 탐지 화면으로 이어가기

복구 실행기는 정상 부하를 보내며 이상 데이터 주입 봇을 실행하지 않는다. 복구부터 탐지 검증까지 한 번에 진행하려면 run.sh 대신 아래 실행기를 사용한다.

```bash
bash deploy/k3s/demo/run-full.sh
```

복구 검증 통과 후 살아 있는 Gateway에서 봇을 Service 주소로 주입한다. 이번 시작 시각 이후 새로 생성된 L2/L6/L7 사건을 각각 기다리고 상세 API와 STR의 recompute가 동일하며 MATCH인지 검증한다. 사건 ID를 출력하므로 기존 관제 화면에서 해당 사건과 STR을 열어 보여준다. 이전 사건을 성공 근거로 사용하지 않는다. 탐지 흐름까지 통과하기 전에는 전체 시연 완료로 판정하지 않는다. UI에서 실제 경보 표시 여부는 별도 리허설로 확인한다.

run-full.sh는 Gateway 이미지의 Python·pymongo·tar와 Secret의 ADMIN_API_KEY를 사용한다. `deploy/k3s/demo/verify-crosscheck.py`가 사건 상세와 STR을 검증한다. Gateway 파드에는 재시작 시 사라지는 `/tmp/aegis-demo` 스크립트·봇 CSV가 생긴다. 장기 운영 설치가 아닌 시연 절차다.
