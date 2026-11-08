# RabbitMQ 인프라와 메시지 계약

이 문서는 백엔드와 AI 서버가 함께 따르는 RabbitMQ 구성과 메시지 계약을 정의한다. 원본 설계는 Notion의 「비동기 아키텍처 설계(RabbitMQ 테크스펙 문서)」이며, 구현은 다음 문서로 나뉜다.

- 백엔드 구현: [`../backend/RABBITMQ_BACKEND_IMPLEMENTATION.md`](../backend/RABBITMQ_BACKEND_IMPLEMENTATION.md)
- AI Worker 구현: [`../ai/RABBITMQ_AI_WORKER_IMPLEMENTATION.md`](../ai/RABBITMQ_AI_WORKER_IMPLEMENTATION.md)

이 문서의 이름, 값, 형식을 바꾸면 백엔드와 AI 문서를 함께 고친다. 큐, Exchange, binding은 **백엔드 `RabbitAdmin`만 선언**한다. AI 서버와 운영자는 선언하지 않는다.

---

## 1. 전체 흐름

```text
[API / 포토미션 스케줄러]
   │ 작업 행 + message_outbox 행을 한 트랜잭션으로 저장
   │ 커밋 직후 발행 (실패 시 Outbox Relay가 재발행)
   ▼
ai.request (direct) ──► ai.{작업}.task (quorum)
                              │ AI Worker 소비 (prefetch 1)
             ┌────────────────┼──────────────────────┐
             ▼                ▼                      ▼
     ai.retry (direct)   ai.result (direct)     ai.dlx (direct)
     ai.{작업}.retry.Ns   ai.result.task         ai.{작업}.dlq
     TTL 만료 시                │ 백엔드 Result Worker   │ 백엔드 DLQ Consumer
     ai.request로 복귀         ▼                      ▼ (포토미션 생성은 05:00 재처리)
                        작업 테이블 조건부 UPDATE + 알림 저장
                              │ 커밋 후
                              ▼
                     Redis Pub/Sub → WebSocket 알림
```

---

## 2. 배포 구성 (ECS Fargate 단일 노드)

### 2.1 태스크와 서비스

| 항목 | 값 | 이유 |
| --- | --- | --- |
| 이미지 | `rabbitmq:4.1-management` (배포 시 정확한 패치 버전으로 고정) | Quorum Queue, `x-delivery-limit`, Quorum Queue TTL 사용 |
| 태스크 크기 | 메모리 2GB | 결과 큐 200MB, 요청 큐 적재량 기준 |
| ECS 서비스 `desiredCount` | `1` | 단일 노드 |
| `minimumHealthyPercent` / `maximumPercent` | `0` / `100` | 두 노드가 같은 EFS 데이터를 동시에 쓰지 않도록 기존 태스크 종료 후 새 태스크 기동 |
| 접속 주소 | ECS Service Connect(또는 Cloud Map) DNS 이름 | 태스크 IP는 재시작마다 바뀜 |
| 포트 | `5672`(AMQP), `15672`(Management API, 백엔드 보안 그룹에서만 허용) | |

배포·재시작 중 RabbitMQ 중단 시간은 단일 노드 구성의 허용 범위로 본다.

### 2.2 저장소

| 항목 | 값 |
| --- | --- |
| 데이터 경로 | EFS를 `/var/lib/rabbitmq`에 마운트 |
| EFS Access Point | 소유자 uid/gid `999` (RabbitMQ 실행 사용자) |
| 노드 이름 | 환경 변수 `RABBITMQ_NODENAME=rabbit@localhost` (고정하지 않으면 재시작 시 빈 노드로 기동) |
| Erlang cookie | Secrets Manager 값을 `RABBITMQ_ERLANG_COOKIE`로 주입 |
| EFS 처리량 모드 | 발행 confirm 지연을 측정한 뒤 `CONFIRM_TIMEOUT`(5초)과 함께 확정 |

### 2.3 `rabbitmq.conf`

```ini
# 메모리 사용량이 전체의 60%를 넘으면 발행 차단
vm_memory_high_watermark.relative = 0.6
# 남은 디스크가 메모리의 1.2배 아래면 발행 차단
disk_free_limit.relative = 1.2
# 기본 guest 계정은 사용하지 않는다
loopback_users.guest = true
management.tcp.port = 15672
```

운영 경고(Sentry)는 백엔드 모니터 스케줄러가 별도로 보낸다(백엔드 문서 9절).

### 2.4 vhost와 사용자

vhost는 `planit` 하나를 쓴다. Management API 경로에서는 `/api/queues/planit/{queue}`로 쓴다.

| 사용자 | 태그 | configure | write | read | 용도 |
| --- | --- | --- | --- | --- | --- |
| `planit-backend` | 없음 | `^ai\..*` | `^ai\..*` | `^ai\..*` | 선언, Outbox 발행, Result Worker, DLQ Consumer, 05시 재처리 |
| `planit-ai` | 없음 | `^$` | `^ai\.(retry\|result\|dlx)$` | `^ai\..*\.task$` | 요청 소비, 결과·재시도·DLQ 발행 |
| `planit-monitor` | `monitoring` | `^$` | `^$` | `^$` | 백엔드 모니터 스케줄러의 Management API 조회 |

- `planit-ai`는 configure 권한이 없으므로 큐나 Exchange를 선언할 수 없다. AI 서버는 passive 조회만 한다.
- 비밀번호는 Secrets Manager에 두고 각 서비스에 환경 변수로 주입한다.

---

## 3. 토폴로지

### 3.1 Exchange

모두 `direct`, `durable=true`, `auto-delete=false`이다.

| Exchange | 용도 |
| --- | --- |
| `ai.request` | 작업 메시지를 작업별 Task Queue로 라우팅한다. Retry Queue에서 TTL이 끝난 메시지도 이 exchange로 돌아온다. |
| `ai.retry` | AI Worker와 Result Worker가 실패한 메시지를 다음 단계 Retry Queue로 발행한다. |
| `ai.dlx` | 재시도를 모두 쓴 메시지, 처리할 수 없는 메시지, `delivery-limit`을 넘은 메시지를 DLQ로 라우팅한다. |
| `ai.result` | AI 처리 결과를 응답 큐로 라우팅한다. Result Retry Queue에서 TTL이 끝난 메시지도 이 exchange로 돌아온다. |

### 3.2 작업별 값

| 작업 (`jobType`) | 이름 접두사 `{name}` | routing key `{rk}` | Task `x-max-length` | Retry TTL | DLQ `x-max-length` |
| --- | --- | --- | --- | --- | --- |
| 일정 생성 (`SCHEDULE_GENERATE`) | `ai.schedule-generation` | `ai-schedule-generation` | 3,450 | 5s / 10s / 20s | 50,000 |
| 포토미션 생성 (`PHOTO_MISSION_GENERATE`) | `ai.photomission-generation` | `ai-photomission-generation` | 10,500 | 5s / 10s / 20s | 10,500 |
| 포토미션 판정 (`PHOTO_JUDGE`) | `ai.photomission-verification` | `ai-photomission-verification` | 100 | 5s / 10s / 20s | 50,000 |
| 응답 | `ai.result` | `ai-result` | 200MB (`x-max-length-bytes: 209715200`) | 1s / 2s / 4s | 200MB (`x-max-length-bytes: 209715200`) |

Retry Queue의 `x-max-length`(응답은 `x-max-length-bytes`)는 같은 작업의 Task Queue 값과 같다.

### 3.3 요청 작업 큐 (작업 3종 공통 구조)

`{name}`, `{rk}`, `{max}`, `{dlqMax}`는 3.2절 표의 값이다.

| 큐 | binding (exchange / routing key) | 인자 |
| --- | --- | --- |
| `{name}.task` | `ai.request` / `{rk}` | `x-queue-type: quorum`, `x-max-length: {max}`, `x-overflow: reject-publish`, `x-dead-letter-exchange: ai.dlx`, `x-dead-letter-routing-key: {rk}.dlq`, `x-dead-letter-strategy: at-least-once`, `x-delivery-limit: 5` |
| `{name}.retry.5s` | `ai.retry` / `{rk}.retry.1` | `x-queue-type: quorum`, `x-max-length: {max}`, `x-overflow: reject-publish`, `x-message-ttl: 5000`, `x-dead-letter-exchange: ai.request`, `x-dead-letter-routing-key: {rk}`, `x-dead-letter-strategy: at-least-once` |
| `{name}.retry.10s` | `ai.retry` / `{rk}.retry.2` | 위와 같고 `x-message-ttl: 10000` |
| `{name}.retry.20s` | `ai.retry` / `{rk}.retry.3` | 위와 같고 `x-message-ttl: 20000` |
| `{name}.dlq` | `ai.dlx` / `{rk}.dlq` | `x-queue-type: quorum`, `x-max-length: {dlqMax}`, `x-overflow: reject-publish` |

### 3.4 응답 큐

| 큐 | binding (exchange / routing key) | 인자 |
| --- | --- | --- |
| `ai.result.task` | `ai.result` / `ai-result` | `x-queue-type: quorum`, `x-max-length-bytes: 209715200`, `x-overflow: reject-publish`, `x-dead-letter-exchange: ai.dlx`, `x-dead-letter-routing-key: ai-result.dlq`, `x-dead-letter-strategy: at-least-once`, `x-delivery-limit: 5` |
| `ai.result.retry.1s` | `ai.retry` / `ai-result.retry.1` | `x-queue-type: quorum`, `x-max-length-bytes: 209715200`, `x-overflow: reject-publish`, `x-message-ttl: 1000`, `x-dead-letter-exchange: ai.result`, `x-dead-letter-routing-key: ai-result`, `x-dead-letter-strategy: at-least-once` |
| `ai.result.retry.2s` | `ai.retry` / `ai-result.retry.2` | 위와 같고 `x-message-ttl: 2000` |
| `ai.result.retry.4s` | `ai.retry` / `ai-result.retry.3` | 위와 같고 `x-message-ttl: 4000` |
| `ai.result.dlq` | `ai.dlx` / `ai-result.dlq` | `x-queue-type: quorum`, `x-max-length-bytes: 209715200`, `x-overflow: reject-publish` |

### 3.5 큐 설정 규칙

- 모든 큐는 `durable=true`인 Quorum Queue다. 단일 노드라도 at-least-once dead-lettering과 `x-delivery-limit`을 쓰기 위해 Quorum Queue를 쓴다.
- at-least-once dead-lettering은 원본 큐에 `x-overflow: reject-publish`가 있어야 동작한다. 그래서 Task Queue와 Retry Queue 모두 `reject-publish`를 쓴다.
- DLQ가 가득 차면 dead-letter 대상이 거절되므로 메시지는 원본 큐에 남는다(유실되지 않음). 이 상황은 백엔드 모니터가 90%에서 먼저 차단한다.
- 큐 인자를 바꾸려면 큐를 지우고 다시 만들어야 한다(RabbitMQ는 기존 큐의 인자 변경을 거부한다). 백엔드 문서 15절의 절차를 따른다.

---

## 4. 메시지 계약

### 4.1 공통 Basic Properties

| 속성 | 값 | 설명 |
| --- | --- | --- |
| `content_type` | `application/json` | 본문은 UTF-8 JSON |
| `delivery_mode` | `2` | 영속 메시지 |
| `message_id` | UUID 문자열 | 요청: Outbox `message_id`. 결과: AI Worker가 새로 만든 UUID |
| `correlation_id` | 작업 ID 문자열 (`jobId`) | 요청, 결과, DLQ 메시지를 같은 작업으로 묶는다. 본문을 읽을 수 없는 DLQ 메시지도 이 값으로 작업을 찾는다. |
| `type` | `SCHEDULE_GENERATE`, `PHOTO_MISSION_GENERATE`, `PHOTO_JUDGE` (결과는 `_RESULT` 접미사) | Outbox `job_type`과 같은 값 |
| `timestamp` | 발행 시각 Unix epoch 초 | AMQP 표준 속성이므로 초 단위만 저장된다. |
| `app_id` | `planit-backend` 또는 `planit-ai` | 발행한 서버 |

모든 발행은 `mandatory=true`와 Publisher Confirms를 사용한다. `ack`이고 `basic.return`이 없을 때만 성공이다.

### 4.2 헤더

| 헤더 | 붙이는 곳 | 값 | 설명 |
| --- | --- | --- | --- |
| `x-retry-count` | 백엔드(최초 0), AI Worker | 0~4 | AI 처리 실패로 Retry Queue에 보낸 횟수. AI Worker가 실패 시점에 +1 한다. 4는 포토미션 생성 05:00 재처리 메시지다. AI Worker는 결과 메시지에도 요청의 값을 그대로 복사한다. |
| `x-result-retry-count` | 백엔드 Result Worker | 0~3 | 응답 큐 처리 실패로 Result Retry Queue에 보낸 횟수. `x-retry-count`와 섞이지 않게 따로 둔다. 없으면 0으로 본다. |
| `x-error-kind` | DLQ로 보낼 때 | 4.6절 값 | 실패 원인 분류 |
| `x-error-message` | DLQ로 보낼 때 | 최대 500자 | 마지막 오류 내용 |
| `x-failed-at` | DLQ로 보낼 때 | ISO-8601 (`+09:00`) | 최종 실패 시각 |

`x-delivery-limit` 초과로 RabbitMQ가 dead-letter한 메시지에는 `x-error-kind`가 없고, RabbitMQ가 붙인 `x-death`(`reason: delivery_limit`)가 있다. 이 경우 소비자는 `x-error-kind`를 `DELIVERY_LIMIT`으로 본다.

### 4.3 요청 메시지 본문

```json
{
  "schemaVersion": 1,
  "jobId": "123",
  "jobType": "SCHEDULE_GENERATE",
  "deadlineAt": "2026-11-05T12:28:00+09:00",
  "request": { }
}
```

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| `schemaVersion` | int | 현재 `1`. AI Worker는 모르는 버전이면 영구 오류로 처리한다. |
| `jobId` | string | 작업 테이블 ID. JavaScript 정밀도 문제를 피하려 문자열로 보낸다. |
| `jobType` | string | 4.1절 `type`과 같은 값 |
| `deadlineAt` | string (ISO-8601, `+09:00`) | Stale Job Checker가 이 작업을 실패 처리 대상으로 보는 시각. 지나면 AI Worker는 AI를 호출하지 않는다. |
| `request` | object | 작업별 AI 요청 본문 (4.5절) |

작업별 `deadlineAt`:

| 작업 | `deadlineAt` |
| --- | --- |
| 일정 생성 | 작업 생성 시각 + 28분 |
| 포토미션 생성 | 작업 생성일 06:00 (KST) |
| 포토미션 판정 | 작업 생성 시각 + 16분 |

### 4.4 결과 메시지 본문

```json
{
  "schemaVersion": 1,
  "jobId": "123",
  "jobType": "SCHEDULE_GENERATE",
  "requestMessageId": "0192f3a1-...",
  "status": "SUCCEEDED",
  "errorKind": null,
  "result": { }
}
```

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| `requestMessageId` | string | 처리한 요청 메시지의 `message_id` |
| `status` | `SUCCEEDED` \| `FAILED` | AI 처리 성공 여부. 판정 결과가 "미션 실패"여도 판정 자체를 마쳤으면 `SUCCEEDED`다. |
| `errorKind` | string \| null | `status=FAILED`일 때 4.6절 값 |
| `result` | object \| null | `SUCCEEDED`일 때 작업별 AI 응답 본문 (4.5절), `FAILED`면 `null` |

`status=FAILED` 결과는 `x-retry-count=4`(포토미션 생성 05:00 재처리)에서 실패했을 때만 발행한다. 그 밖의 실패는 Retry Queue 또는 DLQ로 간다.

### 4.5 작업별 `request`와 `result`

기존 동기 HTTP API의 요청·응답 JSON을 그대로 쓴다(필드 이름은 snake_case 유지).

| 작업 | 기존 HTTP API | `request` | `result` |
| --- | --- | --- | --- |
| 일정 생성 | `POST /trips/place-selection` | 기존 요청 본문 (`region`, `start_date`, `end_date`, `members`) | 기존 응답 본문 (`status_code`, `data.places`, `data.routes`) |
| 포토미션 생성 | `POST /photomissions/generate` | 기존 요청 본문 (`itinerary_id`, `places`) | 기존 응답 본문 (`missions`) |
| 포토미션 판정 | `POST /photomissions/{missionId}/verify` | 기존 요청 본문 + `mission_id`(경로 변수였던 값, 문자열) | 기존 응답 본문 (`result`, `reason`, `match_score`, `detected_labels`, `landmark_confidence`, `retry_hint`) |

포토미션 판정의 `image_url`은 백엔드가 만료 시각을 `deadlineAt + 30초`(판정 AI timeout)로 발급한 presigned URL이다.

### 4.6 `errorKind` 값

| 값 | 분류 | 의미 |
| --- | --- | --- |
| `AI_TIMEOUT` | 재시도 가능 | AI 처리 시간 초과 |
| `AI_UNAVAILABLE` | 재시도 가능 | AI 서버 내부 오류, RunPod·Gemini 429/5xx, 네트워크 오류, AI 서버 DB 일시 오류 |
| `INVALID_MESSAGE` | 재시도 불가 | 본문 파싱 실패, 필수 필드 누락 |
| `UNSUPPORTED_SCHEMA_VERSION` | 재시도 불가 | 지원하지 않는 `schemaVersion` |
| `INVALID_REQUEST` | 재시도 불가 | `request` 값이 AI 입력 검증을 통과하지 못함 |
| `IMAGE_NOT_FOUND` | 재시도 불가 | 판정 사진 없음(404) |
| `UNSUPPORTED_IMAGE` | 재시도 불가 | 지원하지 않는 이미지 형식 |
| `DELIVERY_LIMIT` | — | RabbitMQ가 `x-delivery-limit` 초과로 dead-letter함 (소비자가 판별) |

---

## 5. 처리 경로 요약

| 상황 | 처리 주체 | 목적지 |
| --- | --- | --- |
| AI 성공 | AI Worker | `ai.result` / `ai-result` |
| 재시도 가능 오류, `x-retry-count` < 3 | AI Worker | `ai.retry` / `{rk}.retry.{x-retry-count+1}`, 헤더 +1 |
| 재시도 가능 오류, `x-retry-count` = 3 | AI Worker | `ai.dlx` / `{rk}.dlq` |
| 재시도 불가 오류 (파싱 실패 포함) | AI Worker | `ai.dlx` / `{rk}.dlq` |
| `x-retry-count` = 4에서 실패 (오류 종류 무관) | AI Worker | `ai.result` / `ai-result`, `status=FAILED` |
| `deadlineAt` 지남 | AI Worker | 발행 없이 ACK |
| 결과 발행이 계속 실패해 재전달 5회 초과 | RabbitMQ | `ai.dlx` / `{rk}.dlq` (`x-death`) |
| Result Worker 일시 오류, `x-result-retry-count` < 3 | 백엔드 | `ai.retry` / `ai-result.retry.{n+1}` |
| Result Worker 일시 오류, `x-result-retry-count` = 3 | 백엔드 | `ai.dlx` / `ai-result.dlq` |

---

## 6. 운영 확인 항목

| 항목 | 방법 |
| --- | --- |
| 토폴로지 확인 | Management UI의 Queues 탭에서 20개 큐(작업 4종 × task·retry 3·dlq)와 인자 확인 |
| 노드 재시작 후 데이터 유지 | 테스트 큐에 메시지를 넣고 ECS 태스크를 재시작한 뒤 메시지 수 확인 |
| 단일 태스크 보장 | 배포 중 `RunningCount`가 2가 되지 않는지 확인 |
| confirm 지연 | 백엔드 발행 confirm 지연 max를 측정해 `CONFIRM_TIMEOUT`(현재 5초)을 확정 |
