# PlanIt 메시지 큐 기반 AI 작업 처리 구현 가이드

## 1. 문서 목적과 상태

이 문서는 일정 생성, 포토미션 생성, 사진 판정 세 가지 AI 작업을 RabbitMQ로 비동기 처리하기 위한 구조, 테이블, 메시지 계약, 구현 순서와 검증 기준을 정리한다.

- 일정 생성 HTTP 계약은 [`SCHEDULE_API_SPEC.md`](../docs/SCHEDULE_API_SPEC.md)의 6절 `POST /api/trips/{tripId}/schedule-generation-jobs`를 기준으로 한다.
- 사진 판정 HTTP 계약은 [`PHOTO_MISSION_API_SPEC.md`](../docs/PHOTO_MISSION_API_SPEC.md)를 기준으로 한다.
- 메시지 브로커 선정 배경은 [`TECH_STACK.md`](./TECH_STACK.md)의 Message Queue 절을 기준으로 한다.
- 전체 구성도는 [Figma: Planit RabbitMQ 구성도](https://www.figma.com/design/cHvtG1hhQey4e1YxnxLD5F)를 기준으로 한다.
- 이 문서의 내용은 구현 예정 계약이며 코드·마이그레이션·통합 테스트가 확인되기 전까지 `IMPLEMENTED` 또는 `VERIFIED`가 아니다.

### 1.1 구현 현황

- 상태 기준
  - `NOT_STARTED`: 코드 구현을 시작하지 않음
  - `PARTIAL`: 범위 중 일부만 구현됨
  - `IMPLEMENTED`: 코드 구현은 완료됐지만 완료 조건 전체가 검증되지 않음
  - `VERIFIED`: 관련 코드와 명시된 테스트가 현재 체크아웃에서 통과함
- 이슈는 아직 생성하지 않았다. 이슈를 만들면 아래 표의 `범위` 열에 링크를 추가한다.

| 순서 | 범위 | 상태 | 담당 문서 범위 | 다음 시작점 |
| ---: | --- | --- | --- | --- |
| 1 | RabbitMQ 인프라와 큐 토폴로지 | `NOT_STARTED` | 5절 | Docker Compose에 RabbitMQ 추가, 선언 코드로 exchange·queue·binding 생성 |
| 2 | `message_outbox` 테이블과 작업 테이블 상태 정비 | `NOT_STARTED` | 6절 | Flyway `V41` 이후 버전으로 Outbox 생성, 6.3절 작업 테이블 제약·상태 변경 |
| 3 | Outbox 발행과 Relay | `NOT_STARTED` | 7절 | 커밋 직후 발행, `@Scheduled` Relay, Publisher Confirm |
| 4 | AI Worker 처리 규약 | `NOT_STARTED` | 8절 | FastAPI 측 consumer, 결과 발행 후 ACK, Retry Queue 재발행 |
| 5 | Result Worker와 DLQ 처리 | `NOT_STARTED` | 9절 | 작업 유형별 조건부 상태 갱신, DLQ 소비 |
| 6 | 작업별 전환 | `NOT_STARTED` | 10절 | 사진 판정 → 포토미션 생성 → 일정 생성 순서로 기존 동기 호출 교체 |
| 7 | 완료 알림 | `NOT_STARTED` | 11절 | Redis Pub/Sub `planit:ai-jobs` 발행·구독과 WebSocket `/user/queue/ai-jobs` 전송, 상태 조회 복구 |
| 8 | 장애·유실 시나리오 통합 테스트 | `NOT_STARTED` | 12절, 15절 | 브로커·AI·백엔드 장애 시나리오 검증 |

## 2. 도입 배경과 범위

### 2.1 현재 처리 방식

| 작업 | 현재 시작 지점 | 현재 AI 호출 방식 | 현재 재시도 |
| --- | --- | --- | --- |
| 일정 생성 | 방장의 생성 요청 | 요청 스레드에서 동기 호출 | AI 호출은 재시도하지 않음. 장소 저장만 최대 3회 재시도 |
| 포토미션 생성 | `PhotoMissionGenerationScheduler` (5분마다, ShedLock `photoMissionGeneration`) | 스케줄러 스레드에서 동기 호출 | `mission_generation_jobs.attempt_count` 기준 |
| 사진 판정 | 사진 제출 요청 | `PhotoMissionVerificationService.verify`에서 동기 호출 | 429·502·504·네트워크 오류는 `RETRY_PENDING`과 `retry_after_at`으로 기록하고 `PhotoMissionEvaluationRetryScheduler`가 30초마다 재시도 |

- AI 응답이 늦거나 AI 서버에 장애가 나면 요청 스레드와 스케줄러가 AI 응답을 기다리며 묶인다.
- 일시적 장애가 끝난 직후 재시도가 한꺼번에 몰리면 새 요청의 처리도 함께 늦어진다.
- 백엔드 인스턴스가 AI 호출 도중 종료되면 진행 중이던 작업을 이어서 처리할 수단이 작업별 스케줄러에 흩어져 있다.

### 2.2 범위

- 대상: 일정 생성, 포토미션 생성, 사진 판정
- 제외: 알림용 AI `/trips/precheck` 호출. 하루 한 번 실행하는 스케줄 작업이며 [`NOTIFICATION_IMPLEMENTATION_GUIDE.md`](./NOTIFICATION_IMPLEMENTATION_GUIDE.md)의 HTTP 계약을 그대로 사용한다.
- 브로커는 RabbitMQ 4.x의 Quorum Queue를 사용한다.
- `rabbitmq_delayed_message_exchange`(`x-delay`)는 지원 종료와 Quorum Queue 비호환 때문에 사용하지 않는다. 지연 재시도는 Queue 단위 TTL을 가진 Retry Queue로 구현한다.

## 3. 전체 구조

```text
Client ──① 요청──▶ Backend API ──② 작업 행 + Outbox 행 (한 트랜잭션) ──▶ MySQL
   ▲                   │ 커밋 후 202 Accepted(jobId)
   │                   ├──③ 커밋 직후 즉시 발행 ──────────────────▶ [ai.request exchange]
   │                   ▼                                                     ▲
   │             Outbox Relay (안전망) ──③ 즉시 발행에 실패한 행 재발행 ──────┘
   │                                                          ④
   │                                                          │ routing key
   │                                    ┌─────────────────────┼─────────────────────┐
   │                                    ▼                     ▼                     ▼
   │                          schedule.generate     photo-mission.generate     photo.judge
   │                          (Task·Retry·DLQ)       (Task·Retry·DLQ)          (Task·Retry·DLQ)
   │                                    └──────────⑤ 소비──────▶ AI Worker (FastAPI)
   │                                                          │ ⑥ 실패 시 Retry Queue 또는 DLQ
   │                                                          │ ⑦ 결과 발행(confirm) 후 ACK
   │                                                          ▼
   │                                               [ai.result exchange] → ai.result (Retry·DLQ)
   │                                                          │ ⑧ 소비
   │                                                          ▼
   └──⑨ WebSocket ◀── 각 인스턴스 ◀── Redis Pub/Sub ◀── Result Worker ── 작업 테이블 조건부 갱신
```

### 3.1 구성요소 책임

| 구성요소 | 위치 | 책임 |
| --- | --- | --- |
| 작업 요청 API | Backend | 입력 검증, 작업 행과 Outbox 행을 한 트랜잭션으로 저장, 커밋 직후 즉시 발행 시도, `202 Accepted` |
| 작업 테이블 | MySQL | 작업 처리 상태의 기준. `schedule_generation_jobs`, `mission_generation_jobs`, `photo_evaluations` |
| `message_outbox` | MySQL | 발행해야 할 메시지와 발행 상태 |
| Outbox Relay | Backend | 안전망. 즉시 발행에 실패했거나 서버 종료로 남은 `PENDING` 행 재발행, Publisher Confirm 확인, 발행 상태 갱신 |
| `ai.request` exchange | RabbitMQ | routing key에 따라 작업별 Task Queue로 라우팅 |
| Retry Queue | RabbitMQ | TTL 동안 메시지를 보관한 뒤 Task Queue로 되돌림 |
| DLQ | RabbitMQ | 재시도를 모두 사용했거나 처리할 수 없는 메시지 보관 |
| AI Worker | FastAPI | 작업 소비, AI 처리, 결과 발행 후 ACK, 실패 시 재시도 경로 선택 |
| `ai.result` 큐 | RabbitMQ | 세 작업이 공유하는 결과 큐 |
| Result Worker | Backend | 결과를 작업 유형별 테이블에 멱등하게 반영, 완료 알림 발행 |
| DLQ Consumer | Backend | DLQ 메시지의 작업을 끝난 상태가 아닐 때만 `FAILED`로 변경 |
| Stale Job Checker | Backend | 다른 안전장치가 놓쳐 너무 오래 `QUEUED`·`RUNNING`에 머문 작업을 `FAILED`로 정리하고 실패 알림 발행 |
| Redis Pub/Sub | Redis | 완료 알림을 모든 백엔드 인스턴스로 전파 |
| WebSocket | Backend | 이벤트를 받은 인스턴스가 연결된 수신자에게 `/user/queue/ai-jobs`로 전달 |

## 4. 처리 흐름

| 단계 | 처리 | 기준 |
| --- | --- | --- |
| ① | 클라이언트가 작업을 요청한다. 포토미션 생성은 스케줄러가 시작한다 | 일정 생성은 `Idempotency-Key` 필수 |
| ② | 작업 행(`QUEUED`)과 Outbox 행(`PENDING`)을 한 트랜잭션으로 저장한다 | 커밋이 성공한 뒤에만 `202 Accepted(jobId)`를 반환한다 |
| ③ | 커밋 직후 같은 요청에서 즉시 발행을 시도한다. 실패하거나 서버가 종료되면 Relay가 `PENDING` 행을 다시 발행한다 | ack이면서 return이 없을 때만 `PUBLISHED`. Relay는 안전망이다 |
| ④ | `ai.request` exchange가 routing key로 작업별 Task Queue에 넣는다 | `mandatory=true`, 라우팅 실패는 return으로 받는다 |
| ⑤ | AI Worker가 작업을 소비해 처리한다 | `prefetch=1` |
| ⑥ | 일시적 실패나 Timeout이면 다음 단계 Retry Queue로, 재시도를 다 썼거나 처리할 수 없는 메시지면 DLQ로 보낸다 | 8.3절 |
| ⑦ | 결과를 `ai.result`에 발행하고 confirm을 받은 뒤 원본 메시지를 ACK한다 | 결과 발행 전에 ACK하지 않는다 |
| ⑧ | Result Worker가 결과를 작업 테이블에 조건부로 반영하고 커밋 후 ACK한다 | 이미 끝난 작업이면 반영 없이 ACK |
| ⑨ | 상태가 실제로 바뀐 경우에만 Redis Pub/Sub로 완료 알림을 발행하고, 각 인스턴스가 WebSocket으로 전달한다 | 유실은 상태 조회로 복구한다 |

## 5. RabbitMQ 토폴로지

### 5.1 Exchange

| Exchange | 종류 | 용도 |
| --- | --- | --- |
| `ai.request` | direct | 작업 요청을 작업별 Task Queue로 라우팅 |
| `ai.retry` | direct | 실패한 작업을 작업별 Retry Queue로 라우팅 |
| `ai.result` | direct | AI 결과를 공용 결과 큐로 라우팅 |
| `ai.dlx` | direct | 처리 불가 메시지와 `delivery-limit` 초과 메시지를 DLQ로 라우팅 |

### 5.2 큐 이름 규칙

```text
ai.{job}.task                     # Task Queue
ai.{job}.retry.{tier}.{sub}       # Retry Queue, tier = 1..N, sub = a..k
ai.{job}.dlq                      # DLQ
ai.result.task / ai.result.retry.{tier}.{sub} / ai.result.dlq
```

| `{job}` | routing key |
| --- | --- |
| `schedule-generate` | `schedule.generate` |
| `photo-mission-generate` | `photo-mission.generate` |
| `photo-judge` | `photo.judge` (임시 이름, 14절) |

### 5.3 큐 공통 설정

| 인자 | 값 | 이유 |
| --- | --- | --- |
| `x-queue-type` | `quorum` | 복제 기반 내구성 |
| `x-delivery-limit` | 미정 | Worker가 ACK 전에 계속 종료되는 메시지를 무한 재전달하지 않도록 함 |
| `x-dead-letter-exchange` | `ai.dlx` | 처리 불가·`delivery-limit` 초과 메시지 보관 |
| `x-dead-letter-strategy` | `at-least-once` | 기본값 `at-most-once`는 DLQ 이동 중 유실될 수 있음 |
| `x-overflow` | `reject-publish` | `at-least-once` dead-lettering의 필수 조건 |
| `x-max-length` | 미정 | 큐 길이 상한. 없으면 `reject-publish`가 동작하지 않음 |

- RabbitMQ 4.x Quorum Queue의 `delivery-limit` 기본값은 20이므로 값을 정해 명시적으로 설정한다.
- Quorum Queue의 우선순위는 2단계(0~4 일반, 5 이상 높음)뿐이다. 새 요청은 우선순위 5, 재시도 메시지는 0으로 발행해 재시도가 새 요청을 밀어내지 않게 한다.

### 5.4 Retry Queue와 Jitter

- Retry Queue는 메시지별 TTL이 아닌 **Queue 단위 TTL(`x-message-ttl`)**을 사용한다. 메시지별 TTL은 큐 맨 앞 메시지만 만료를 검사하므로 뒤 메시지가 늦게 나온다.
- Retry Queue에는 소비자가 없다. TTL이 지나면 `x-dead-letter-exchange=ai.request`, `x-dead-letter-routing-key={job의 routing key}`로 Task Queue에 돌아간다.
- 한 단계(tier)에 TTL이 조금씩 다른 sub-queue를 k개 두고, Worker가 무작위로 고른다. 같은 시점에 실패한 작업이 같은 시점에 Task Queue로 몰리는 것을 막는다.

단계 수(N), sub-queue 수(k), 단계별 TTL은 미정이다(14절).

| tier | sub-queue TTL | 의미 |
| --- | --- | --- |
| 1 | 미정 | 첫 재시도 |
| 2 ~ N−1 | 미정 | 중간 재시도 |
| N | 미정 | 마지막 재시도, 이후 실패 시 DLQ |

결과 큐도 같은 구조를 사용하며 tier 수는 M으로 따로 정한다.

### 5.5 선언 방식

- exchange·queue·binding은 백엔드의 Spring AMQP `Declarables` Bean으로 선언한다. AI 서버는 선언하지 않고 소비·발행만 한다.
- Quorum Queue 인자는 생성 후 바꿀 수 없다. 인자를 바꿀 때는 새 이름으로 큐를 만들고 옮긴 뒤 기존 큐를 지운다.

## 6. DB 설계

### 6.1 `message_outbox`

```sql
CREATE TABLE `message_outbox` (
    `id`               BIGINT       NOT NULL AUTO_INCREMENT COMMENT '메시지 발행 Outbox ID',
    `job_type`         VARCHAR(40)  NOT NULL COMMENT 'SCHEDULE_GENERATE, PHOTO_MISSION_GENERATE, PHOTO_JUDGE',
    `job_id`           BIGINT       NOT NULL COMMENT 'job_type별 작업 테이블의 ID',
    `message_id`       BINARY(16)   NOT NULL COMMENT '메시지 ID(중복 확인용)',
    `exchange_name`    VARCHAR(100) NOT NULL COMMENT '발행할 exchange',
    `routing_key`      VARCHAR(100) NOT NULL COMMENT '라우팅 키',
    `payload`          JSON         NOT NULL COMMENT '메시지 본문',
    `status`           VARCHAR(20)  NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING, PUBLISHED, FAILED (발행 상태)',
    `attempt_count`    TINYINT      NOT NULL DEFAULT 0 COMMENT '발행 시도 횟수',
    `next_attempt_at`  DATETIME(6)  NOT NULL COMMENT '다음 발행 시도 시각(선점 lease 겸용)',
    `last_error`       VARCHAR(500) NULL     COMMENT '마지막 발행 실패 이유',
    `created_at`       DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    `published_at`     DATETIME(6)  NULL     COMMENT '발행(confirm) 완료 시각',
    CONSTRAINT `PK_MESSAGE_OUTBOX` PRIMARY KEY (`id`),
    CONSTRAINT `uk_message_outbox_message_id` UNIQUE (`message_id`),
    INDEX `idx_message_outbox_poll` (`status`, `next_attempt_at`),
    INDEX `idx_message_outbox_job` (`job_type`, `job_id`)
);
```

- 실제 마이그레이션은 신규 Flyway 버전으로 추가하고 기존 마이그레이션을 수정하지 않는다.
- 세 작업이 테이블 하나를 공유한다. Relay는 작업 내용을 해석하지 않고 exchange, routing key, payload만 사용한다.
- `job_id`는 `job_type`에 따라 다른 테이블을 가리키므로 FK를 걸지 않는다.
- 작업 하나에 메시지가 여러 개일 수 있다(사용자 재요청, 이후 취소 메시지 등). `(job_type, job_id)`에 UNIQUE를 걸지 않는다.
- `message_id`는 UUIDv7을 `BINARY(16)`으로 저장하고, AMQP `message-id` 속성에는 문자열로 넣는다. Hibernate 매핑은 `@JdbcTypeCode(SqlTypes.BINARY)`로 명시한다.
- `exchange_name`은 현재 모든 행이 `ai.request`다. 다른 exchange로 발행할 메시지가 생기지 않는다면 컬럼을 빼고 설정값으로 대체해도 된다.

#### 컬럼별 역할

| 컬럼 | 역할 |
| --- | --- |
| `status` | 발행 여부만 나타낸다. 작업 처리 상태가 아니다 |
| `attempt_count` | 발행 재시도 횟수. 한도(미정)를 넘으면 `FAILED` |
| `next_attempt_at` | 저장할 때 유예 시간만큼 미래로 둬 즉시 발행 중인 행을 Relay가 가져가지 않게 한다. 실패 시 백오프 간격만큼 뒤로 미룬다. Relay가 행을 가져갈 때도 미래로 미뤄 다른 Relay가 같은 행을 가져가지 못하게 한다 |
| `last_error` | Relay가 confirm·return·예외에서 얻은 실패 이유를 분류 코드와 함께 기록한다. payload는 넣지 않는다 |

#### 발행 상태

| 상태 | 의미 | 다음 처리 |
| --- | --- | --- |
| `PENDING` | 저장 직후 상태. 아직 ack(return 없음)를 받지 못함 | Relay가 `next_attempt_at` 이후 발행 |
| `PUBLISHED` | ack을 받았고 return이 없음 | 보관 기간(미정) 후 삭제 |
| `FAILED` | 발행 재시도 한도 초과, 라우팅 실패(return), Stale Job Checker가 작업을 `FAILED` 처리 | 작업도 `FAILED`로 변경, 원인 해결 후 수동으로 `PENDING` 복구 가능 |

- 행은 항상 `PENDING`으로 저장하고, 발행이 성공했다는 증거(ack, return 없음)를 받은 뒤에만 `PUBLISHED`로 바꾼다.
- 발행 전에 `PUBLISHED`로 저장하고 실패 시 `PENDING`으로 되돌리는 방식은 쓰지 않는다. 커밋 직후 서버가 종료되거나 confirm을 기다리다 종료되면 실패를 기록할 기회가 없어, 발행되지 않은 메시지가 `PUBLISHED`로 남고 Relay도 다시 보내지 않는다.

### 6.2 작업 테이블과의 관계

작업 처리 상태는 새 공통 테이블을 만들지 않고 기존 작업 테이블이 계속 담당한다.

| `job_type` | 작업 테이블 | `job_id` | 처리 상태 컬럼 |
| --- | --- | --- | --- |
| `SCHEDULE_GENERATE` | `schedule_generation_jobs` | `id` | `status` (QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED) |
| `PHOTO_MISSION_GENERATE` | `mission_generation_jobs` | `id` | `status` (RUNNING, SUCCEEDED, FAILED) |
| `PHOTO_JUDGE` | `photo_evaluations` | `id` | `execution_status` (QUEUED, RUNNING, SUCCEEDED, FAILED, 6.3.3 변경 후) |

- 공통 작업 테이블을 만들면 상태가 두 곳에 생기고, `schedules.generation_job_id` 등 기존 FK를 모두 다시 연결해야 한다.
- Outbox는 발행 상태, 작업 테이블은 처리 상태를 담당한다. 발행된 작업도 AI 처리 중이거나 실패할 수 있으므로 두 상태를 섞지 않는다.
- 세 테이블 모두 **행 하나가 작업 하나**이고, AI 기술적 재시도는 같은 행에서 처리한다. 그래야 메시지의 `jobId`가 재시도 중에도 바뀌지 않는다.
- 진행 중인 작업의 중복은 `active_slot` 패턴으로 막는다. 진행 중(QUEUED, RUNNING)이면 `active_slot = 1`, 끝나면 `NULL`로 두고 `UNIQUE (대상 ID, active_slot)`을 건다. UNIQUE 인덱스에서 NULL끼리는 충돌하지 않으므로 끝난 작업은 여러 개 남고 진행 중인 작업은 대상당 하나만 존재한다. `schedules`, `mission_photos`, `chat_policy_versions`가 이미 같은 패턴을 사용한다.

### 6.3 작업 테이블 변경 요구사항

- 실제 변경은 신규 Flyway 버전으로 추가하고 기존 마이그레이션을 수정하지 않는다.
- 세 테이블 모두 오래 머문 작업을 찾을 수 있도록 `(상태, 요청 시각)` 인덱스를 둔다.

#### 6.3.1 `schedule_generation_jobs`

현재 테이블은 V5에서 만들어졌지만 백엔드에 Entity·Repository가 없고, 동기 일정 생성 API는 이 테이블을 사용하지 않는다. 컬럼 주석에 적힌 중복 방지 규칙에 대응하는 제약도 없다.

```sql
ALTER TABLE `schedule_generation_jobs`
    ADD CONSTRAINT `uk_schedule_generation_jobs_trip_active`
        UNIQUE (`trip_id`, `active_slot`),
    ADD CONSTRAINT `uk_schedule_generation_jobs_idempotency_key`
        UNIQUE (`idempotency_key`),
    ADD INDEX `idx_schedule_generation_jobs_stale` (`status`, `created_at`);
```

- 작업 생성 시 `status = 'QUEUED'`, `active_slot = 1`로 저장한다. 같은 여행에 진행 중인 작업이 있으면 UNIQUE 위반으로 막힌다.
- `SUCCEEDED`, `FAILED`, `CANCELLED`로 바꿀 때 같은 UPDATE에서 `active_slot = NULL`로 바꾼다.
- `idempotency_key` UNIQUE 위반이면 새 작업을 만들지 않고 기존 작업의 `jobId`를 반환한다.

#### 6.3.2 `mission_generation_jobs`

```sql
ALTER TABLE `mission_generation_jobs`
    MODIFY COLUMN `status` VARCHAR(20) NOT NULL
        COMMENT 'QUEUED, RUNNING, SUCCEEDED, FAILED',
    ADD INDEX `idx_mission_generation_jobs_stale` (`status`, `last_attempted_at`);
```

- 발행 후 처리 전 상태인 `QUEUED`를 추가한다.
- `UNIQUE (trip_id)`로 여행당 작업 행이 하나이므로 `active_slot`은 필요 없다. 스케줄러는 `FAILED` 작업을 다시 시작할 때 같은 행을 `QUEUED`로 되돌리고 `attempt_count`를 늘린다.

#### 6.3.3 `photo_evaluations`

현재 구조는 **행 하나가 AI 호출 한 번**이다. 재시도할 때마다 이전 행을 `FAILED`로 바꾸고 `attempt_no + 1`인 새 행을 만든다. 비동기 처리에는 다음 문제가 있다.

| 문제 | 영향 |
| --- | --- |
| 재시도마다 행이 바뀜 | Retry Queue에서 돌아온 메시지의 `jobId`가 이미 `FAILED`가 된 이전 행을 가리킨다 |
| `attempt_no`가 사진별 요청 순번과 기술적 재시도 횟수를 함께 셈 | 기술적 재시도 한도와 요청 순번을 구분할 수 없다 |
| `QUEUED` 상태 없음, 생성 즉시 `RUNNING` | API 응답의 `evaluationStatus=QUEUED`와 DB 상태가 다르다. 큐 대기와 AI 처리 중을 구분할 수 없다 |
| 진행 중 판정 중복 방지 없음 | 최대 `attempt_no`를 잠금 없이 읽고 +1 하므로 같은 사진에 판정이 동시에 진행될 수 있다. 경합은 `uk_photo_evaluations_photo_attempt` 위반 예외로만 드러난다 |
| `RETRY_PENDING`, `retry_after_at`, 30초 재시도 스케줄러 | MQ의 Retry Queue와 역할이 겹친다 |

변경 방향은 **판정 요청 1건 = 행 1개**다.

- `attempt_no`는 "이 사진에 대한 몇 번째 판정 요청인가"만 나타내고 기술적 재시도로는 늘리지 않는다.
- AI 일시 오류로 재시도해도 같은 행을 유지하고 `execution_attempt_count`만 늘린다. Result Worker와 DLQ Consumer가 메시지의 `x-retry-count + 1`로 기록한다.
- 진행 중(`QUEUED`, `RUNNING`)이면 `active_slot = 1`, 끝나면(`SUCCEEDED`, `FAILED`) `NULL`로 둔다.
- 재시도는 MQ의 Retry Queue가 맡는다. 전환이 끝나면 `RETRY_PENDING`, `retry_after_at`, `idx_photo_evaluations_retry_pending`, `PhotoMissionEvaluationRetryScheduler`를 제거한다. 기존 동기 경로를 플래그로 남겨 두는 전환 기간에는 유지한다.
- `recoverStaleRunning`의 지연 작업 복구는 9.4절 Stale Job Checker로 옮기며, 재시도 대기 대신 `FAILED`로 처리한다.

```sql
ALTER TABLE `photo_evaluations`
    ADD COLUMN `execution_attempt_count` TINYINT NOT NULL DEFAULT 0
        COMMENT 'AI 호출 기술적 재시도 횟수'
        AFTER `attempt_no`,
    ADD COLUMN `active_slot` TINYINT NULL
        COMMENT '진행 중인 판정일 때만 1'
        AFTER `execution_status`,
    MODIFY COLUMN `attempt_no` TINYINT NOT NULL
        COMMENT '사진 기준 판정 요청 순서 (기술적 재시도로 증가하지 않음)',
    MODIFY COLUMN `execution_status` VARCHAR(20) NOT NULL
        COMMENT 'QUEUED, RUNNING, SUCCEEDED, FAILED',
    ADD CONSTRAINT `uk_photo_evaluations_photo_active`
        UNIQUE (`mission_photo_id`, `active_slot`);

-- 전환 완료 후 별도 버전
ALTER TABLE `photo_evaluations`
    DROP INDEX `idx_photo_evaluations_retry_pending`,
    DROP COLUMN `retry_after_at`;
```

- 기존 `RETRY_PENDING` 행은 전환 마이그레이션에서 `FAILED`로 정리하거나, 전환 전에 기존 스케줄러가 모두 처리하도록 기다린다.
- 기존 `RUNNING`·`RETRY_PENDING` 행이 남아 있으면 `active_slot`을 채우는 데이터 정리가 함께 필요하다. 같은 사진에 진행 중 행이 둘 이상이면 UNIQUE 추가가 실패하므로 마이그레이션 전에 확인한다.

## 7. 발행: Transactional Outbox

### 7.1 요청 트랜잭션

```java
@Transactional
public ScheduleGenerationJobResponse start(Long tripId, UUID idempotencyKey) {
    ScheduleGenerationJob job = jobRepository.save(ScheduleGenerationJob.queued(tripId, idempotencyKey));
    outboxRepository.save(MessageOutbox.pending(
            JobType.SCHEDULE_GENERATE, job.getId(), "schedule.generate", payloadOf(job)));
    return ScheduleGenerationJobResponse.from(job);   // 커밋 후 컨트롤러가 202 반환
}
```

- Outbox 행은 `PENDING`으로 저장하고 `next_attempt_at`은 `NOW(6) + 유예 시간`으로 둔다. 유예 시간은 confirm 대기 시간(7.4절)보다 길게 잡아, 즉시 발행이 진행 중인 행을 Relay가 함께 발행하지 않게 한다.
- 커밋 후(`@TransactionalEventListener(phase = AFTER_COMMIT)`) 같은 요청에서 즉시 발행을 시도한다. 결과 처리는 Relay와 같은 규칙(7.4절)을 따른다.
- 즉시 발행이 실패해도 요청은 성공으로 응답한다. 행은 `PENDING`으로 남고 유예 시간이 지나면 Relay가 발행한다.
- 커밋 전에는 절대 `202`를 반환하거나 발행하지 않는다.

### 7.2 Publisher 설정

```yaml
spring:
  rabbitmq:
    publisher-confirm-type: correlated
    publisher-returns: true
    template:
      mandatory: true
```

### 7.3 Relay

- 백엔드 인스턴스마다 `@Scheduled(fixedDelay = 미정)`로 실행한다. ShedLock은 사용하지 않고, `FOR UPDATE SKIP LOCKED`로 여러 인스턴스가 서로 다른 행을 나눠 처리한다.
- 선점은 짧은 트랜잭션에서 하고, 발행은 트랜잭션 밖에서 한다. 발행 대기 중 DB 커넥션을 잡지 않기 위함이다.

```sql
SELECT id FROM message_outbox
WHERE status = 'PENDING' AND next_attempt_at <= NOW(6)
ORDER BY id
LIMIT {배치 크기: 미정}
FOR UPDATE SKIP LOCKED;
-- 같은 트랜잭션에서 next_attempt_at = NOW(6) + {lease 시간: 미정}으로 선점한 뒤 커밋
```

### 7.4 발행 결과 처리

```java
CorrelationData correlation = new CorrelationData(row.messageIdText());
rabbitTemplate.send(row.getExchangeName(), row.getRoutingKey(), toMessage(row), correlation);
CorrelationData.Confirm confirm = correlation.getFuture().get(confirmTimeout);   // 대기 시간 미정
// 라우팅 실패에도 ack가 오므로 return을 먼저 확인한다
```

즉시 발행과 Relay가 같은 규칙을 쓴다. **ack이면서 return이 없을 때만 `PUBLISHED`로 바꾸고**, 나머지는 `PENDING`을 유지한 채 `attempt_count`, `next_attempt_at`, `last_error`만 갱신한다. 예외는 return(설정 오류라 재시도해도 소용없음)과 시도 한도 초과로, 이때는 `FAILED`로 바꾼다. 상태 변경은 `WHERE status = 'PENDING'` 조건부 UPDATE로 한다.

| 결과 | `last_error` 예시 | 처리 |
| --- | --- | --- |
| ack, return 없음 | - | `PUBLISHED`, `published_at` 기록 |
| return | `RETURNED 312 NO_ROUTE key=schedule.generate` | 설정 오류이므로 즉시 `FAILED`, 작업도 `FAILED` |
| nack | `NACK {reason}` | `attempt_count + 1`, 백오프 후 재시도 |
| confirm 시간 초과 | `CONFIRM_TIMEOUT {대기 시간}` | 재시도. 브로커에는 들어갔을 수 있으므로 소비자가 중복을 처리한다 |
| 연결·채널 예외 | `AmqpConnectException: ...` | 재시도 |

- 백오프: `next_attempt_at = now + 백오프 + jitter`. 백오프 방식(지수 등), 상한, jitter 폭은 미정이다.
- `attempt_count`가 한도(미정)에 도달하면 Outbox와 작업을 함께 `FAILED`로 바꾸는 한 트랜잭션을 실행한다.
- `PUBLISHED`이고 보관 기간(미정)이 지난 행은 정리 작업으로 삭제한다. 정리 주기도 미정이다.

## 8. AI Worker 처리 규약

### 8.1 요청 메시지

| AMQP 속성 | 값 |
| --- | --- |
| `message_id` | Outbox `message_id` (UUID 문자열) |
| `content_type` | `application/json` |
| `delivery_mode` | 2 (persistent) |
| `priority` | 새 요청 5, 재시도 0 |
| header `x-job-type` | `SCHEDULE_GENERATE` 등 |
| header `x-job-id` | 작업 ID |
| header `x-retry-count` | 0부터 시작, 재시도 시 1씩 증가 |

```json
{
  "jobType": "SCHEDULE_GENERATE",
  "jobId": "4001",
  "requestedAt": "2026-10-28T14:00:00.123456+09:00",
  "data": {}
}
```

`data`는 작업별 기존 AI HTTP 요청 본문을 그대로 사용한다.

### 8.2 처리 순서

```python
def on_message(channel, method, properties, body):
    message = parse(body)                         # 실패하면 nack(requeue=False) → DLQ
    cached = result_cache.get(message.job_id)     # 재전달이면 LLM 재호출 없이 결과 재사용
    try:
        result = cached or run_with_timeout(message, timeout_of(message.job_type))
    except RetryableError as error:
        republish_to_retry(message, error)        # confirm 후
        channel.basic_ack(method.delivery_tag)
        return
    result_cache.set(message.job_id, result, ex=3600)
    publish_and_confirm("ai.result", "result", result)
    channel.basic_ack(method.delivery_tag)        # 결과 발행 confirm 이후에만 ACK
```

- `prefetch_count=1`로 Worker 하나가 한 번에 한 작업만 가져간다.
- 결과 발행 전에 ACK하면, 그 사이 Worker가 종료됐을 때 결과가 사라진다. 반드시 결과 confirm 후 ACK한다.
- ACK가 유실되면 브로커가 같은 메시지를 다시 전달한다. Worker는 `jobId` 결과 캐시로 AI 재호출을 피하고 결과를 다시 발행한다. 백엔드는 이미 끝난 작업의 결과를 무시하므로 정합성 문제가 없다.

### 8.3 실패 처리

| 실패 | 처리 |
| --- | --- |
| 메시지 파싱 불가, 필수 필드 누락 | `basic_nack(requeue=False)` → `ai.dlx` → DLQ |
| 일시적 오류(LLM 429·5xx, 네트워크), Timeout | `x-retry-count + 1`로 `ai.retry`에 재발행(tier는 횟수, sub-queue는 무작위) 후 ACK |
| `x-retry-count`가 N 이상 | `ai.dlx`로 재발행 후 ACK |
| 입력 자체로 결과를 만들 수 없음(업무 실패) | 재시도하지 않고 `FAILED` 결과를 `ai.result`에 발행 후 ACK |
| Worker 프로세스 종료 | ACK되지 않은 메시지는 재전달된다. `delivery-limit` 초과 시 DLQ |

- `basic_nack(requeue=True)`는 사용하지 않는다. 즉시 재전달되어 실패를 반복한다.

### 8.4 작업별 Timeout

| 작업 | Timeout |
| --- | --- |
| 사진 판정 | 30s |
| 포토미션 생성 | 110s |
| 일정 생성 | 미정 (14절) |

## 9. 결과 처리 (Result Worker)

### 9.1 결과 메시지

```json
{
  "jobType": "SCHEDULE_GENERATE",
  "jobId": "4001",
  "requestMessageId": "0192f3a1-...",
  "status": "SUCCEEDED",
  "errorCode": null,
  "completedAt": "2026-10-28T14:00:41.123456+09:00",
  "data": {}
}
```

- `status`는 `SUCCEEDED` 또는 `FAILED`다. `data`는 작업별 기존 AI HTTP 응답 본문을 그대로 사용한다.

### 9.2 처리 규칙

- `jobType`에 따라 작업별 처리기로 넘긴다.
- 상태 갱신은 조건부 UPDATE로 한다. 영향받은 행이 0이면 이미 끝난 작업이므로 결과를 반영하지 않고 ACK한다.

```sql
UPDATE schedule_generation_jobs
SET status = 'SUCCEEDED', finished_at = ?
WHERE id = ? AND status IN ('QUEUED', 'RUNNING');
```

- 결과 저장(일정 후보, 미션, 판정 결과)과 상태 갱신은 한 트랜잭션에서 수행하고, 커밋 후 ACK한다.
- `SUCCEEDED`, `FAILED`, `CANCELLED`는 바꾸지 않는다. 늦게 도착한 `FAILED` 결과나 DLQ 메시지가 성공한 작업을 덮어쓰지 않는다.
- 먼저 반영된 결과를 유지한다. AI 결과는 같은 입력에도 달라질 수 있으므로 나중 결과로 덮어쓰지 않는다.
- 완료 알림 등 부수 효과는 상태가 실제로 바뀐 경우에만 실행한다.
- 여행방이 소프트 삭제됐으면 결과를 반영하지 않는다([`SCHEDULE_API_SPEC.md`](../docs/SCHEDULE_API_SPEC.md) 일정 변경 가능 시점).
- DB 오류 등 일시적 실패는 결과 큐의 Retry Queue로 보내고, 재시도를 다 쓰면 결과 DLQ로 보낸다.

### 9.3 DLQ 처리

- 백엔드 DLQ Consumer가 요청 DLQ를 소비해 해당 작업을 끝난 상태가 아닐 때만 `FAILED`로 바꾼다.
- 결과 DLQ의 메시지는 AI가 성공한 결과일 수 있으므로 자동으로 `FAILED` 처리하지 않는다. 경보를 보내고 원인을 해결한 뒤 결과 큐로 다시 넣는다.

### 9.4 Stale Job Checker

Outbox 재시도, Retry Queue, DLQ는 실패를 알아챈 경우만 처리한다. Stale Job Checker는 아무 신호 없이 멈춘 작업을 마무리하는 마지막 안전망이다.

| 멈추는 경우 | 결과 |
| --- | --- |
| Outbox 발행이 유실되거나 Relay가 동작하지 않음 | 작업이 `QUEUED`에 머묾 |
| 작업 메시지·결과 메시지 유실 | 작업이 `QUEUED`·`RUNNING`에 머묾 |
| 결과는 왔지만 Result Worker가 반영하지 못함 | 작업이 `RUNNING`에 머묾 |

이 상태에서는 `active_slot = 1`이 풀리지 않아 사용자가 같은 작업을 다시 요청할 수 없다.

#### 처리 순서

1. **조회:** 3분마다 작업 테이블별로 `QUEUED`·`RUNNING`이면서 기준 시간을 넘긴 작업을 조회한다.

   | 작업 테이블 | 사용하는 인덱스 |
   | --- | --- |
   | `schedule_generation_jobs` | `(status, created_at)` |
   | `mission_generation_jobs` | `(status, last_attempted_at)` |
   | `photo_evaluations` | `(execution_status, requested_at)` |

2. **원인 확인(선택):** `idx_message_outbox_job`으로 해당 작업의 Outbox 행을 조회한다. `PENDING`·`FAILED`면 발행되지 않은 것이고, `PUBLISHED`면 발행 후 처리·결과 단계에서 멈춘 것이다. 확인한 원인은 `error_code`에 기록한다.
3. **조건부 UPDATE:** 그 사이 결과가 먼저 반영된 작업은 건드리지 않는다.

   ```sql
   UPDATE {작업 테이블}
   SET status = 'FAILED', active_slot = NULL, error_code = ?
   WHERE id = ? AND status IN ('QUEUED', 'RUNNING');
   ```

   변경된 행이 0건이면 결과가 먼저 반영된 것이므로 종료한다. 1건이면 같은 트랜잭션에서 해당 작업의 `PENDING` Outbox 행도 `FAILED`로 바꿔, 실패 처리한 작업이 뒤늦게 발행되지 않게 한다.
4. **실패 알림:** 커밋 후 Redis Pub/Sub로 실패 알림을 발행하고, 각 인스턴스가 WebSocket으로 전달한다. `active_slot`이 풀렸으므로 사용자는 다시 요청할 수 있다.

#### 기준 시간

- 정상 경로 중 가장 오래 걸리는 경우보다 길게 잡아야 진행 중인 작업을 실패로 만들지 않는다.
- `Outbox 발행 재시도 최대 시간 + Timeout × (재시도 횟수 + 1) + Retry Queue TTL 합 + 결과 반영 시간 + 여유 시간`으로 계산한다.
- 작업마다 Timeout과 재시도 설정이 다르므로 기준 시간도 작업별로 둔다.
- 늦게 도착한 결과는 Result Worker의 조건부 UPDATE(9.2절)에서 무시되므로 최종 상태는 바뀌지 않는다.
- 실행 주기(3분)는 감지를 늦출 뿐 정상 작업을 실패로 만들지 않으므로 기준 시간에 넣지 않는다. 멈춘 작업의 실패 알림은 최악의 경우 `기준 시간 + 3분` 뒤에 전달된다.
- 조건부 UPDATE로 처리하므로 여러 인스턴스에서 동시에 실행돼도 한 번만 반영되고, 알림도 1건 변경된 인스턴스만 발행한다.

## 10. 작업별 전환

| 작업 | 시작 지점 변경 | 기존 코드 처리 |
| --- | --- | --- |
| 사진 판정 | 사진 제출 트랜잭션에서 `photo_evaluations`(`QUEUED`, `active_slot = 1`)와 Outbox 행 저장 | `PhotoMissionVerificationService`의 동기 AI 호출과 DB 기반 재시도 스케줄러를 MQ 처리로 교체(6.3.3절) |
| 포토미션 생성 | `PhotoMissionGenerationScheduler`가 AI를 호출하지 않고 작업 행(`QUEUED`)과 Outbox 행만 저장 | AI 호출과 결과 저장은 Result Worker로 이동 |
| 일정 생성 | `POST /api/trips/{tripId}/schedule-generation-jobs`가 작업 행과 Outbox 행 저장 후 `202` | 현재 동기 생성 API는 비동기 API 전환 후 제거 여부를 API 명세에서 결정 |

- 전환은 한 작업씩 진행하고, 작업마다 설정 플래그로 기존 동기 경로와 MQ 경로를 전환할 수 있게 한다.
- 일정 생성의 경로 계산과 저장은 Result Worker에서 수행한다. 최대 일정(9일 × 하루 5곳, 후보 3개) 기준 측정값은 다음과 같다(`planit-backend/performance/route`, Apple M2 Pro·로컬 MySQL 8.4, 100회).

| 단계 | 평균 | p99 | 첫 실행 |
| --- | ---: | ---: | ---: |
| 경로 계산 | 0.17ms | 0.21ms | 0.64ms |
| 장소 저장 | 14.7ms | 25.2ms | 63.6ms |
| 경로 저장 | 88.3ms | 134.3ms | 300.2ms |
| 저장 합계 | 103.0ms | 154.1ms | 363.8ms |

- 결과 반영 시간은 AI 처리 시간에 비해 작으므로 Result Worker가 결과 큐의 병목이 될 가능성은 낮다. 저장은 행 단위 INSERT가 대부분을 차지한다.

## 11. 완료 알림

완료 알림은 기존 알림 기능과 같은 방식으로 **Redis Pub/Sub → 각 인스턴스 → 사용자 WebSocket** 경로로 전달한다.

```text
Result Worker ─(커밋 후)─▶ Redis Pub/Sub `planit:ai-jobs`
                              ├─▶ 인스턴스 A ─▶ 연결된 수신자에게 STOMP `/user/queue/ai-jobs`
                              └─▶ 인스턴스 B ─▶ 연결된 수신자에게 STOMP `/user/queue/ai-jobs`
```

- Result Worker는 결과 커밋 후(`AFTER_COMMIT`) Redis Pub/Sub 채널 `planit:ai-jobs`로 완료 이벤트를 발행한다. 발행 실패는 재시도하지 않는다.
- 각 인스턴스는 이벤트를 받아 자신이 보유한 연결 중 수신자가 있는 경우에만 `/user/queue/ai-jobs`로 전송한다. 알림 기능의 `planit:notifications`, `/user/queue/notifications`와 채널을 분리한다. 작업 완료 이벤트는 `notifications` 테이블에 저장하는 알림이 아니기 때문이다.
- 수신자는 일정 생성이면 여행방의 활성 멤버, 사진 판정이면 사진을 제출한 멤버, 포토미션 생성이면 여행방의 활성 멤버다.
- 구독은 알림 기능과 같이 인증된 STOMP 연결에서 `receipt` 헤더로 구독 완료를 확인한다.
- 내부 Pub/Sub 이벤트 DTO와 WebSocket 응답 DTO는 별도 타입으로 두고, WebSocket payload에서는 라우팅용 수신자 식별자를 제외한다.

WebSocket payload 예시:

```json
{
  "jobType": "SCHEDULE_GENERATE",
  "jobId": "4001",
  "tripId": "7",
  "status": "SUCCEEDED",
  "completedAt": "2026-10-28T14:00:41.123456+09:00"
}
```

- payload에는 결과 본문을 넣지 않는다. 클라이언트는 알림을 받으면 조회 API로 결과를 가져온다.
- Pub/Sub과 WebSocket 전송은 그 순간 연결된 사용자에게만 전달되므로, 알림은 빠른 전달 수단이고 정합성의 기준은 작업 테이블이다.
- 완료 알림에는 Outbox를 사용하지 않는다. Outbox로 발행을 보장해도 구독자가 없으면 Pub/Sub 메시지는 사라지기 때문이다.

### 11.1 미접속·재연결 복구

1. 화면 진입 시 WebSocket을 연결하고 `/user/queue/ai-jobs` 구독 완료(`RECEIPT`)를 확인한다.
2. 진행 중인 작업이 있는 화면이면 작업 상태 조회 API를 호출한다.
   - 일정 생성: `GET /api/schedule-generation-jobs/{jobId}`
   - 사진 판정: `GET /api/mission-photos/{photoId}/evaluation`
3. 이미 끝난 작업이면 조회 응답으로 결과를 반영한다.
4. 구독 후 조회하므로, 조회 직전에 끝난 작업도 WebSocket 이벤트나 조회 응답 중 하나로 반드시 받는다. 둘 다 받으면 `jobId`로 한 번만 반영한다.

## 12. 장애·유실 시나리오

| 장애 지점 | 결과 | 복구 |
| --- | --- | --- |
| 요청 트랜잭션 커밋 실패 | 클라이언트는 오류 응답을 받는다 | 클라이언트 재요청. 작업과 메시지 모두 남지 않음 |
| 커밋 후 즉시 발행 전 서버 종료 | Outbox `PENDING` 유지 | 유예 시간 후 Relay가 발행 |
| 즉시 발행 confirm 대기 중 서버 종료 | Outbox `PENDING` 유지, 브로커에는 들어갔을 수 있음 | Relay가 다시 발행, 중복은 소비자가 멱등 처리 |
| 브로커 장애 | 발행 실패, Outbox `PENDING` | 백오프 재시도, 한도 초과 시 `FAILED` |
| confirm 시간 초과 후 실제로는 저장됨 | 같은 메시지가 두 번 발행될 수 있음 | AI 결과 캐시와 백엔드 조건부 갱신으로 한 번만 반영 |
| AI 처리 중 Worker 종료 | 메시지 재전달 | 재처리, 반복되면 `delivery-limit` 초과로 DLQ |
| AI 일시 장애 | Retry Queue를 거쳐 재시도 | N회 실패 시 DLQ → 작업 `FAILED` |
| 결과 발행 후 ACK 유실 | 요청 메시지 재전달 | 결과 캐시로 결과 재발행, 백엔드가 중복 무시 |
| Result Worker 커밋 전 종료 | 결과 메시지 재전달 | 조건부 갱신으로 재처리 |
| 완료 알림 유실 | 클라이언트가 알림을 받지 못함 | 재연결·화면 진입 시 상태 조회 |
| 결과가 끝내 오지 않음 | 작업이 `QUEUED`·`RUNNING`에 머묾 | Stale Job Checker가 `FAILED` 처리, `active_slot` 해제, 실패 알림 |

## 13. 구현 순서

1. RabbitMQ Docker Compose 추가, Spring AMQP 의존성과 `Declarables` 토폴로지 선언
2. `message_outbox` Flyway 마이그레이션, Entity·Repository, 6.3절 작업 테이블 변경(일정 작업 UNIQUE 제약, 포토미션 `QUEUED`, 사진 판정 구조 변경)
3. 커밋 후 즉시 발행, Relay, Publisher Confirm·return 처리, 정리 작업
4. FastAPI Worker 소비·결과 발행·ACK·Retry Queue 재발행 (AI 팀과 메시지 계약 합의)
5. Result Worker 작업별 처리기, 결과 Retry·DLQ, 요청 DLQ Consumer, Stale Job Checker
6. 사진 판정 전환
7. 포토미션 생성 전환
8. 일정 생성 비동기 API 전환
9. 완료 알림 Redis Pub/Sub·WebSocket 전달과 재연결 복구
10. 장애·유실 시나리오 통합 테스트와 부하 테스트

## 14. 결정이 필요한 사항

| 항목 | 현재 상태 | 결정 기준 |
| --- | --- | --- |
| 사진 판정 routing key | `photo.judge` 임시 | 다른 routing key 명명 규칙과 일치 |
| 요청 Retry Queue 단계 수(N)와 sub-queue 수(k), TTL | 미정 | AI 장애 회복 시간, Jitter로 분산할 시간 폭 |
| 결과 Retry Queue 단계 수(M) | 미정 | 백엔드 DB 장애 회복 시간 |
| 작업별 AI Worker 수 | 미정 | AI 서버 처리 한계와 LLM API 호출 한도에서 작업별 배분 |
| 일정 생성 Timeout | 미정 | AI 응답 시간 측정값의 p99 |
| `exchange_name` 컬럼 유지 여부 | 유지 | 다른 exchange로 발행할 메시지가 생길 가능성 |
| 기존 `RETRY_PENDING` 행 정리 방식 | 사진 판정 재시도는 MQ Retry Queue로 통일하기로 결정(6.3.3절) | 전환 시점에 남은 행을 `FAILED`로 정리할지, 기존 스케줄러가 모두 처리하도록 기다릴지 |
| AI 측 멱등 구조 | `jobId` 결과 캐시(Redis) 제안, TTL 미정 | 재전달 가능 시간보다 긴 TTL |
| 큐 `x-delivery-limit` | 미정 | Worker 비정상 종료를 몇 번까지 재전달로 견딜지 |
| 큐 `x-max-length` | 미정 | 허용할 최대 큐 대기 시간, Worker 처리량 |
| Outbox 발행 시도 한도 | 미정 | 브로커 장애 허용 시간, Stale 기준 시간에 주는 영향 |
| Outbox 백오프 방식·상한·jitter | 미정 | 브로커 회복 시간, 재시도 몰림 방지 |
| Publisher confirm 대기 시간 | 미정 | 정상 confirm 응답 시간 측정값 |
| 즉시 발행 유예 시간 | 미정 | confirm 대기 시간보다 길게 |
| Relay 주기·배치 크기·lease 시간 | 미정 | 발행 지연 허용치, DB 부하, 장애 후 적체 해소 속도, 발행+confirm 시간보다 긴 lease |
| `PUBLISHED` Outbox 보관 기간·정리 주기 | 미정 | 장애 조사에 필요한 기간, 테이블 크기 |
| Stale Job Checker 주기·작업별 기준 시간 | 미정 | 9.4절 계산식, 감지 지연 허용치 |

## 15. 검증 체크리스트

### 발행

- [ ] 요청 트랜잭션이 롤백되면 작업 행과 Outbox 행이 모두 남지 않고 `202`를 반환하지 않는다.
- [ ] Outbox 행은 `PENDING`으로 저장되고, ack이면서 return이 없을 때만 `PUBLISHED`가 된다.
- [ ] 즉시 발행이 진행 중인 행은 유예 시간 동안 Relay가 가져가지 않는다.
- [ ] 커밋 후 즉시 발행이 실패해도 Relay가 `PENDING` 행을 발행한다.
- [ ] 두 백엔드 인스턴스의 Relay가 같은 Outbox 행을 동시에 발행하지 않는다.
- [ ] 라우팅되지 않는 routing key는 return으로 감지해 `FAILED`와 `last_error`를 기록한다.
- [ ] 브로커 중단 중에는 백오프 간격으로만 재시도하고, 한도 초과 시 Outbox와 작업이 함께 `FAILED`가 된다.

### 처리와 재시도

- [ ] AI 일시 장애 시 메시지가 Retry Queue를 거쳐 Task Queue로 돌아온다.
- [ ] 같은 시각에 실패한 작업들이 서로 다른 sub-queue TTL로 분산되어 돌아온다.
- [ ] 재시도 메시지가 새 요청보다 먼저 처리되지 않는다.
- [ ] 재시도를 모두 사용한 메시지는 DLQ로 가고 작업이 `FAILED`가 된다.
- [ ] Worker가 ACK 전에 계속 종료되는 메시지는 `delivery-limit` 초과 후 DLQ로 간다.

### 결과 반영

- [ ] 같은 결과가 두 번 도착해도 한 번만 반영되고 알림도 한 번만 발행된다.
- [ ] 동시에 도착한 중복 결과 중 하나만 반영된다.
- [ ] 이미 `SUCCEEDED`인 작업에 늦은 `FAILED` 결과나 DLQ 메시지가 와도 상태가 바뀌지 않는다.
- [ ] 소프트 삭제된 여행방의 결과는 반영하지 않는다.
- [ ] Stale Job Checker가 기준 시간을 넘긴 작업만 `FAILED`로 바꾸고 `active_slot`을 해제한다.
- [ ] Stale Job Checker가 실패 처리한 작업의 `PENDING` Outbox 행도 `FAILED`가 되어 뒤늦게 발행되지 않는다.
- [ ] Stale Job Checker 실행 중 결과가 먼저 반영되면 작업 상태를 바꾸지 않고 알림도 보내지 않는다.
- [ ] 같은 여행에 진행 중인 일정 생성 작업이 있으면 새 작업 생성이 `uk_schedule_generation_jobs_trip_active`로 막힌다.
- [ ] 같은 사진에 진행 중인 판정이 있으면 새 판정 생성이 `uk_photo_evaluations_photo_active`로 막힌다.
- [ ] 작업이 끝나면 `active_slot`이 `NULL`이 되어 같은 대상에 새 작업을 만들 수 있다.
- [ ] 사진 판정의 AI 재시도는 같은 행을 유지하고 `execution_attempt_count`만 늘어난다.

### 알림

- [ ] 인증 사용자가 `/user/queue/ai-jobs` 구독 완료 후 본인 작업의 완료 이벤트를 받는다.
- [ ] 다른 인스턴스에서 처리된 작업의 완료 이벤트도 Redis Pub/Sub을 거쳐 수신자의 WebSocket으로 전달된다.
- [ ] 완료 알림을 놓친 클라이언트가 상태 조회로 결과를 받는다.
- [ ] 구독 등록 후 상태를 조회해, 조회와 알림 사이에 끝난 작업도 놓치지 않는다.

## 16. 운영 관측 기준

- Outbox: `PENDING` 행 수와 가장 오래된 `PENDING`의 대기 시간, `FAILED` 행 수와 `last_error` 분류별 개수
- RabbitMQ: 큐별 메시지 수(ready, unacked), DLQ 메시지 수, 소비자 수
- 작업: 작업 유형별 `QUEUED`→`SUCCEEDED` 소요 시간, `FAILED` 비율, Stale Job Checker 처리 건수
- 메트릭 이름은 `planit.mq.*` 접두사를 사용하고, DLQ 메시지 증가와 Outbox `FAILED` 발생은 경보 대상으로 삼는다.
