# RabbitMQ 비동기 AI 작업 — 백엔드 구현 문서

이 문서는 Spring 백엔드(`planit-backend`)가 구현할 범위를 정의한다. 큐 이름, 인자, 메시지 형식, 헤더, `errorKind`는 [`../infra/RABBITMQ_INFRA_AND_CONTRACT.md`](../infra/RABBITMQ_INFRA_AND_CONTRACT.md)(이하 **계약 문서**)를 따른다. 이 문서에서 값을 다시 적은 곳은 계약 문서와 같아야 한다.

이 문서는 기존 [`MESSAGE_QUEUE_IMPLEMENTATION_GUIDE.md`](./MESSAGE_QUEUE_IMPLEMENTATION_GUIDE.md)를 대체한다.

## 0. 구현 범위와 순서

| 순서 | 범위 | 절 |
| ---: | --- | --- |
| 1 | 의존성, 설정, 스케줄러 스레드 | 1절 |
| 2 | Flyway 마이그레이션 (확장, 데이터 정리) | 2절 |
| 3 | 토폴로지 선언 | 3절 |
| 4 | Outbox 저장과 발행, 발행 결과 처리 | 4절, 5절 |
| 5 | 작업별 요청 접수 (202 / 503) | 6절 |
| 6 | Outbox Relay, Stale Job Checker, Cleanup, Monitor | 7~10절 |
| 7 | Result Worker, DLQ Consumer, 포토미션 05시 재처리 | 11~13절 |
| 8 | 알림 | 14절 |
| 9 | 운영 전환 플래그, 기존 재시도 제거 | 15절 |
| 10 | 테스트 | 16절 |

패키지는 공통 기능을 `com.planit.mq`에 둔다.

```text
com.planit.mq
├── config        // RabbitMqProperties, RabbitMqConfig, RabbitTopologyConfig
├── outbox        // MessageOutbox, MessageOutboxRepository, OutboxService, OutboxPublisher, OutboxPublishResultHandler, OutboxFailureService, MessageOutboxRelayScheduler, MessageOutboxCleanupScheduler
├── job           // AiJobType, AiJobTerminationService(공통 종료), GenerationJobPublishFailureListener, StaleJobChecker, StaleJobFailureService
├── intake        // AiJobIntakeGuard(전환·차단 플래그 확인), AiJobAcceptedResponse
├── monitor       // MessageQueueMonitorScheduler, MessageQueueMonitor, RabbitManagementClient
├── result        // AiResultListener, AiResultEnvelopeParser, AiResultRepublisher, ScheduleGenerationResultHandler, MissionGenerationResultHandler
├── dlq           // AiDlqListener, PhotoMissionGenerationDlqReprocessor
└── message       // AiRequestEnvelope, AiRequestMessageWriter
```

---

## 1. 의존성과 설정

### 1.1 Gradle

```groovy
implementation 'org.springframework.boot:spring-boot-starter-amqp'
testImplementation 'org.testcontainers:rabbitmq'
```

### 1.2 `application.yaml`

```yaml
spring:
  rabbitmq:
    host: ${RABBITMQ_HOST:localhost}
    port: ${RABBITMQ_PORT:5672}
    username: ${RABBITMQ_USERNAME:planit-backend}
    password: ${RABBITMQ_PASSWORD}
    virtual-host: planit
    publisher-confirm-type: correlated
    publisher-returns: true
    template:
      mandatory: true
    listener:
      simple:
        acknowledge-mode: manual
        default-requeue-rejected: false

planit:
  mq:
    confirm-timeout: 5s            # CONFIRM_TIMEOUT. 부하 테스트에서 confirm 지연 max를 측정해 확정
    retry-after: 60s               # 503 응답의 Retry-After
    outbox:
      relay:
        batch-size: 500
      initial-grace: 5s            # 최초 next_attempt_at = 저장 시각 + confirm-timeout + initial-grace
      backoff-base: 5s             # NACK, confirm 유실·연결 예외 공통 시작 간격 (x2)
      nack-max-retries: 3          # 5s, 10s, 20s
      failure-max-retries: 4       # 5s, 10s, 20s, 40s
      failed-retention: 30d
      cleanup:
        delete-batch-size: 1000
    monitor:
      management-base-url: ${RABBITMQ_MANAGEMENT_URL:http://localhost:15672}
      virtual-host: ${spring.rabbitmq.virtual-host}
      username: ${RABBITMQ_MONITOR_USERNAME:planit-monitor}
      password: ${RABBITMQ_MONITOR_PASSWORD}
      connect-timeout: 300ms
      read-timeout: 500ms
      task-block-ratio: 0.95
      task-unblock-ratio: 0.50
      dlq-block-ratio: 0.90
      dlq-unblock-ratio: 0.50
```

`@ConfigurationProperties(prefix = "planit.mq")` record `RabbitMqProperties`로 읽는다. 큐 감시와 접수 설정은 `MessageQueueMonitorProperties`, `AiJobIntakeProperties`로 따로 읽는다.

Stale 기준 시간(일정 생성 28분, 포토미션 생성 06:00)은 설정이 아니라 코드 상수다. 요청 메시지 `deadlineAt`과 같은 값이어야 하므로 접수 코드와 `StaleJobChecker`에 함께 둔다.

### 1.3 스케줄러 스레드

현재 `SchedulingConfig`의 `ThreadPoolTaskScheduler` pool size는 1이다. Relay(500ms)가 confirm을 최대 5초 기다리는 동안 Monitor(5초)와 Stale이 밀리므로, pool size를 **`@Scheduled` 메서드 수 이상**으로 올린다. 이렇게 하면 어떤 작업도 다른 작업 때문에 대기하지 않는다.

```java
scheduler.setPoolSize(properties.schedulerPoolSize()); // = 앱 전체 @Scheduled 메서드 수
```

### 1.4 confirm 콜백 실행기

Publisher confirm은 Spring AMQP 연결 스레드가 `CorrelationData`의 Future를 완료한다. 연결 스레드에서 DB 작업을 하면 다음 confirm 처리가 막히므로, 결과 처리는 별도 실행기에서 한다.

```java
@Bean(name = "mqConfirmExecutor", destroyMethod = "close")
ExecutorService mqConfirmExecutor() {
    return Executors.newVirtualThreadPerTaskExecutor();
}
```

---

## 2. 데이터베이스 마이그레이션

확장 → 데이터 정리 → 전환 → 제거 순서로 진행한다. 비동기 작업 전 마지막 버전은 `V41`이다.

### 2.1 `V42__create_message_outbox_and_add_ai_job_constraints.sql` (확장, 기존 코드와 호환)

`schedule_generation_jobs`는 V5에서 이미 만들어졌으나 쓰지 않던 테이블이다. 그래서 새로 만들지 않고 값 범위를 맞춘 뒤 제약과 인덱스를 추가한다. `photo_evaluations` 변경은 이 버전에 넣지 않고 포토미션 판정 작업에서 별도 버전으로 진행한다(아래 SQL의 `photo_evaluations` 부분과 2.2절).

```sql
CREATE TABLE `message_outbox` (
    `id`                    BIGINT       NOT NULL AUTO_INCREMENT COMMENT '메시지 발행 Outbox ID',
    `job_type`              VARCHAR(40)  NOT NULL COMMENT 'SCHEDULE_GENERATE, PHOTO_MISSION_GENERATE, PHOTO_JUDGE',
    `job_id`                BIGINT       NOT NULL COMMENT 'job_type별 작업 테이블의 ID',
    `message_id`            BINARY(16)   NOT NULL COMMENT '메시지 ID(중복 확인용)',
    `exchange_name`         VARCHAR(100) NOT NULL COMMENT '발행할 exchange',
    `routing_key`           VARCHAR(100) NOT NULL COMMENT '라우팅 키',
    `payload`               JSON         NOT NULL COMMENT '메시지 본문',
    `status`                VARCHAR(20)  NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING, PUBLISHED, FAILED',
    `nack_attempt_count`    TINYINT      NOT NULL DEFAULT 0 COMMENT 'NACK 재시도 횟수',
    `failure_attempt_count` TINYINT      NOT NULL DEFAULT 0 COMMENT 'confirm 유실·연결 예외 재시도 횟수',
    `next_attempt_at`       DATETIME(6)  NOT NULL COMMENT '다음 발행 시도 시각(선점 lease 겸용)',
    `last_error`            VARCHAR(500) NULL     COMMENT '마지막 발행 실패 이유',
    `created_at`            DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    `published_at`          DATETIME(6)  NULL     COMMENT '발행(confirm) 완료 시각',
    CONSTRAINT `PK_MESSAGE_OUTBOX` PRIMARY KEY (`id`),
    CONSTRAINT `uk_message_outbox_message_id` UNIQUE (`message_id`),
    INDEX `idx_message_outbox_poll` (`status`, `next_attempt_at`),
    INDEX `idx_message_outbox_job` (`job_type`, `job_id`)
);

-- V5에서 만든 테이블의 값 범위를 맞추고, 중복 방지 제약과 Stale 조회 인덱스를 추가한다
ALTER TABLE `schedule_generation_jobs`
    MODIFY COLUMN `job_type` VARCHAR(30) NOT NULL DEFAULT 'INITIAL'
        COMMENT 'INITIAL, GENERAL_REGENERATION, WEATHER_REGENERATION',
    MODIFY COLUMN `status` VARCHAR(20) NOT NULL DEFAULT 'QUEUED'
        COMMENT 'QUEUED, SUCCEEDED, FAILED',
    MODIFY COLUMN `updated_at` DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6);
ALTER TABLE `schedule_generation_jobs`
    ADD CONSTRAINT `uk_schedule_generation_jobs_active` UNIQUE (`trip_id`, `active_slot`),
    ADD CONSTRAINT `uk_schedule_generation_jobs_idempotency` UNIQUE (`idempotency_key`);
CREATE INDEX `idx_schedule_generation_jobs_stale`
    ON `schedule_generation_jobs` (`status`, `created_at`);

ALTER TABLE `mission_generation_jobs`
    MODIFY COLUMN `status` VARCHAR(20) NOT NULL COMMENT 'QUEUED, RUNNING, SUCCEEDED, FAILED';
CREATE INDEX `idx_mission_generation_jobs_stale`
    ON `mission_generation_jobs` (`status`, `last_attempted_at`);

-- 아래는 V42에 넣지 않는다. 포토미션 판정 작업의 별도 버전에서 진행한다
ALTER TABLE `photo_evaluations`
    ADD COLUMN `execution_attempt_count` TINYINT NOT NULL DEFAULT 0
        COMMENT '기술적 재시도 횟수(AI x-retry-count + 1)' AFTER `attempt_no`,
    ADD COLUMN `active_slot` TINYINT NULL
        COMMENT '판정 진행 중일 때만 1' AFTER `execution_status`,
    MODIFY COLUMN `execution_status` VARCHAR(20) NOT NULL
        COMMENT 'QUEUED, RUNNING, SUCCEEDED, RETRY_PENDING, FAILED';
```

- `schedule_generation_jobs`는 아직 쓰지 않던 테이블이라 중복 데이터가 없으므로 UNIQUE를 바로 건다.
- `photo_evaluations`의 판정 Stale 조회는 기존 인덱스 `idx_photo_evaluations_stale_running (execution_status, requested_at)`를 그대로 쓴다.

### 2.2 포토미션 판정 데이터 정리 (판정 작업의 별도 버전)

같은 사진에 진행 중인 판정이 두 개 이상이면 가장 최근 행만 진행 중으로 남기고, 나머지는 `FAILED`로 정리한 뒤 UNIQUE를 추가한다.

```sql
UPDATE `photo_evaluations` pe
JOIN (
    SELECT `mission_photo_id`, MAX(`id`) AS `keep_id`
    FROM `photo_evaluations`
    WHERE `execution_status` IN ('RUNNING', 'RETRY_PENDING')
    GROUP BY `mission_photo_id`
) latest ON latest.`mission_photo_id` = pe.`mission_photo_id`
SET pe.`execution_status` = 'FAILED',
    pe.`error_code` = 'DUPLICATE_ACTIVE_EVALUATION'
WHERE pe.`execution_status` IN ('RUNNING', 'RETRY_PENDING')
  AND pe.`id` <> latest.`keep_id`;

UPDATE `photo_evaluations`
SET `active_slot` = 1
WHERE `execution_status` IN ('RUNNING', 'RETRY_PENDING');

ALTER TABLE `photo_evaluations`
    ADD CONSTRAINT `uk_photo_evaluations_active`
        UNIQUE (`mission_photo_id`, `active_slot`);
```

`active_slot`은 진행 중이면 1, 끝나면 NULL이다. MySQL UNIQUE는 NULL끼리 충돌하지 않으므로 종료된 판정은 여러 개 남을 수 있다. **모든 종료 전이(성공, 실패, Stale, DLQ, 발행 실패)에서 `active_slot = NULL`로 바꾼다.**

### 2.3 전환 단계

비동기 코드를 플래그(15절)와 함께 배포한다. 동기 경로의 `RUNNING`은 그대로 두고, 새 요청만 `QUEUED`로 저장한다. 기존 `RUNNING` 행은 기존 동기 코드가 계속 처리한다.

### 2.4 제거 단계 (완전 전환 후 별도 버전)

15.3절의 완전 전환 조건을 만족한 뒤 별도 Flyway 버전으로 진행한다.

1. `RETRY_PENDING`, `RUNNING` 행이 남아 있는지 확인하고, 남아 있으면 `FAILED`(`active_slot = NULL`)로 바꾼다.
2. `photo_evaluations.retry_after_at`, `idx_photo_evaluations_retry_pending`을 제거한다.
3. `execution_status`, `mission_generation_jobs.status` 주석에서 `RUNNING`, `RETRY_PENDING`을 제거한다.
4. `PhotoMissionEvaluationRetryScheduler`, `PhotoMissionEvaluationRetryService`, `recoverStaleRunning` 관련 코드를 같은 배포에서 제거한다.

---

## 3. 토폴로지 선언

백엔드만 선언한다. `RabbitAdmin`은 최초 연결 시 `Declarables` 빈의 내용을 멱등적으로 선언한다. 이름과 인자는 계약 문서 3절과 같아야 한다.

```java
@Configuration(proxyBeanMethods = false)
class RabbitTopologyConfig {

    @Bean
    Declarables aiTopology() {
        List<Declarable> d = new ArrayList<>();
        DirectExchange request = new DirectExchange("ai.request", true, false);
        DirectExchange retry = new DirectExchange("ai.retry", true, false);
        DirectExchange dlx = new DirectExchange("ai.dlx", true, false);
        DirectExchange result = new DirectExchange("ai.result", true, false);
        d.addAll(List.of(request, retry, dlx, result));

        addJobQueues(d, request, retry, dlx, "ai.schedule-generation", "ai-schedule-generation", 3_450, 50_000);
        addJobQueues(d, request, retry, dlx, "ai.photomission-generation", "ai-photomission-generation", 10_500, 10_500);
        addJobQueues(d, request, retry, dlx, "ai.photomission-verification", "ai-photomission-verification", 100, 50_000);
        addResultQueues(d, result, retry, dlx);
        return new Declarables(d);
    }

    private void addJobQueues(List<Declarable> d, DirectExchange request, DirectExchange retry,
                              DirectExchange dlx, String name, String rk, int max, int dlqMax) {
        Queue task = QueueBuilder.durable(name + ".task").quorum()
                .maxLength(max).overflow(QueueBuilder.Overflow.rejectPublish)
                .deadLetterExchange("ai.dlx").deadLetterRoutingKey(rk + ".dlq")
                .deadLetterStrategy("at-least-once")
                .deliveryLimit(5)
                .build();
        d.add(task);
        d.add(BindingBuilder.bind(task).to(request).with(rk));

        int[] ttls = {5_000, 10_000, 20_000};
        String[] suffix = {"5s", "10s", "20s"};
        for (int i = 0; i < 3; i++) {
            Queue rq = QueueBuilder.durable(name + ".retry." + suffix[i]).quorum()
                    .maxLength(max).overflow(QueueBuilder.Overflow.rejectPublish)
                    .ttl(ttls[i])
                    .deadLetterExchange("ai.request").deadLetterRoutingKey(rk)
                    .deadLetterStrategy("at-least-once")
                    .build();
            d.add(rq);
            d.add(BindingBuilder.bind(rq).to(retry).with(rk + ".retry." + (i + 1)));
        }

        Queue dlq = QueueBuilder.durable(name + ".dlq").quorum()
                .maxLength(dlqMax).overflow(QueueBuilder.Overflow.rejectPublish)
                .build();
        d.add(dlq);
        d.add(BindingBuilder.bind(dlq).to(dlx).with(rk + ".dlq"));
    }

    // addResultQueues: 계약 문서 3.4절. maxLength 대신 maxLengthBytes(209_715_200L),
    // TTL 1s/2s/4s, retry 큐의 dead-letter 대상은 ai.result / ai-result.
}
```

- 사용하는 Spring AMQP 버전의 `QueueBuilder`에 `deliveryLimit`, `deadLetterStrategy` 메서드가 없으면 `.withArgument("x-delivery-limit", 5)`, `.withArgument("x-dead-letter-strategy", "at-least-once")`로 넣는다.
- 기존 큐와 인자가 다르면 선언이 `PRECONDITION_FAILED`로 실패한다. 인자 변경은 15.4절 절차를 따른다.

---

## 4. Outbox 저장과 발행

### 4.1 저장 (요청 트랜잭션 안)

작업 행과 Outbox 행을 **같은 트랜잭션**에서 저장한다.

| 컬럼 | 값 |
| --- | --- |
| `job_type`, `job_id` | 작업 종류, 방금 저장한 작업 ID |
| `message_id` | UUID v4 (BINARY(16)) |
| `exchange_name` / `routing_key` | `ai.request` / 작업별 routing key |
| `payload` | 계약 문서 4.3절 요청 본문 |
| `status` | `PENDING` |
| `next_attempt_at` | 저장 시각 + `confirm-timeout`(5s) + `initial-grace`(5s) |

`next_attempt_at`을 confirm 대기시간보다 뒤로 두어, 커밋 직후 발행한 메시지를 Relay가 confirm 도착 전에 다시 발행하지 않게 한다.

### 4.2 커밋 직후 발행

```java
TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
    @Override public void afterCommit() { outboxPublisher.publishAsync(outboxId); }
});
```

`publishAsync`는 confirm을 기다리지 않는다. 요청 스레드는 바로 202를 반환하고, confirm 결과는 5절의 처리기가 반영한다. 발행 중 예외가 나면 로그만 남기고 삼킨다(Relay가 재발행한다).

### 4.3 AMQP 메시지 생성

```java
MessageProperties p = new MessageProperties();
p.setContentType("application/json");
p.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
p.setMessageId(uuid(outbox.messageId()));
p.setCorrelationId(String.valueOf(outbox.jobId()));
p.setType(outbox.jobType().name());
p.setTimestamp(Date.from(now.truncatedTo(ChronoUnit.SECONDS).toInstant())); // epoch 초
p.setAppId("planit-backend");
p.setHeader("x-retry-count", 0);
Message message = new Message(outbox.payloadBytes(), p);

CorrelationData cd = new CorrelationData(String.valueOf(outbox.id()));
rabbitTemplate.send(outbox.exchangeName(), outbox.routingKey(), message, cd);
cd.getFuture()
  .orTimeout(props.confirmTimeout().toMillis(), TimeUnit.MILLISECONDS)
  .whenCompleteAsync((confirm, error) -> resultHandler.handle(outbox.id(), cd, confirm, error),
                     mqConfirmExecutor);
```

---

## 5. 발행 결과 처리 (`OutboxPublishResultHandler`)

즉시 발행과 Relay가 같은 처리기를 쓴다. 모든 UPDATE는 `WHERE id = ? AND status = 'PENDING'` 조건부로 수행한다. 0행이면 이미 다른 경로가 처리한 것이므로 아무것도 하지 않는다.

| 신호 | 판별 | 처리 |
| --- | --- | --- |
| ack, return 없음 | `confirm.ack() && cd.getReturned() == null` | `status=PUBLISHED`, `published_at=now` |
| ack + return | `confirm.ack() && cd.getReturned() != null` | **즉시 실패 처리** (5.2절), `last_error='RETURNED: {replyText}'` |
| nack | `!confirm.ack()` | `nack_attempt_count + 1`. 3회를 넘으면 실패 처리, 아니면 백오프 |
| confirm 시간 초과 | `error instanceof TimeoutException` | `failure_attempt_count + 1`. 4회를 넘으면 실패 처리, 아니면 백오프 |
| 발행 중 예외 (연결 끊김 등) | `send()` 예외, 또는 `error`가 그 밖의 예외 | 시간 초과와 같음 |

### 5.1 백오프

`n`은 증가시킨 뒤의 횟수다. Jitter는 equal jitter다.

```text
backoff = backoff-base(5s) × 2^(n-1)
next_attempt_at = now + backoff + random(0, backoff / 2)
```

| 종류 | 재시도 간격 | 한도 |
| --- | --- | --- |
| NACK | 5s, 10s, 20s | 3회 (4번째 NACK에서 실패) |
| confirm 유실, 연결 예외 | 5s, 10s, 20s, 40s | 4회 (5번째에서 실패, 최대 약 75~112.5초) |
| ack + return | 재시도 없음 | 즉시 실패 |

`last_error`에는 실패 종류와 메시지를 500자 이내로 저장한다.

### 5.2 실패 처리 (`OutboxFailureService.fail`)

하나의 트랜잭션에서 수행한다.

1. `UPDATE message_outbox SET status='FAILED', last_error=? WHERE id=? AND status='PENDING'` → 0행이면 종료.
2. `AiJobPublishFailureListener` 구현이 작업 테이블을 조건부 UPDATE한다(12.2절 공통 규칙, `error_code = 'PUBLISH_FAILED'` 또는 `'PUBLISH_RETURNED'`). 일정 생성과 포토미션 생성은 `GenerationJobPublishFailureListener`가 `AiJobTerminationService`로 끝내고, 포토미션 판정은 판정 쪽 구현이 끝낸다.
3. 2가 1행이면 실패 알림 저장 (14절). 포토미션 생성, 삭제된 여행은 알림을 만들지 않는다.
4. 커밋 후 Redis Pub/Sub 발행.

---

## 6. 작업별 요청 접수

### 6.1 공통 순서

1. 전환 플래그(15.1절)가 꺼져 있으면 기존 동기 경로를 그대로 탄다.
2. **차단 플래그 확인**: Redis에 `planit:mq:block:taskqueue:{jobType}` 또는 `planit:mq:block:deadletterqueue:{jobType}`가 있으면 저장하지 않고 503을 반환한다.
3. 기존 사전 검증(권한, 재생성 횟수 등)을 수행한다.
4. 한 트랜잭션에서 작업 행(`QUEUED`, `active_slot=1`)과 Outbox 행을 저장한다.
5. 커밋 후 즉시 발행(4.2절)하고 202를 반환한다.

Redis 조회가 실패하면 차단하지 않은 것으로 보고 진행한다. 큐가 실제로 가득 차면 `reject-publish` → NACK → Outbox 실패 처리로 이어지므로 데이터는 유실되지 않는다.

### 6.2 응답 형식

202:

```json
{
  "code": "AI_JOB_ACCEPTED",
  "message": "AI 작업 요청이 접수되었습니다.",
  "data": { "jobType": "SCHEDULE_GENERATE", "jobId": "123", "status": "QUEUED" }
}
```

503 (`Retry-After: 60` 헤더 포함):

```json
{
  "code": "AI_JOB_TEMPORARILY_UNAVAILABLE",
  "message": "요청이 많아 잠시 후 다시 시도해 주세요.",
  "data": null
}
```

`ErrorCode`에 다음을 추가한다.

| 코드 | HTTP | 메시지 |
| --- | --- | --- |
| `AI_JOB_TEMPORARILY_UNAVAILABLE` | 503 | 요청이 많아 잠시 후 다시 시도해 주세요. |
| `AI_JOB_ALREADY_IN_PROGRESS` | 409 | 이미 진행 중인 AI 작업이 있습니다. |
| `IDEMPOTENCY_KEY_REUSED` | 409 | 다른 요청에 사용한 Idempotency-Key입니다. |

`GlobalExceptionHandler`는 `AI_JOB_TEMPORARILY_UNAVAILABLE`일 때 `Retry-After` 헤더를 `planit.mq.retry-after` 초로 붙인다.

### 6.3 일정 생성

- URL은 기존 `POST /api/trips/{tripId}/schedule-generation`을 유지한다. 플래그가 꺼져 있으면 200(동기), 켜져 있으면 202를 반환한다.
- `ScheduleGenerationPreparationService.prepare()`로 기존 검증과 AI 요청 본문을 만든다.
- `schedule_generation_jobs` 저장 값:

| 컬럼 | 값 |
| --- | --- |
| `requested_by_member_id` | 요청자 `trip_members.id` |
| `job_type` | `preparation.mode().name()` |
| `status` / `active_slot` | `QUEUED` / `1` |
| `idempotency_key` | 요청 헤더 `Idempotency-Key`가 있으면 그 값, 없으면 서버가 만든 UUID v4 |
| `input_cutoff_at` | 현재 시각 |

- `Idempotency-Key`는 UUID여야 한다. UUID가 아니면 400 `INVALID_REQUEST`를 반환한다(비동기 경로만).
- 같은 `Idempotency-Key`로 접수한 작업이 있으면 차단 확인, 사전 검증, 저장 없이 그 작업의 202 응답을 그대로 반환한다. 그 작업이 다른 여행의 작업이면 409 `IDEMPOTENCY_KEY_REUSED`를 반환한다.
- `(trip_id, active_slot)` 중복이면 409 `AI_JOB_ALREADY_IN_PROGRESS`를 반환한다. 같은 키로 동시에 들어온 요청이 먼저 저장해 유일 제약에 걸린 경우에는 먼저 저장된 작업의 202를 반환한다.
- 같은 키로 다시 요청하면 작업이 이미 끝났어도 응답 `status`는 `QUEUED`다. 결과는 알림으로 받는다.
- 메시지: routing key `ai-schedule-generation`, `deadlineAt = created_at + 28분`, `request = preparation.request()`.

### 6.4 포토미션 판정

- 사진 제출과 재판정 API에서 기존 `verificationService.verify()` 호출을 접수로 바꾼다.
- `photo_evaluations` 행을 `execution_status='QUEUED'`, `active_slot=1`, `execution_attempt_count=0`, `requested_at=now`로 저장한다. `(mission_photo_id, active_slot)` 중복이면 409.
- `image_url`은 만료 시각이 `deadlineAt + 30초`인 presigned URL로 발급한다. `ImageStorage`에 TTL을 받는 발급 메서드를 추가한다.
- 메시지: routing key `ai-photomission-verification`, `deadlineAt = requested_at + 16분`, `request = PhotoMissionVerifyRequest` + `mission_id`.
- 202 `data`는 `{ jobType: "PHOTO_JUDGE", jobId: evaluationId, status: "QUEUED" }`이다.

### 6.5 포토미션 생성

사용자 요청이 없으므로 차단 플래그로 503을 반환하지 않는다. 13절 스케줄러가 접수한다.

---

## 7. Outbox Relay — `MessageOutboxRelayScheduler`

```java
@Scheduled(fixedDelay = 500)
@SchedulerLock(name = "messageOutboxRelay", lockAtMostFor = "10s")
public void relay() { ... }
```

1. `SELECT id FROM message_outbox WHERE status='PENDING' AND next_attempt_at <= now ORDER BY id LIMIT :batchSize` (500).
2. 선점: `UPDATE message_outbox SET next_attempt_at = now + confirm-timeout + initial-grace WHERE id IN (...) AND status='PENDING' AND next_attempt_at <= now`. 조회 뒤 다른 경로가 먼저 처리했거나 시각을 미룬 행은 선점하지 않는다. 그다음 `next_attempt_at`이 이번 선점 시각인 행만 다시 읽어 발행한다.
3. 모든 행을 4.3절 방식으로 먼저 발행한다(confirm을 기다리지 않음).
4. 각 `CorrelationData` Future를 남은 시간(`confirm-timeout` 기준) 안에서 기다린 뒤 5절 처리기로 반영한다.

- lockAtMostFor 10s는 confirm 대기(5s)보다 길다. 작업이 빨리 끝나면 락은 바로 해제된다. 락이 잡혀 있으면 그 틱은 건너뛴다.
- 한 틱에 500건만 메모리에 올려 OOME를 막는다.

---

## 8. Stale Job Checker — `StaleJobChecker`

```java
@Scheduled(cron = "0 */3 * * * *", zone = "Asia/Seoul")
@SchedulerLock(name = "staleJobChecker", lockAtMostFor = "179s")
public void check() { ... }
```

| 작업 | 대상 조건 |
| --- | --- |
| 일정 생성 | `status='QUEUED' AND created_at < now - 28분` |
| 포토미션 판정 | `execution_status='QUEUED' AND requested_at < now - 16분` |
| 포토미션 생성 | `status='QUEUED' AND last_attempted_at < cutoff` (`cutoff` = now 이전의 가장 최근 06:00 KST) |

대상 ID를 조회한 뒤 **건마다 한 트랜잭션**(`StaleJobFailureService`)으로 처리한다. 한 건에서 오류가 나면 로그와 Sentry에 남기고 다음 건을 계속 처리한다. 포토미션 판정 Stale은 판정 작업에서 구현한다.

1. 작업 조건부 UPDATE → `FAILED`, `error_code='STALE'`, `active_slot=NULL`. 0행이면 다음 건.
2. `UPDATE message_outbox SET status='FAILED', last_error='STALE' WHERE job_type=? AND job_id=? AND status='PENDING'` (재발행 방지).
3. 실패 알림 저장 (포토미션 생성, 삭제된 여행 제외).
4. 커밋 후 Redis 발행.

사용자는 최악의 경우 기준 시간 + 3분 뒤 실패 알림을 받는다.

---

## 9. 큐 감시 — `MessageQueueMonitorScheduler`

```java
@Scheduled(fixedDelay = 5000)
@SchedulerLock(name = "messageQueueMonitor", lockAtMostFor = "4s")
public void monitor() { ... }
```

Management API `GET {base}/api/queues/planit/{queue}`를 `planit-monitor` 계정으로 호출해 `messages_ready`, `consumers`를 읽는다.

### 9.1 Task Queue 포화

| 조건 | 동작 |
| --- | --- |
| `messages_ready ≥ max-length × 0.95` | `SET planit:mq:block:taskqueue:{jobType} 1` |
| `messages_ready ≤ max-length × 0.50` | `DEL planit:mq:block:taskqueue:{jobType}` |

5초 동안 일정 생성은 최대 150건(30 RPS)이 들어오고, 3,450의 5%는 172건이므로 플래그가 켜지기 전에도 접수는 큐에 들어간다. 그래도 큐가 가득 차면 `reject-publish` → NACK → Outbox 재시도(5절)로 처리된다.

### 9.2 요청 DLQ

| 조건 | 동작 |
| --- | --- |
| `messages_ready ≥ DLQ max-length × 0.90` 또는 `consumers = 0` | `SET planit:mq:block:deadletterqueue:{jobType} 1`, Sentry 경고 |
| `messages_ready ≤ DLQ max-length × 0.50` 이고 `consumers > 0` | `DEL planit:mq:block:deadletterqueue:{jobType}` |

포토미션 생성 DLQ는 Consumer가 없으므로 `consumers` 조건을 적용하지 않는다(개수 조건만 적용).

### 9.3 응답 DLQ

`ai.result.dlq`의 `messages_ready ≥ 1`이면 Sentry 경고만 보낸다. 차단 플래그는 없다.

### 9.4 공통

- Management API 조회가 실패하면 플래그를 바꾸지 않고 Sentry 경고만 보낸다.
- Sentry 경고는 상태가 바뀔 때(플래그가 켜질 때, 응답 DLQ가 0에서 1 이상이 될 때)만 보낸다. 5초마다 같은 경고를 보내지 않는다.

---

## 10. Outbox 정리 — `MessageOutboxCleanupScheduler`

```java
@Scheduled(cron = "0 0 3 * * MON", zone = "Asia/Seoul")
@SchedulerLock(name = "messageOutboxCleanup", lockAtMostFor = "3600s")
public void cleanup() { ... }
```

| 대상 | 조건 |
| --- | --- |
| 발행 완료 | `status = 'PUBLISHED'` |
| 발행 실패 | `status = 'FAILED' AND created_at < now - 30일` |

대량 삭제로 락이 길어지지 않도록 `DELETE ... ORDER BY id LIMIT 1000`을 0행이 될 때까지 반복한다. 각 반복은 별도 트랜잭션이다.

- 삭제 단위 1,000건: PK 순서로 지우므로 한 번의 DELETE가 수십 ms 안에 끝나 Relay의 `PENDING` 조회와 락이 오래 겹치지 않는다. 1주일 분량(최대 수십만 행)도 수백 번 반복으로 lockAtMostFor(3600초) 안에 끝난다.

---

## 11. 결과 처리 — `AiResultListener`

### 11.1 Listener 설정

| 항목 | 값 |
| --- | --- |
| 큐 | `ai.result.task` |
| Consumer 수 | 4 (HikariCP 기본 10 × task 2개 기준, 커넥션을 과하게 쓰지 않도록) |
| prefetch | 20 |
| ACK | MANUAL |

```java
@RabbitListener(queues = "ai.result.task", containerFactory = "resultListenerFactory")
public void onMessage(Message message, Channel channel,
                      @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException { ... }
```

### 11.2 처리 순서

1. 본문을 `AiResultEnvelope`로 파싱한다. 파싱에 실패하면 Sentry 경고 후 `ai.dlx`/`ai-result.dlq`로 발행(`x-error-kind=INVALID_MESSAGE`)하고 confirm 후 ACK한다.
2. `jobType`으로 작업별 Handler를 고른다. Handler가 없는 작업 종류면 Sentry 경고 후 `x-error-kind=UNSUPPORTED_JOB_TYPE`으로 응답 DLQ에 보낸다.
3. **한 트랜잭션**에서 다음을 수행한다.
   - 대상 여행이 삭제됐으면(`deleted_at IS NOT NULL`): 조건부 UPDATE로 `FAILED`(`error_code='TRIP_DELETED'`). 결과와 알림은 저장하지 않는다.
   - `status = FAILED` 결과: 조건부 UPDATE로 `FAILED`(`error_code = errorKind`).
   - `status = SUCCEEDED` 결과: 작업별 결과 검증. 검증에 실패하면 조건부 UPDATE로 `FAILED`(`error_code='INVALID_AI_RESULT'`)와 실패 알림.
   - 검증 통과: 조건부 UPDATE로 `SUCCEEDED`. **1행일 때만** 결과를 저장하고 성공 알림을 저장한다.
4. 커밋 후 Redis Pub/Sub 발행, 그다음 `basicAck`.

조건부 UPDATE가 0행이면 이미 종료된 작업(성공, Stale, DLQ, 발행 실패)이다. 결과와 알림을 저장하지 않고 ACK한다. 같은 결과가 두 번 와도 한 번만 반영된다.

### 11.3 작업별 Handler

| 작업 | 조건부 UPDATE | 결과 반영 |
| --- | --- | --- |
| 일정 생성 (`ScheduleGenerationResultHandler`) | `UPDATE schedule_generation_jobs SET status=?, active_slot=NULL, finished_at=now, error_code=? WHERE id=? AND status='QUEUED'` | 아래 "일정 생성 결과 반영" 참고 |
| 포토미션 생성 (`MissionGenerationResultHandler`) | `UPDATE mission_generation_jobs SET status=? WHERE id=? AND status='QUEUED'` | 아래 "포토미션 생성 결과 반영" 참고. 알림 없음 |
| 포토미션 판정 | `UPDATE photo_evaluations SET execution_status=?, active_slot=NULL, execution_attempt_count=?, evaluated_at=now, error_code=? WHERE id=? AND execution_status='QUEUED'` | 기존 `PhotoMissionVerificationTransactionService.complete()`의 판정 결과·미션 완료 반영 로직을 쓴다. `execution_attempt_count`는 결과 메시지 헤더 `x-retry-count + 1` |

**일정 생성 결과 반영**

1. 진행 중(`QUEUED`)인 작업이 아니면 AI 응답을 읽지 않고 ACK한다.
2. 여행이 삭제됐으면 `FAILED(TRIP_DELETED)`.
3. 응답을 읽지 못하거나 `ScheduleAiResultValidator`(기존 `ScheduleGenerationServiceImpl`에서 분리한 검증)를 통과하지 못하면 `FAILED(INVALID_AI_RESULT)`와 실패 알림.
4. 트랜잭션 밖에서 `AiRecommendedPlaceMapper.mapRoutes()` → `ScheduleRoutePlanner.plan()`(동기 경로에서 분리한 경로 계산)을 먼저 한다. `RouteCalculationException`이면 조건을 만족하는 동선이 없을 때 `FAILED(SCHEDULE_ROUTE_NOT_FOUND)`, 장소 값이 잘못됐을 때 `FAILED(INVALID_AI_RESULT)`.
5. 장소를 저장한다(여행과 무관한 공용 데이터라 기존과 같이 따로 커밋).
6. 한 트랜잭션에서 작업을 `SUCCEEDED`로 바꾸고, 1행일 때만 `SchedulePersistenceService.saveCandidates(tripId, plannedRoutes, placeIds, ScheduleGenerationMode.valueOf(job_type))`와 성공 알림을 저장한다.
7. 6에서 업무 오류(`BusinessException`)가 나면 롤백한 뒤 새 트랜잭션에서 실패 처리한다. `error_code`는 오류 코드 이름(`SCHEDULE_REGENERATION_LIMIT_EXCEEDED` 등)이고, `TRIP_NOT_FOUND`는 `TRIP_DELETED`, `INVALID_AI_PLACE_RESULT`는 `INVALID_AI_RESULT`로 저장한다. 그 밖의 예외는 11.4절 재시도로 넘긴다.

**포토미션 생성 결과 반영**

1. 진행 중(`QUEUED`)인 작업이 아니면 ACK한다. 결과 본문을 읽지 못하면 `FAILED(INVALID_AI_RESULT)`.
2. 한 트랜잭션에서 작업을 `SUCCEEDED`로 바꾸고(여행이 삭제됐으면 `FAILED(TRIP_DELETED)`), 1행일 때만 `PhotoMissionGenerationTransactionService.createMissions()`(기존 `complete()`에서 분리한 미션·참여 상태 생성)를 실행한다.
3. `createMissions()`가 `PhotoMissionGenerationRejectedException`을 던지면 롤백한 뒤 새 트랜잭션에서 실패 처리한다. AI 결과가 방문 장소와 맞지 않거나 미션 범위가 잘못됐으면 `INVALID_AI_RESULT`, 확정 일정이나 방문 장소가 없으면 `MISSION_TARGET_NOT_FOUND`다. `mission_generation_jobs`에는 `error_code` 컬럼이 없어 로그로만 남긴다.

### 11.4 Result Worker 일시 오류

DB 일시 오류 등 3단계에서 예상하지 못한 예외가 나면 롤백 후 재시도한다.

| `x-result-retry-count` (없으면 0) | 발행 대상 |
| --- | --- |
| 0, 1, 2 | `ai.retry` / `ai-result.retry.{n+1}`, 헤더 `x-result-retry-count = n+1` |
| 3 | `ai.dlx` / `ai-result.dlq`, 헤더 `x-error-kind=RESULT_PROCESSING_FAILED`, `x-error-message`(500자), `x-failed-at` |

발행은 confirm을 받은 뒤 원본을 ACK한다. confirm 실패 시 `basicNack(tag, false, true)`로 재전달한다. 재전달이 5회를 넘으면 RabbitMQ가 `ai-result.dlq`로 보낸다.

재발행 메시지는 원본 본문, `message_id`, `correlation_id`, `type`과 헤더를 그대로 옮긴다. RabbitMQ가 붙인 `x-death`, `x-first-death-*`, `x-last-death-*` 헤더는 옮기지 않는다.

### 11.5 응답 DLQ 수동 재처리

응답 DLQ에는 AI가 정상 처리한 결과가 있을 수 있으므로 자동으로 실패 처리하지 않는다. Sentry 경고를 받으면 원인을 해결한 뒤 Management UI의 **Move messages**(Shovel 플러그인 `rabbitmq_shovel`, `rabbitmq_shovel_management` 활성화 필요)로 `ai.result.dlq`의 메시지를 `ai.result.task`로 옮긴다. 결과 반영은 조건부 UPDATE이므로 이미 Stale로 실패 처리된 작업은 0행이 되어 무시된다.

---

## 12. 요청 DLQ 처리 — `AiDlqListener`

### 12.1 대상

| 큐 | Consumer |
| --- | --- |
| `ai.schedule-generation.dlq` | 있음 (`AiDlqListener`) |
| `ai.photomission-verification.dlq` | 있음 (포토미션 판정 작업에서 구현) |
| `ai.photomission-generation.dlq` | **없음** (13.2절 05시 재처리) |

| 항목 | 값 |
| --- | --- |
| Consumer 수 | 큐마다 1개 |
| prefetch | Spring AMQP 기본값(250) |
| ACK | MANUAL |

- Consumer 1개: DLQ는 장애 때만 쌓이고, 1건 처리가 조건부 UPDATE와 알림 저장 한 트랜잭션(수십 ms)이므로 1개로도 초당 수십 건을 처리한다. 일정 생성 30 RPS가 모두 실패하는 최악의 경우에도 따라갈 수 있고, HikariCP 커넥션을 1개만 쓴다.
- Consumer가 0이 되면 9.2절 모니터가 차단 플래그를 켠다.

### 12.2 처리 순서

1. 작업 ID와 종류는 본문이 아니라 Basic Properties의 `correlation_id`, `type`에서 읽는다(파싱 실패 메시지도 처리하기 위해).
2. `error_code`는 `x-error-kind` 헤더 값(최대 100자)이다. 없고 `x-death[0].reason == "delivery_limit"`이면 `DELIVERY_LIMIT`, 둘 다 아니면 `DEAD_LETTERED`이다.
   - `correlation_id`가 숫자가 아니거나 `type`이 큐의 작업 종류와 다르면 작업을 알 수 없으므로, 속성과 본문 앞 200자를 Sentry 오류로 남기고 ACK한다. 그 작업은 Stale Job Checker가 실패 처리한다.
3. 한 트랜잭션에서 작업 조건부 UPDATE → `FAILED`, `active_slot=NULL`. 1행이고 여행이 삭제되지 않았으면 실패 알림을 저장한다.
4. 커밋 후 Redis 발행, 그다음 ACK. 예외가 나면 `basicNack(tag, false, true)`.

**공통 종료 규칙**: 모든 종료 전이는 `WHERE status = 'QUEUED'`(판정은 `execution_status`) 조건부 UPDATE이고, 종료 시 `active_slot = NULL`로 바꾼다. 0행이면 알림을 만들지 않는다.

---

## 13. 포토미션 생성

### 13.1 스케줄러 변경

기존 5분 주기 스케줄러(`PhotoMissionGenerationScheduler`)와 00:30 마감 규칙은 플래그가 켜지면 바로 끝난다. 미션 공개 시각(00:30)은 그대로다.

```java
// PhotoMissionGenerationEnqueueScheduler
@Scheduled(cron = "0 0 0 * * *", zone = "Asia/Seoul")
@SchedulerLock(name = "photoMissionGenerationEnqueue", lockAtMostFor = "PT5M")
public void enqueue() { ... }
```

- 락 이름은 기존 스케줄러(`photoMissionGeneration`)와 나눈다. 같은 이름이면 00:00에 기존 스케줄러가 락을 먼저 잡았을 때 새 스케줄러가 그날 접수를 건너뛴다. 두 스케줄러는 플래그로 한쪽만 일하므로 락을 나눠도 같은 여행을 두 번 처리하지 않는다.
- 플래그가 꺼져 있거나 Redis 조회가 실패하면 접수하지 않는다(기존 스케줄러가 동작).
- 대상 여행 조회는 기존 `TripRepository.findPhotoMissionGenerationTargetIds`를 쓴다.
- 여행마다 한 트랜잭션(`PhotoMissionGenerationEnqueueService`)에서 `mission_generation_jobs`를 `QUEUED`로 저장하고 Outbox 행을 만든다. 커밋 후 즉시 발행한다. 한 여행이 실패해도 로그와 Sentry에 남기고 다음 여행을 처리한다.
  - 행이 없으면 새로 만든다(`attempt_count=1`).
  - `FAILED`이거나 동기 실행 중 5분 넘게 멈춘 `RUNNING`이면 다시 접수한다(`status='QUEUED'`, `attempt_count+1`, `last_attempted_at=now`).
  - 아직 동기 실행 중인 `RUNNING`, 이미 접수한 `QUEUED`, `SUCCEEDED`는 다시 접수하지 않는다.
- 메시지: routing key `ai-photomission-generation`, `deadlineAt` = 당일 06:00 KST, `request = loadRequest()`의 결과.

### 13.2 05시 DLQ 재처리 — `PhotoMissionGenerationDlqReprocessor`

포토미션 판정과 구분되도록 클래스와 락 이름에 생성 작업임을 드러낸다.

```java
@Scheduled(cron = "0 0 5 * * *", zone = "Asia/Seoul")
@SchedulerLock(name = "photoMissionGenerationDlqReprocessor", lockAtMostFor = "PT60S")
public void reprocess() { ... }
```

`rabbitTemplate.execute(channel -> ...)` 안에서 반복한다.

1. `GetResponse r = channel.basicGet("ai.photomission-generation.dlq", false)`. `null`이면 종료한다.
2. 본문의 `deadlineAt`이 지났으면 발행하지 않고 `basicAck`한다(이미 Stale 대상). 본문에서 `deadlineAt`을 읽을 수 없으면(JSON이 아니거나 값이 없거나 형식이 틀림) Sentry 오류로 남기고 `basicAck`해 버린다. 이런 메시지는 AI Worker도 영구 오류로 처리한다.
3. 같은 본문과 `message_id`, `correlation_id`, `type`으로 헤더 `x-retry-count = 4`를 붙여 `ai.request` / `ai-photomission-generation`에 발행한다. `x-error-*`, `x-failed-at`, `x-death`, `x-first-death-*`, `x-last-death-*` 헤더는 복사하지 않는다.
4. confirm(ack, return 없음)을 받으면 `basicAck(r.getEnvelope().getDeliveryTag(), false)`.
5. confirm 실패나 시간 초과면 `basicNack(tag, false, true)`로 되돌리고 반복을 멈춘다. Sentry 경고를 보낸다.

작업 행은 `QUEUED`로 남아 있으므로 상태를 바꾸지 않는다. 재처리에서도 실패하면 AI Worker가 `status=FAILED` 결과를 보내고, Result Worker가 `FAILED`로 바꾼다. 결과가 오지 않으면 06:00 Stale이 실패 처리한다.

lockAtMostFor는 60초다. 05:00에 한 번만 실행되므로, 반복이 60초를 넘겨 락이 풀려도 다른 인스턴스가 같은 틱에 다시 실행하지는 않는다.

---

## 14. 알림

### 14.1 타입 추가

`NotificationType`:

| 값 | 시점 | 수신자 | 대상 |
| --- | --- | --- | --- |
| `SCHEDULE_GENERATION_SUCCEEDED` | 일정 생성 성공 | 여행방 활성 멤버 전원 | `TRIP` / tripId |
| `SCHEDULE_GENERATION_FAILED` | 일정 생성 실패 (AI 실패, DLQ, Stale, 발행 실패) | 여행방 활성 멤버 전원 | `TRIP` / tripId |
| `PHOTO_EVALUATION_COMPLETED` | 판정 완료 (성공, 재촬영, 실패 판정) | 사진을 제출한 멤버 | `MISSION_PHOTO` / photoId |
| `PHOTO_EVALUATION_FAILED` | 판정 자체를 못 함 (AI 오류, DLQ, Stale, 발행 실패) | 사진을 제출한 멤버 | `MISSION_PHOTO` / photoId |

`NotificationTargetType`에 `MISSION_PHOTO`를 추가한다. 포토미션 생성은 알림이 없다.

### 14.2 문구

| 상황 | 제목 | 본문 |
| --- | --- | --- |
| 일정 생성 성공 | 일정이 완성됐어요 | {여행 이름}의 일정 3가지를 확인해 보세요. |
| 일정 생성 실패 | 일정을 만들지 못했어요 | 잠시 후 다시 시도해 주세요. |
| 판정 완료 (성공) | 미션 성공! | {장소} 미션을 완료했어요. |
| 판정 완료 (재촬영) | 다시 찍어 주세요 | {retry_hint} |
| 판정 실패 | 사진을 확인하지 못했어요 | 잠시 후 다시 제출해 주세요. |

### 14.3 저장과 발행

- 작업 상태 변경과 같은 트랜잭션에서 `NotificationStore.saveIdempotently()`로 저장한다(`delivered_at = NULL`).
- `dedupeKey`는 `AI_JOB:{jobType}:{jobId}:{notificationType}:{recipientId}`로 만들어, 재전달로 같은 알림이 두 번 저장되지 않게 한다.
- 커밋 후 `NotificationEventPublisher.publish()`로 Redis Pub/Sub(`planit:notifications`)에 발행한다. WebSocket 경로는 기존 `/user/queue/notifications`이다.
- 클라이언트는 `delivery-ack`으로 수신을 알리고, 재진입 시 `pending-deliveries`로 복구한다(기존 API).

---

## 15. 운영 전환과 롤백

### 15.1 작업별 전환 플래그

| 작업 | Redis 키 |
| --- | --- |
| 일정 생성 | `planit:mq:async:SCHEDULE_GENERATE` |
| 포토미션 생성 | `planit:mq:async:PHOTO_MISSION_GENERATE` |
| 포토미션 판정 | `planit:mq:async:PHOTO_JUDGE` |

키 값이 `on`이면 비동기 경로, 없거나 다른 값이면 기존 동기 경로다. 재배포 없이 `redis-cli SET/DEL`로 바로 전환·롤백한다.

- 롤백(키 삭제) 후에도 이미 `QUEUED`로 저장된 작업은 Result Worker, DLQ Consumer, Stale이 계속 처리한다. 비동기 컴포넌트는 플래그와 무관하게 항상 동작한다.
- 포토미션 생성은 기존 스케줄러와 새 스케줄러 중 플래그에 맞는 하나만 동작한다.

### 15.2 전환 순서

1. RabbitMQ 배포(계약 문서 2절)와 백엔드 배포(토폴로지 선언). **AI 서버는 선언 권한이 없으므로 백엔드가 먼저 연결해야 한다.**
2. AI Worker 배포.
3. 202 응답을 처리하는 프론트 배포.
4. 포토미션 판정 → 포토미션 생성 → 일정 생성 순서로 플래그를 켠다.
   - 포토미션 생성 플래그는 00:30 이후에 켠다. 새 스케줄러는 00:00에 한 번만 돌기 때문에 00:00~00:30 사이에 켜면 기존 스케줄러는 멈추고 새 스케줄러는 다음 날에야 돌아, 그날 아직 생성하지 않은 여행은 미션이 생기지 않는다.
5. 오류가 나면 해당 작업의 플래그를 지워 동기 경로로 되돌리고 원인을 분석한다.

### 15.3 완전 전환 조건

동기 경로 요청이 0건이 된 뒤 동기 처리 최대 시간이 지나고 일정 기간 문제가 없으면 완전 전환한다. 그 후 2.4절 제거 단계와 기존 재시도 로직 제거를 진행한다.

### 15.4 큐 인자 변경 절차

기존 큐의 인자는 바꿀 수 없다. 해당 작업 플래그를 끄고 큐를 비운 뒤(또는 Shovel로 임시 큐에 옮긴 뒤) Management UI에서 큐를 삭제하고, 새 인자로 백엔드를 배포해 다시 선언한다.

---

## 16. 테스트

Testcontainers RabbitMQ와 MySQL을 쓴다. confirm 유실, 연결 끊김은 Toxiproxy로 만든다.

| 구분 | 시나리오 | 기대 결과 |
| --- | --- | --- |
| 토폴로지 | 앱 기동 | 계약 문서 3절의 큐 20개, 인자 일치 |
| 발행 | 정상 발행 | Outbox `PUBLISHED`, `published_at` 기록 |
| 발행 | routing key 오타(return) | 즉시 Outbox·작업 `FAILED`, 실패 알림 1건 |
| 발행 | Task Queue 가득 참(NACK) | 5/10/20초 재시도 후 4번째 NACK에서 `FAILED` |
| 발행 | Toxiproxy로 confirm 지연 | `failure_attempt_count` 증가, 5회째 `FAILED` |
| 발행 | 커밋 직후 앱 종료 | Relay가 `next_attempt_at` 이후 재발행 |
| 접수 | 차단 플래그 존재 | 503, `Retry-After: 60`, 작업·Outbox 저장 없음 |
| 접수 | 같은 여행 중복 요청 | 409 `AI_JOB_ALREADY_IN_PROGRESS` |
| 결과 | 같은 결과 2회 | 결과·알림 1회만 저장 |
| 결과 | Stale 후 늦은 성공 결과 | `FAILED` 유지, 알림 없음 |
| 결과 | 삭제된 여행 | `FAILED(TRIP_DELETED)`, 알림 없음 |
| 결과 | DB 예외 | `ai-result.retry.1~3` 후 `ai.result.dlq` |
| DLQ | `delivery_limit` 메시지 | `FAILED(DELIVERY_LIMIT)`, 실패 알림 |
| Stale | 기준 시간 초과 | `FAILED(STALE)`, Outbox `PENDING` → `FAILED` |
| 모니터 | `messages_ready` 95% | 차단 플래그 생성, 50%에서 해제 |
| 모니터 | DLQ consumer 0 | 차단 플래그 + Sentry (포토미션 생성 제외) |
| 05시 재처리 | DLQ 메시지 1건 | `x-retry-count=4`로 Task Queue 이동, DLQ ACK |
| 05시 재처리 | 재발행 confirm 실패 | 메시지가 DLQ에 남고 재처리 중단 |
| 정리 | 월요일 03:00 | `PUBLISHED` 전부, 30일 지난 `FAILED` 삭제 |

---

## 17. 결정이 필요한 값

다음 값은 스펙에 없거나 측정이 필요하다. 정해지면 이 문서와 설정에 반영한다.

| 항목 | 위치 | 상태 |
| --- | --- | --- |
| `CONFIRM_TIMEOUT` | `planit.mq.confirm-timeout` | 5초 임시, 부하 테스트 max로 확정 |
