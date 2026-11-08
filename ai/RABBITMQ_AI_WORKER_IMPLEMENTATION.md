# RabbitMQ 비동기 AI 작업 — AI Worker 구현 문서

이 문서는 FastAPI AI 서버가 구현할 RabbitMQ Worker를 정의한다. 큐 이름, 메시지 형식, 헤더, `errorKind`는 [`../infra/RABBITMQ_INFRA_AND_CONTRACT.md`](../infra/RABBITMQ_INFRA_AND_CONTRACT.md)(이하 **계약 문서**)를 따른다.

AI 서버는 **큐, Exchange, binding을 선언하지 않는다.** 모두 백엔드가 선언하며, AI 계정(`planit-ai`)에는 선언 권한이 없다. AI 서버는 이미 있는 큐와 Exchange를 passive로 조회해서 쓴다.

## 0. 구현 범위

| 순서 | 범위 | 절 |
| ---: | --- | --- |
| 1 | 의존성, 환경 변수, 연결 | 1절 |
| 2 | Worker 처리 순서 | 2절 |
| 3 | 오류 분류와 발행 대상 | 3절 |
| 4 | 발행과 confirm, 발행 실패 처리 | 4절 |
| 5 | 시간대별 Worker 수 조절 | 5절 |
| 6 | 종료(Scale-in) | 6절 |
| 7 | 지표, 테스트 | 7절, 8절 |

기존 HTTP API(`/trips/place-selection`, `/photomissions/generate`, `/photomissions/{missionId}/verify`)는 전환 기간 동안 유지한다. 백엔드가 작업별 플래그로 동기 경로로 롤백할 수 있어야 하기 때문이다. Worker는 HTTP를 거치지 않고 같은 서비스 함수를 직접 호출한다.

---

## 1. 의존성과 연결

### 1.1 의존성

```text
aio-pika>=9.4,<10
```

### 1.2 환경 변수

| 이름 | 예시 | 설명 |
| --- | --- | --- |
| `RABBITMQ_URL` | `amqp://planit-ai:{password}@rabbitmq.planit.local:5672/planit` | vhost `planit` |
| `MQ_CONFIRM_TIMEOUT_SECONDS` | `5` | 발행 confirm 대기시간. 백엔드 `CONFIRM_TIMEOUT`과 같은 값 |
| `MQ_REPUBLISH_MAX_RETRIES` | `3` | 결과 발행 프로세스 내 재발행 횟수 |
| `MQ_REPUBLISH_BACKOFF_BASE_SECONDS` | `5` | 재발행 백오프 시작 간격 (5s, 10s, 20s) |
| `MQ_WORKER_SCHEDULE` | 5.2절 | 시간대별 Worker 수 |
| `MQ_SHUTDOWN_GRACE_SECONDS` | `120` | 종료 시 처리 중 작업을 기다리는 최대 시간 |

### 1.3 작업별 고정 값

| 작업 (`jobType`) | 소비 큐 | routing key `{rk}` | AI timeout |
| --- | --- | --- | --- |
| `SCHEDULE_GENERATE` | `ai.schedule-generation.task` | `ai-schedule-generation` | 120초 |
| `PHOTO_MISSION_GENERATE` | `ai.photomission-generation.task` | `ai-photomission-generation` | 110초 |
| `PHOTO_JUDGE` | `ai.photomission-verification.task` | `ai-photomission-verification` | 30초 |

### 1.4 연결과 채널

- 프로세스당 연결 1개를 `aio_pika.connect_robust()`로 만든다. 연결이 끊기면 자동으로 다시 연결하고 Consumer를 복구한다.
- **Worker 1개 = 채널 1개 = Consumer 1개**다. 채널은 `publisher_confirms=True`, `on_return_raises=True`로 열고 `prefetch_count=1`을 설정한다. 소비, 발행, ACK를 모두 같은 채널에서 한다.
- 큐와 Exchange는 `channel.get_queue(name, ensure=True)`, `channel.get_exchange(name, ensure=True)`로 passive 조회한다. 없으면 기동을 실패시킨다(백엔드가 아직 선언하지 않은 상태).

```python
connection = await aio_pika.connect_robust(settings.rabbitmq_url)

async def open_worker_channel(queue_name: str):
    channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
    await channel.set_qos(prefetch_count=1)
    queue = await channel.get_queue(queue_name, ensure=True)
    exchanges = {
        name: await channel.get_exchange(name, ensure=True)
        for name in ("ai.result", "ai.retry", "ai.dlx")
    }
    return channel, queue, exchanges
```

Worker는 FastAPI `lifespan`에서 시작하고 종료한다.

---

## 2. Worker 처리 순서

메시지 1건을 다음 순서로 처리한다. **ACK는 다음 목적지 발행의 confirm을 받은 뒤에만** 한다.

| 단계 | 내용 |
| ---: | --- |
| 1 | 소비 (prefetch 1) |
| 2 | 본문 파싱과 검증. 실패하면 `INVALID_MESSAGE`로 DLQ 발행 (3.3절) |
| 3 | 기한 확인: 현재 시각 > `deadlineAt`이면 AI를 호출하지 않고 ACK 후 종료. 이 작업은 Stale Job Checker의 실패 처리 대상이다. |
| 4 | AI 호출 (1.3절 timeout) |
| 5 | 결과에 따라 다음 목적지 결정 (3.2절) |
| 6 | 발행 후 confirm 대기 (4절) |
| 7 | confirm 성공 시 요청 메시지 ACK |

```python
async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
    retry_count = int(message.headers.get("x-retry-count", 0))
    try:
        envelope = parse_envelope(message.body)            # schemaVersion, jobId, jobType, deadlineAt, request
    except InvalidEnvelope as e:
        await publish_and_ack(message, to_dlq(message, "INVALID_MESSAGE", str(e)))
        return
    if envelope.schema_version != 1:
        await publish_and_ack(message, to_dlq(message, "UNSUPPORTED_SCHEMA_VERSION", ...))
        return
    if now_kst() > envelope.deadline_at:
        await message.ack()
        return

    try:
        result = await run_ai(envelope, timeout=AI_TIMEOUT[envelope.job_type])
        outgoing = to_result(message, envelope, status="SUCCEEDED", result=result)
    except AiError as e:
        outgoing = route_failure(message, envelope, retry_count, e)   # 3.2절

    await publish_and_ack(message, outgoing)                          # 4절
```

- 기존 AI 서비스 함수가 동기(blocking) 함수면 `asyncio.to_thread()`로 감싸서 이벤트 루프를 막지 않게 한다. 이때 `asyncio.wait_for()`의 timeout은 스레드를 멈추지 못하므로, 서비스 함수 안의 RunPod·Gemini 호출에도 같은 timeout을 건다.
- 재전달된 메시지(`message.redelivered == True`)도 똑같이 처리한다. AI가 두 번 실행될 수 있지만 허용한다. 정합성은 백엔드의 조건부 UPDATE가 보장한다.

---

## 3. 오류 분류와 발행 대상

### 3.1 `errorKind` 분류

| 예외 | `errorKind` | 재시도 |
| --- | --- | --- |
| `asyncio.TimeoutError` (AI timeout) | `AI_TIMEOUT` | 가능 |
| RunPod 429, 5xx, 네트워크 오류 | `AI_UNAVAILABLE` | 가능 |
| Gemini 429, 5xx, 네트워크 오류 | `AI_UNAVAILABLE` | 가능 |
| AI 서버 DB 일시 오류 | `AI_UNAVAILABLE` | 가능 |
| 그 밖의 예상하지 못한 AI 서버 오류 | `AI_UNAVAILABLE` | 가능 |
| `request` 입력 검증 실패 (pydantic `ValidationError` 등) | `INVALID_REQUEST` | 불가 |
| 판정 사진 없음 (이미지 URL 404) | `IMAGE_NOT_FOUND` | 불가 |
| 지원하지 않는 이미지 형식 | `UNSUPPORTED_IMAGE` | 불가 |

### 3.2 다음 목적지

`x-retry-count`는 요청 메시지 헤더 값이다(없으면 0).

| 상황 | Exchange / routing key | 헤더 |
| --- | --- | --- |
| 성공 | `ai.result` / `ai-result` | `x-retry-count` 복사 |
| `x-retry-count` = 4 (05시 재처리)에서 실패, 오류 종류 무관 | `ai.result` / `ai-result`, `status=FAILED` | `x-retry-count` 복사 |
| 재시도 가능, `x-retry-count` < 3 | `ai.retry` / `{rk}.retry.{x-retry-count + 1}` | `x-retry-count + 1` |
| 재시도 가능, `x-retry-count` = 3 | `ai.dlx` / `{rk}.dlq` | `x-error-kind`, `x-error-message`, `x-failed-at` |
| 재시도 불가 | `ai.dlx` / `{rk}.dlq` | `x-error-kind`, `x-error-message`, `x-failed-at` |

```python
def route_failure(message, envelope, retry_count, error) -> Outgoing:
    if retry_count == 4:
        return to_result(message, envelope, status="FAILED", error_kind=error.kind, result=None)
    if error.retryable and retry_count < 3:
        return to_retry(message, rk=RK[envelope.job_type], next_count=retry_count + 1)
    return to_dlq(message, error.kind, error.message)
```

### 3.3 메시지 만들기

| 대상 | 본문 | Basic Properties |
| --- | --- | --- |
| 결과 | 계약 문서 4.4절 (`requestMessageId` = 요청 `message_id`) | `message_id` 새 UUID, `correlation_id` = `jobId`, `type` = `{jobType}_RESULT`, `app_id` = `planit-ai`, `timestamp` = epoch 초, `delivery_mode` = 2, `content_type` = `application/json` |
| 재시도 | 요청 본문 그대로 | 요청 속성 그대로, `x-retry-count`만 +1 |
| DLQ | 요청 본문 그대로 | 요청 속성 그대로 + `x-error-kind`, `x-error-message`(최대 500자), `x-failed-at`(ISO-8601 `+09:00`) |

본문을 파싱하지 못한 `INVALID_MESSAGE`도 요청의 `correlation_id`, `type` 속성을 그대로 복사한다. 백엔드 DLQ Consumer는 이 속성으로 작업을 찾는다. `{rk}`는 본문이 아니라 **소비한 큐**로 정한다.

---

## 4. 발행과 confirm

### 4.1 confirm 판정

```python
from pamqp.commands import Basic

async def publish_once(exchange, outgoing) -> None:
    confirmation = await exchange.publish(
        outgoing.message,
        routing_key=outgoing.routing_key,
        mandatory=True,
        timeout=settings.confirm_timeout_seconds,
    )
    if not isinstance(confirmation, Basic.Ack):
        raise PublishNacked()
```

| 신호 | aio-pika에서 나타나는 형태 | 분류 |
| --- | --- | --- |
| ack, return 없음 | `Basic.Ack` 반환 | 성공 |
| ack + return (라우팅할 큐 없음) | `on_return_raises=True`이면 `aio_pika.exceptions.DeliveryError` | 발행 실패 |
| nack (`reject-publish` 등) | `DeliveryError` 또는 `Basic.Nack` 반환 | 발행 실패 |
| confirm 시간 초과 | `asyncio.TimeoutError` | 발행 실패 |
| 연결·채널 끊김 | `aio_pika.exceptions.AMQPConnectionError`, `ChannelInvalidStateError` 등 | 연결 오류 |

aio-pika 버전마다 예외 이름이 조금 다를 수 있으므로, 고정한 버전에서 위 다섯 경우를 테스트로 확인한다(8절).

### 4.2 발행 실패 처리

발행에 실패하면 **요청 메시지를 ACK하지 않는다.**

1. **프로세스 내 재발행**: 같은 메시지를 최대 3회 다시 발행한다. AI는 다시 호출하지 않는다. 간격은 백엔드 Outbox와 같다.

   ```text
   backoff = 5s × 2^(n-1)          # n = 1, 2, 3 → 5s, 10s, 20s
   sleep(backoff + random(0, backoff / 2))
   ```

2. **재발행 소진**: 이 Worker의 Consumer를 취소하고 채널을 닫는다. 채널이 닫히면 ACK하지 않은 요청 메시지가 Task Queue로 돌아가고 전달 횟수가 1 늘어난다. 새 채널을 열어 Consumer를 다시 등록한다. 재전달된 메시지를 받은 Worker는 AI를 다시 호출한다.
3. **재전달 한도**: 재전달이 `x-delivery-limit`(5회)을 넘으면 RabbitMQ가 메시지를 `{rk}.dlq`로 dead-letter한다. AI Worker가 DLQ로 보내는 것이 아니다.
4. **연결 오류**: 연결이 끊긴 경우에는 ACK를 할 수 없으므로 재발행하지 않는다. 메시지는 자동으로 재전달되고 `connect_robust`가 Consumer를 복구한다.

```python
async def publish_and_ack(message, outgoing) -> None:
    for attempt in range(settings.republish_max_retries + 1):   # 최초 1회 + 재발행 3회
        try:
            await publish_once(exchanges[outgoing.exchange], outgoing)
            await message.ack()
            return
        except CONNECTION_ERRORS:
            return                                              # 재전달에 맡긴다
        except (PublishNacked, aio_pika.exceptions.DeliveryError, asyncio.TimeoutError):
            if attempt == settings.republish_max_retries:
                break
            backoff = settings.backoff_base * (2 ** attempt)
            await asyncio.sleep(backoff + random.uniform(0, backoff / 2))
    await worker.restart_channel()                              # 소비 중단 → 채널 재생성 → 재전달
```

ACK 전에 Worker가 죽어도 메시지는 재전달된다. 이때 생기는 AI 중복 실행은 허용하고, 백엔드가 조건부 UPDATE로 한 번만 반영한다.

---

## 5. Worker 수

### 5.1 설계값

Worker 수는 시나리오를 만족하는 설계값이다. RunPod 접근 Worker가 Task당 최대 26개이므로 **각 AI Task의 Worker 합계는 26개 이하**로 둔다. 운영은 AI Task 6개 기준으로, 설계값의 비율을 유지해 Task마다 나눠 배치한다.

| 시간대 (KST) | 일정 생성 | 포토미션 판정 | 포토미션 생성 | 합계 (설계) |
| --- | ---: | ---: | ---: | ---: |
| 06:00 ~ 00:00 | 90 | 44 | 0 | 134 |
| 00:00 ~ 06:00 | 81 | 30 | 23 | 134 |

- 포토미션 생성은 00:00~06:00에 **Task당 4개**(6 Task 기준)로 고정한다. 그 밖의 시간에는 0개다.
- 모든 Worker의 prefetch는 1이다.

### 5.2 설정 형식

```json
{
  "06:00-00:00": { "SCHEDULE_GENERATE": 15, "PHOTO_JUDGE": 8, "PHOTO_MISSION_GENERATE": 0 },
  "00:00-06:00": { "SCHEDULE_GENERATE": 14, "PHOTO_JUDGE": 5, "PHOTO_MISSION_GENERATE": 4 }
}
```

`MQ_WORKER_SCHEDULE`에는 **Task 1개 기준 Worker 수**를 넣는다. 5.1절 설계값을 AI Task 6개로 나누고 소수점은 올림한 값이다.

| 시간대 (KST) | 일정 생성 | 포토미션 판정 | 포토미션 생성 | Task당 합계 |
| --- | ---: | ---: | ---: | ---: |
| 06:00 ~ 00:00 | 15 (90 ÷ 6) | 8 (44 ÷ 6 올림) | 0 | 23 |
| 00:00 ~ 06:00 | 14 (81 ÷ 6 올림) | 5 (30 ÷ 6) | 4 | 23 |

Task당 합계는 RunPod 접근 Worker 한도(26) 이하다.

### 5.3 시간대별 조절

Worker 수가 바뀌는 시각은 00:00과 06:00 두 번뿐이다. 따라서 주기적으로 시각을 확인하지 않고, **다음 경계 시각까지 기다렸다가 한 번 적용**한다.

1. 기동 직후 KST 현재 시각이 속한 시간대의 Worker 수로 시작한다. 태스크가 재시작돼도 현재 시각에 맞는 수로 시작한다.
2. 다음 경계 시각(00:00 또는 06:00 KST)까지 `asyncio.sleep()`으로 기다린 뒤, 새 시간대의 목표 수를 적용하고 다시 다음 경계까지 기다린다.
3. **줄일 때**: 줄일 Worker의 Consumer를 `queue.cancel(consumer_tag)`(`basic.cancel`)로 취소해 새 메시지를 받지 않게 한다. 처리 중인 메시지를 끝낸(발행·ACK 완료) 뒤 채널을 닫는다.
4. **늘릴 때**: 새 채널을 열고 Consumer를 등록한다.

```python
async def worker_schedule_loop():
    await apply(target_for(now_kst()))
    while True:
        boundary = next_boundary(now_kst())          # 다음 00:00 또는 06:00
        await asyncio.sleep((boundary - now_kst()).total_seconds())
        await apply(target_for(boundary))
```

`asyncio.sleep`이 늦게 깨어나도 `target_for(boundary)`로 적용하므로 시간대가 틀어지지 않는다.

---

## 6. 종료 (Scale-in, 배포)

ECS `stopTimeout`은 최대값인 120초다. SIGTERM을 받으면 다음을 수행한다.

1. 모든 Consumer를 `basic.cancel`로 취소한다.
2. 처리 중인 메시지가 끝날 때까지 최대 `MQ_SHUTDOWN_GRACE_SECONDS`(120초)만큼 기다린다.
3. 모두 끝나면 연결을 닫고 종료한다.

대기시간이 `stopTimeout`과 같은 120초이므로, 120초 안에 끝나지 않으면 ECS가 SIGKILL로 프로세스를 종료한다. 이때 ACK하지 않은 메시지는 연결이 끊기면서 Task Queue로 돌아가 다른 Task가 처리한다(재전달 1회 증가). 유실은 없다.

가장 긴 포토미션 생성(110초)에 결과 재발행 시간이 더해지면 120초를 넘을 수 있다. 이 경우 재전달되며, 중복 실행은 백엔드가 처리한다.

---

## 7. 지표와 로그

| 지표 | 의미 |
| --- | --- |
| `ai_worker_messages_total{job_type, outcome}` | outcome: `succeeded`, `retry`, `dlq`, `failed_result`, `deadline_skipped` |
| `ai_worker_redelivered_total{job_type}` | `message.redelivered == True`인 메시지 수. 비율이 오르면 결과 발행 실패나 Worker 비정상 종료를 의심 |
| `ai_worker_publish_failures_total{job_type, kind}` | kind: `nack`, `return`, `timeout`, `connection` |
| `ai_worker_processing_seconds{job_type}` | AI 호출 시간 (평균 처리 시간 측정용) |
| `ai_worker_active{job_type}` | 현재 Worker 수 |

로그에는 `jobId`, `jobType`, `message_id`, `x-retry-count`, `redelivered`를 함께 남긴다.

---

## 8. 테스트

RabbitMQ 컨테이너에 계약 문서 3절 토폴로지를 선언한 뒤(테스트용 관리자 계정) Worker를 `planit-ai` 권한으로 실행한다.

| 시나리오 | 기대 결과 |
| --- | --- |
| 정상 처리 | `ai.result.task`에 `SUCCEEDED` 결과, 요청 ACK |
| 본문 JSON 깨짐 | `{rk}.dlq`에 `x-error-kind=INVALID_MESSAGE`, `correlation_id`·`type` 유지 |
| `schemaVersion=2` | `{rk}.dlq`에 `UNSUPPORTED_SCHEMA_VERSION` |
| `deadlineAt` 지남 | AI 호출 없음, 발행 없음, ACK |
| AI timeout, `x-retry-count=0` | `{rk}.retry.1`에 `x-retry-count=1` |
| AI timeout, `x-retry-count=3` | `{rk}.dlq`에 `AI_TIMEOUT` |
| 이미지 404 | 재시도 없이 `{rk}.dlq`에 `IMAGE_NOT_FOUND` |
| `x-retry-count=4`에서 실패 | `ai.result.task`에 `status=FAILED` |
| `ai.result.task` 가득 참 (nack) | 5/10/20초 재발행 후 채널 재생성, 메시지 재전달 |
| 존재하지 않는 routing key (return) | `DeliveryError`로 발행 실패 처리 |
| 처리 중 연결 끊김 | 재발행 없음, 메시지 재전달, Consumer 자동 복구 |
| 재전달 6회 | RabbitMQ가 `{rk}.dlq`로 dead-letter (`x-death.reason=delivery_limit`) |
| 시간대 변경 (00:00) | 포토미션 생성 Consumer 4개 생성, 다른 큐 Consumer 감소 |
| SIGTERM | Consumer 취소, 처리 중 작업 완료 후 종료 |
| 큐 선언 시도 | 권한 오류 (AI 계정은 선언 불가) |

---

## 9. 결정이 필요한 값

| 항목 | 위치 | 상태 |
| --- | --- | --- |
| `CONFIRM_TIMEOUT` | `MQ_CONFIRM_TIMEOUT_SECONDS` | 5초 임시, 백엔드 측정값과 함께 확정 |
