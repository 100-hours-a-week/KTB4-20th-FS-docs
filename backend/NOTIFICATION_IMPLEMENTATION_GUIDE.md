# PlanIt 알림 기능 구현 가이드

## 1. 문서 목적과 상태

이 문서는 D-1 일반 알림과 이상 기후 알림을 다중 인스턴스 환경에서 생성·전달·복구하기 위한 구현 순서와 검증 기준을 정리한다.

- HTTP·WebSocket 계약은 [`NOTIFICATION_API_SPEC.md`](../docs/NOTIFICATION_API_SPEC.md)를 기준으로 한다.
- 비즈니스 규칙은 [`FIGMA_FINAL_BUSINESS_RULES.md`](../FIGMA_FINAL_BUSINESS_RULES.md)의 `BR-NOTI`를 기준으로 한다.
- 이 문서의 내용은 구현 예정 계약이며 코드·마이그레이션·통합 테스트가 확인되기 전까지 `IMPLEMENTED` 또는 `VERIFIED`가 아니다.

### 1.1 이슈별 구현 현황

- 상위 이슈: [#181 여행 D-1 알림 기능 구현](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/181)
- 상태 기준
  - `NOT_STARTED`: 코드 구현을 시작하지 않음
  - `PARTIAL`: 이슈 범위 중 일부만 구현됨
  - `IMPLEMENTED`: 코드 구현은 완료됐지만 이슈 완료 조건 전체가 검증되지 않음
  - `VERIFIED`: 관련 코드와 명시된 테스트가 현재 체크아웃에서 통과함
- 다음 세션은 아래 표의 `다음 시작점`을 확인하고 해당 이슈 범위만 구현한다.

| 순서 | 이슈 | 상태 | 담당 문서 범위 | 현재 구현과 다음 시작점 |
| ---: | --- | --- | --- | --- |
| 1 | [#182 알림 도메인 모델 및 DB 스키마](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/182) | `IMPLEMENTED` | 2절, 3절 | Entity·Repository·V30·평문 `dedupe_key`·UNIQUE Upsert·동시성 테스트 구현. 기존 `read_at`, `delivered_at`, `deleted_at`이 값이 있는 상태에서도 Upsert 후 보존되는지 통합 테스트를 보강하면 `VERIFIED`로 전환 가능 |
| 2 | [#184 AI 외부 요인 Precheck 연동 및 Redis 상태 관리](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/184) | `IMPLEMENTED` | 4절, 5.1절 | AI Client·응답 DTO, 키가 없을 때만 AI를 호출하는 오케스트레이션, Redis JSON 최초 생성·조회와 순차 조건부 상태 전이를 구현. Store는 JSON이 통합된 Redis 8에서, 연결 흐름은 단위 테스트로 검증 |
| 3 | [#185 ShedLock 기반 알림 생성·장애 복구 스케줄러](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/185) | `IMPLEMENTED` | 5절, 6절 | Redis 기반 ShedLock과 동일 워크플로를 호출하는 10:00·10:30 스케줄, 서울 기준 D+1 계산 및 Precheck 재시도를 구현. 상태별 후속 동작 자체는 #186·#183의 책임이며, 두 이슈가 연결되면 같은 스케줄이 `PRECHECKED`·`NOTI_GENERATED`에서 재개 |
| 4 | [#186 여행 D-1 및 이상 기후 알림 생성](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/186) | `VERIFIED` | 3절, 6절 | `PRECHECKED` 결과로 D-1 여행·활성 참여자를 조회하고, 여행 기간·지역의 이상 기후 여부에 따라 사용자별 `TRIP_D1` 또는 `TRIP_WEATHER_ALERT` 하나를 UNIQUE Upsert로 저장한 뒤 `NOTI_GENERATED`로 전이. 반복 실행 멱등성 통합 테스트까지 통과 |
| 5 | [#183 Redis Pub/Sub 기반 다중 인스턴스 알림 전달](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/183) | `IMPLEMENTED` | 6절, 7.1절 | 당일 미전달·미삭제 알림 조회, 내부 이벤트 변환, `planit:notifications` Publisher·Subscriber, `NOTI_GENERATED` 재개와 전체 Publish 성공 후 `DONE` 전이를 구현. 실제 Redis 다중 인스턴스 전달 검증은 #193에서 수행 |
| 6 | [#188 사용자별 WebSocket 실시간 알림](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/188) | `VERIFIED` | 7.2절 | 내부 Spring 이벤트를 사용자 전용 응답 DTO로 변환해 `/user/queue/notifications`로 라우팅. 인증 STOMP 연결·구독 `RECEIPT`·실제 수신과 서울 오프셋 보존을 통합 테스트로 검증 |
| 7 | [#187 알림 조회·읽음 처리 API](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/187) | `IMPLEMENTED` | API 명세 2~5절 | Cursor 목록·미읽음 수 조회, 개별 읽음, 전체 읽음 API 구현 완료. `read_at` 동시 요청 순서는 보장하지 않고 최종 읽음 상태만 보장한다. Repository 쿼리·영속화는 MySQL 통합 테스트, Service·Controller는 단위 테스트 완료 |
| 8 | [#189 알림 ACK 및 미전달 알림 복구 API](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/189) | `PARTIAL` | 7.3절, 8절 | 소유권·미삭제 조건의 멱등 `delivered_at` DB 처리는 구현·검증됨. pending 조회, Service·Controller·응답/오류 코드와 경합 테스트는 미구현 |
| 9 | [#192 알림 소프트 삭제 및 운영 모니터링](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/192) | `NOT_STARTED` | API 명세 9절, 가이드 11절 | 90일 소프트 삭제와 로그·메트릭 구현 |
| 10 | [#193 알림 생성·전송·복구 통합 테스트](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/193) | `PARTIAL` | 10절 전체 | #182 Upsert 동시성, #186 반복 생성 멱등성, #189 ACK 영속성은 검증됨. 전체 Precheck→생성→전송→복구 시나리오는 미구현 |

프론트엔드는 [#190 전역 알림 연결 및 상태 관리](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/190), [#191 알림 목록·배지·이상 기후 팝업 UI](https://github.com/100-hours-a-week/KTB4-20th-BE/issues/191)에서 별도로 진행한다.

### 1.2 다음 세션 인계

- 시작 이슈는 `#189`이다. `#183`의 Pub/Sub과 `#188`의 사용자별 WebSocket 전달은 변경하지 않는다.
- `#188`은 내부 `NotificationPublishEvent`를 수신 사용자 전용 DTO로 변환해 `/user/queue/notifications`로 전송한다.
- 알림 구독은 인증된 STOMP 연결에서 `receipt` 헤더를 받아 공통 `StompSubscriptionReceiptEvent`로 `RECEIPT` 프레임을 반환한다.
- `#187`의 목록·미읽음 수·개별 읽음·전체 읽음 API는 구현됐다. 다음 구현 단위는 `#189`의 `GET /api/notifications/pending-deliveries`다.

## 2. 책임 분리

| 구성요소 | 책임 |
| --- | --- |
| ShedLock | Redis Lock Key를 사용해 동일한 스케줄 작업의 동시 실행 방지 |
| Redis JSON | AI precheck 결과와 작업 진행 상태를 20시간 보관 |
| `notifications` | 알림 원본, 읽음·전달·소프트 삭제 상태 보관 |
| UNIQUE `dedupe_key` | 스케줄 재실행 시 중복 알림 행 생성 방지 |
| Redis Pub/Sub | 알림 이벤트를 모든 애플리케이션 인스턴스로 실시간 전파 |
| WebSocket | 이벤트 수신 인스턴스에 연결된 사용자에게 실시간 전달 |
| ACK API | 클라이언트 화면 반영 후 `delivered_at` 멱등 기록 |
| pending-deliveries API | 미접속·재연결 사용자의 당일 미전달 알림 복구 |

- Redis는 JSON 자료형이 통합된 Redis Open Source 8.x를 사용한다.
- 별도 RedisJSON 모듈 설치나 Redis Stack 이미지를 사용하지 않는다.
- ShedLock도 동일한 Redis의 `job-lock:planit:*` 키를 사용하며 별도 RDB 테이블을 생성하지 않는다.

## 3. DB 변경 요구사항

`notifications`에 다음 변경이 필요하다.

```sql
ALTER TABLE notifications
    ADD COLUMN delivered_at DATETIME(6) NULL,
    ADD CONSTRAINT uk_notifications_dedupe UNIQUE (dedupe_key);
```

- 실제 마이그레이션은 신규 Flyway 버전으로 추가하고 기존 마이그레이션을 수정하지 않는다.
- `dedupe_key`는 `{notificationType}:{tripId}:{userPublicId}:{startDate}` 순서로 생성한다.
- Upsert는 기존 행의 `read_at`, `delivered_at`, `created_at`, `deleted_at`을 초기화하지 않는다.
- 다른 사용자의 알림을 본인 알림처럼 처리하지 못하도록 모든 수정 쿼리에 `user_id`를 포함한다.

## 4. Redis JSON 작업 상태

### 4.1 키와 TTL

```text
planit:trip-remind:precheck:{targetDate}
```

- `targetDate`는 `yyyy-MM-dd`형식의 여행 시작 대상일이다.
- TTL은 20시간(`72,000초`)이다.
- 최초 JSON 생성은 `JSON.SET key $ value NX`로 기존 작업을 덮어쓰지 않는다.
- JSON 생성과 `EXPIRE`는 Lua Script로 원자적으로 실행한다.
- 상태 경로는 `JSON.SET key $.status value XX`로 변경하되, 상태 필드가 없으면 작업을 성공으로 간주하지 않는다.

### 4.2 최초 생성 Lua Script 계약

Lua Script는 `JSON.SET key $ value NX`와 `EXPIRE`를 하나의 원자적 작업으로 수행한다.

```lua
local result = redis.call(
    'JSON.SET',
    KEYS[1],
    '$',
    ARGV[1],
    'NX'
)

if not result then
    return 'ALREADY_EXISTS'
end

redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))

return 'CREATED'
```

| 반환 결과 | 의미 | 처리 규칙 |
| --- | --- | --- |
| `CREATED` | JSON 생성과 TTL 설정 완료 | `PRECHECKED` 상태의 후속 알림 생성 단계로 진행 |
| `ALREADY_EXISTS` | 동일 키가 이미 존재해 `NX` 조건 불충족 | 정상적인 중복 실행으로 간주하고 기존 JSON의 `status`에 따라 재개 |
| Redis 오류 | 연결 실패, 타임아웃, 잘못된 JSON 또는 Script 실행 오류 | 작업 실패로 처리하고 다음 단계로 진행하지 않음 |

- `ALREADY_EXISTS`는 예외나 오류 로그 대상이 아니다.
- Redis 오류는 애플리케이션 예외로 전파하고 로그와 메트릭을 남긴다.
- `ALREADY_EXISTS`일 때 기존 데이터의 TTL을 20시간으로 다시 연장하지 않는다.
- 기존 키의 TTL이 없거나 비정상적인 값이면 운영 로그와 메트릭을 남기되, 기존 JSON을 덮어쓰지 않는다.

### 4.3 상태 정의

| 상태 | 완료 조건 | 다음 실행 |
| --- | --- | --- |
| 키 없음 | AI `/precheck` 미실행 또는 실패 | AI `/precheck` 1회 호출 |
| `PRECHECKED` | AI 응답과 결과 JSON 저장 완료 | 알림 대상 산출과 DB 저장 |
| `NOTI_GENERATED` | 모든 대상 알림 DB 커밋 완료 | 미전달 알림 Redis Pub/Sub 발행 |
| `DONE` | 현재 작업의 Pub/Sub 발행 시도 완료 | 작업 종료 |

`DONE`은 사용자 수신 완료가 아니라 실시간 발행 시도 완료다. 개별 사용자 수신 여부는 `delivered_at`으로 판단한다.

## 5. ShedLock과 장애 복구

- 첫 실행은 매일 `10:00`, 장애 복구용 두 번째 실행은 매일 `10:30`에 `Asia/Seoul` 기준으로 예약한다.
- 두 실행은 동일한 상태 머신 메서드와 ShedLock 이름 `tripRemindNotificationWorkflow`를 사용한다.
- 초기 값은 `lockAtMostFor=PT5M`, `lockAtLeastFor=PT1M`으로 설정한다.
- AI `/precheck`는 스케줄 실행당 1회만 호출하고 `@Retryable`을 사용하지 않는다.
- AI 호출이 실패하면 Redis JSON을 생성하지 않고 예외·로그·메트릭을 남긴다.
- 두 번째 실행도 실패하면 최종 장애로 기록하고 운영 감지 대상으로 삼는다.
- 실행 시간이 `lockAtMostFor`를 초과하지 않는지 메트릭으로 검증한 후 필요하면 값을 늘린다.

```java
@Scheduled(cron = "0 0 10 * * *", zone = "Asia/Seoul")
@Scheduled(cron = "0 30 10 * * *", zone = "Asia/Seoul")
@SchedulerLock(
        name = "tripRemindNotificationWorkflow",
        lockAtMostFor = "PT5M",
        lockAtLeastFor = "PT1M"
)
public void executeTripRemindNotificationWorkflow() {
    LockAssert.assertLocked();
    // Redis 상태에 맞는 단계 실행
}
```

### 5.1 AI `/precheck` 계약

- 요청은 `GET ${AI_BASE_URL}/precheck`다.
- request body와 query parameter는 사용하지 않는다.
- `X-Internal-Token: ${AI_INTERNAL_API_KEY}` 헤더만 추가한다.
- 스케줄 실행당 한 번만 요청하며 `@Retryable`은 사용하지 않는다.
- HTTP 200이 아니거나, body가 없거나, 아래 응답 구조로 역직렬화할 수 없으면 해당 실행을 실패 처리하고 Redis JSON을 생성하지 않는다.
- `generated_at`은 UTC ISO-8601 시각이다.
- `forecasts`에는 `generated_at`의 서울 날짜를 기준으로 다음 날(`D+1`)부터 9일 후(`D+9`)까지 날짜순으로 정확히 9개 항목이 포함된다.
- `region`은 `REGION-SEOUL`, `REGION-GYEONGJU`, `REGION-BUSAN`, `REGION-JEONJU`, `REGION-JEJU`와 같은 백엔드 기준 지역 코드를 사용한다.
- 날씨 제약은 날짜·지역 단위로 표현하며, 일정 재생성은 이 응답을 사용하는 별도 AI 흐름의 책임이다.

AI 응답 예시:

```json
{
  "status_code": 200,
  "data": {
    "generated_at": "2026-10-10T01:00:03Z",
    "forecasts": [
      {
        "date": "2026-10-11",
        "regions": [
          {
            "region": "REGION-SEOUL",
            "has_issue": true,
            "reasons": [
              {
                "category": "weather_risk",
                "detail": "heavy_rain"
              }
            ]
          },
          {
            "region": "REGION-BUSAN",
            "has_issue": false,
            "reasons": []
          }
        ]
      }
    ]
  }
}
```

위 예시는 첫 날짜만 축약해 표시하며 실제 응답에는 `D+1`부터 `D+9`까지 9개 날짜 항목이 있어야 한다.

Redis JSON 저장 예시:

```json
{
  "generated_at": "2026-10-10T01:00:03Z",
  "status": "PRECHECKED",
  "forecasts": []
}
```

- `status`는 AI 응답 필드가 아니다. 백엔드가 AI 응답을 Redis JSON으로 변환해 최초 저장할 때 `PRECHECKED`를 추가한다.
- 지역의 `has_issue=false`와 빈 `reasons` 배열도 정상 결과이며 그대로 Redis에 저장한다.
- `has_issue=true`이면 날짜와 지역 코드를 여행 일정과 비교해 영향받는 여행과 수신자를 결정한다.
- `status_code`는 응답 body 필드이고 실제 HTTP status도 별도로 200인지 검증한다.

## 6. 스케줄 상태 머신

```text
작업 시작
  ├─ Redis 키 없음 → AI precheck → PRECHECKED
  ├─ PRECHECKED → 알림 DB Upsert → NOTI_GENERATED
  ├─ NOTI_GENERATED → Redis Pub/Sub Publish → DONE
  └─ DONE → 종료
```

각 상태 전이는 선행 단계가 완료된 후에만 수행한다.

1. AI 응답을 받은 후 Redis JSON 생성이 완료되면 `PRECHECKED`다.
2. 알림 대상자 전체의 DB 트랜잭션이 커밋된 후 `NOTI_GENERATED`다.
3. 현재 작업의 알림을 모두 Publish한 후 `DONE`이다.
4. 알림 일부 저장 후 장애가 발생하면 다음 실행에서 전체 대상을 다시 산출하고 UNIQUE Upsert로 누락만 복구한다.
5. Publish 일부 완료 후 장애가 발생하면 `NOTI_GENERATED`가 유지되고 다음 실행에서 전체를 다시 Publish한다.

## 7. 실시간 전달과 ACK

### 7.1 Redis Pub/Sub

- Redis Pub/Sub 채널명은 소문자 `planit:notifications`로 고정한다.
- Redis 채널명은 대소문자를 구분하므로 publisher와 모든 subscriber가 정확히 `planit:notifications`를 사용한다.
- 내부 이벤트 DTO와 WebSocket 응답 DTO는 필드가 같더라도 별도 타입으로 둔다.

```json
{
  "recipientPublicId": "user-public-id",
  "notificationId": "16001",
  "notificationType": "TRIP_WEATHER_ALERT",
  "title": "여행지에 많은 비가 예상돼요",
  "body": "여행 기간에 폭우가 예상됩니다.",
  "createdAt": "2026-10-13T10:00:00.123456+09:00"
}
```

### 7.2 WebSocket

- 구독 destination은 `/user/queue/notifications`다.
- 인증된 클라이언트가 `receipt` 헤더와 함께 구독하면 구독 등록 후 같은 값을 `receipt-id`로 가진 STOMP `RECEIPT` 프레임을 반환한다.
- 각 인스턴스는 Redis 이벤트를 받은 후 자신이 보유한 연결 중 수신 사용자가 있는 경우만 전송한다.
- WebSocket payload에서는 서버 내부 라우팅용 `recipientPublicId`를 제외한다.
- `createdAt`은 직렬화 과정에서 서울 오프셋을 유지한다.
- 서버의 WebSocket 전송 메서드 성공은 클라이언트 수신 증거가 아니다.

### 7.3 클라이언트 ACK

1. `notificationId`로 이미 화면에 반영했는지 확인한다.
2. 미처리 알림은 화면과 상태 Store에 반영한다.
3. `notification:{userPublicId}:{notificationId}`를 `expiredAt`과 함께 localStorage에 저장한다.
4. ACK API를 호출한다.
5. localStorage에 이미 있던 알림도 이전 ACK 실패를 복구하기 위해 ACK API는 다시 호출한다.
6. `expiredAt`은 해당 알림을 수신한 시각으로부터 20시간 후로 설정한다.
7. 서비스 진입 시 현재 시각이 `expiredAt` 이상인 알림 키를 일괄 삭제한 후 WebSocket 연결과 미전달 알림 복구를 진행한다.

## 8. 미접속·재연결 복구

1. 서비스 진입 시 WebSocket을 연결한다.
2. `/user/queue/notifications`의 구독 완료를 확인한다.
3. `GET /api/notifications/pending-deliveries`를 호출한다.
4. 조회된 알림을 화면에 반영한 후 개별 ACK를 호출한다.
5. 실시간 이벤트와 HTTP 복구 응답이 겹치면 `notificationId`로 병합한다.

## 9. 이슈 기준 구현 순서

1. `#182` Flyway·Notification Entity·Repository·UNIQUE Upsert
2. `#184` AI `/precheck`·Redis JSON·상태 전이·TTL Lua Script
3. `#185` Redis ShedLock과 10:00·10:30 스케줄 트리거·Precheck 재시도 (`IMPLEMENTED`)
4. `#186` D-1·이상 기후 대상 산출·알림 생성·`NOTI_GENERATED` 전이 (`VERIFIED`)
5. `#183` Redis Pub/Sub publisher·subscriber·`DONE` 전이 (`IMPLEMENTED`)
6. `#188` 사용자 WebSocket destination·인증·라우팅 (`VERIFIED`)
7. `#187` 알림 목록 Cursor 조회 (`IMPLEMENTED`)
8. `#187` 미읽음 알림 수 조회 (`IMPLEMENTED`)
9. `#187` 개별 읽음 처리 (`IMPLEMENTED`)
10. `#187` 전체 읽음 처리 (`IMPLEMENTED`)
11. **다음 시작점:** `#189` pending-deliveries 조회
12. `#189` delivery-ack HTTP API
13. `#192` 90일 소프트 삭제·운영 로그·메트릭
14. `#193` 전체 생성·전송·복구 통합 테스트
15. `#190`, `#191` 프론트엔드 전역 연결·상태 관리·UI

## 10. 검증 체크리스트

### DB와 HTTP

- [x] 동일 `dedupe_key`로 동시 생성해도 알림 행이 하나만 남는다. (`#182` 통합 테스트 완료)
- [x] 알림 목록은 현재 사용자의 미삭제 알림을 `(created_at, id)` Cursor로 조회하고 최초 20개·이후 10개를 반환한다. (`#187` Repository MySQL 통합·Service·Controller 단위 테스트 완료)
- [x] 미읽음 수는 현재 사용자의 `read_at IS NULL`, `deleted_at IS NULL` 알림만 계산한다. (`#187` Repository MySQL 통합·Service·Controller 단위 테스트 완료)
- [x] 개별 읽음은 본인 미삭제 알림만 처리하고 순차 재요청에서 기존 `read_at`을 유지한다. 동시 요청 간 최초 시각 순서는 보장하지 않는다. (`#187` Repository MySQL 통합·Service·Controller 단위 테스트 완료)
- [x] 전체 읽음은 현재 사용자의 미삭제·미읽음 알림만 같은 시각으로 일괄 갱신하고 반복 요청 시 `updatedCount=0`을 반환한다. (`#187` Repository MySQL 통합·Service·Controller 단위 테스트 완료)
- [ ] Upsert 재실행이 기존 `read_at`, `delivered_at`, `created_at`을 변경하지 않는다.
- [x] ACK DB 처리는 최초와 반복 요청 모두 최초 `delivered_at`을 유지한다. (`#189` 영속성 테스트 완료, HTTP API 미구현)
- [ ] 다른 사용자의 ACK는 `404 NOTIFICATION_NOT_FOUND`다.
- [ ] pending-deliveries는 서울 기준 당일·미전달·미삭제·대상 알림 유형만 반환한다.

### 스케줄러와 Redis

- [ ] 두 인스턴스가 동시 실행해도 하나만 ShedLock을 획득한다.
- [x] 서울 시각 `10:00` 첫 실행과 `10:30` 복구 실행이 같은 상태 머신과 락 이름을 사용한다. (`#185` 단위 테스트 완료)
- [ ] AI 성공 후 Redis 저장 전 장애는 두 번째 실행에서 AI를 다시 호출한다.
- [ ] `PRECHECKED` 장애는 알림 DB 생성부터 재개한다.
- [ ] `NOTI_GENERATED` 장애는 Publish부터 재개한다.
- [ ] `DONE`은 두 번째 실행에서 즉시 종료한다.
- [ ] `lockAtMostFor` 초과 실행을 메트릭으로 감지한다.
- [ ] 최초 Redis JSON 생성은 `CREATED`, 기존 키는 `ALREADY_EXISTS`를 반환한다.
- [ ] Redis 오류 발생 시 후속 단계로 진행하지 않고 작업을 실패 처리한다.

### 실시간 전달과 복구

- [x] 인증 사용자가 알림 destination을 구독 완료하고 본인 WebSocket 알림을 수신한다. (`#188` 통합 테스트 완료)
- [ ] 접속 사용자는 WebSocket으로 알림을 받고 ACK한다.
- [ ] 미접속 사용자는 접속 후 pending-deliveries로 복구한다.
- [ ] Pub/Sub 중복 이벤트는 화면에 한 번만 노출하고 ACK는 다시 보낸다.
- [ ] WebSocket 구독과 pending-deliveries 조회 중 겹친 알림을 `notificationId`로 병합한다.
- [ ] Redis Pub/Sub에 구독 인스턴스가 없어도 DB 알림은 pending-deliveries로 복구된다.
- [ ] publisher와 모든 subscriber가 소문자 `planit:notifications` 채널을 사용한다.
- [ ] localStorage 항목은 수신 후 20시간에 만료되고 서비스 진입 시 일괄 정리된다.

## 11. 아직 확인해야 할 값

- 운영 알림·메트릭·Sentry 연결 기준
