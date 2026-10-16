# PlanIt 알림 API 명세

## 1. 공통 정책

- HTTP 성공·오류 응답과 도메인 오류 구현 책임은 [`API_SPEC_GUIDELINES.md`](./API_SPEC_GUIDELINES.md)의 공통 규약을 따른다.
- 이 문서의 사용자용 HTTP API 6개는 `APPROVED`다.
- 인앱 알림만 범위에 포함하며 푸시 알림은 제공하지 않는다.
- 현재 로그인 사용자에게 생성된 알림만 조회·변경할 수 있다.
- 알림은 생성 후 90일이 되면 `deleted_at`을 기록해 소프트 삭제하고 사용자 API에서 제외한다.
- 이동 대상이 만료돼도 90일 전까지 알림과 읽음 상태를 유지한다.
- 만료된 알림도 읽기 전까지 미읽음 수에 포함한다. 선택하면 읽음 처리하지만 대상 화면으로 이동하지 않는다.
- 상대 시간 문구는 API가 제공하지 않는다. 클라이언트가 `createdAt`을 서울 시각 기준으로 변환해 표시한다.
- 동일 여행·사용자·날짜·알림 유형의 중복 생성은 서버 내부 `dedupe_key`로 방지한다.
- `dedupe_key`는 `tripId`, `userPublicId`, `trip.startDate`, `notificationType`을 고정된 순서로 조합하고 UNIQUE 제약과 Upsert로 중복 알림 행 생성을 방지한다.
- `delivered_at`은 서버가 실시간 이벤트를 발행한 시각이 아니라, 클라이언트가 알림을 화면에 반영한 후 ACK를 보내 서버가 수신한 시각이다.
- Redis Pub/Sub과 WebSocket은 실시간 전달을 위한 at-least-once 시도이며, 알림 DB를 최종 원본으로 사용한다.

## 2. 알림 목록

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `GET` |
| URL | `/api/notifications` |
| 인증·인가 | Access Token 본인 |
| 페이지네이션 | 최초 20개, 이후 cursor로 10개씩 |

```http
GET /api/notifications?cursor={opaqueCursor}
Authorization: Bearer {accessToken}
```

```json
{
  "code": "NOTIFICATIONS_RETRIEVED",
  "message": "알림 목록을 조회했습니다.",
  "data": {
    "items": [
      {
        "notificationId": "16001",
        "type": "TRIP_WEATHER_ALERT",
        "title": "여행지에 많은 비가 예상돼요",
        "body": "방장이 일정 변경 여부를 확인하고 있어요.",
        "target": {
          "type": "TRIP",
          "id": "1001",
          "status": "ACTIVE"
        },
        "read": false,
        "readAt": null,
        "createdAt": "2026-09-07T10:00:00.123456+09:00"
      }
    ],
    "page": {
      "nextCursor": "opaque-next-cursor",
      "hasNext": true
    }
  }
}
```

| HTTP 상태 | API 코드 | 조건 |
| ---: | --- | --- |
| `200 OK` | `NOTIFICATIONS_RETRIEVED` | 목록 조회 성공, 빈 목록 포함 |
| `400 Bad Request` | `INVALID_CURSOR` | cursor 형식이나 사용자 문맥이 유효하지 않음 |
| `401 Unauthorized` | `AUTHENTICATION_REQUIRED` | Access Token이 없거나 유효하지 않음 |
| `500 Internal Server Error` | `INTERNAL_SERVER_ERROR` | 예상하지 못한 서버 내부 오류 |

### 처리 규칙

- `deleted_at IS NULL`인 현재 사용자 알림만 `created_at`, `id` 내림차순으로 조회한다.
- 첫 요청은 cursor를 생략하고 20개를 받는다. 다음 요청부터는 직전 `nextCursor`를 사용하고 10개씩 받는다.
- 이동 대상이 만료되면 `target.status=EXPIRED`이며 `target.id`는 기존 값을 유지한다.
- 이동 대상이 없는 안내 알림은 `target=null`이다.
- 빈 목록은 `items=[]`, `nextCursor=null`, `hasNext=false`로 반환한다.

## 3. 미읽음 알림 수

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `GET` |
| URL | `/api/notifications/unread-count` |
| 인증·인가 | Access Token 본인 |

```json
{
  "code": "UNREAD_NOTIFICATION_COUNT_RETRIEVED",
  "message": "읽지 않은 알림 수를 조회했습니다.",
  "data": {
    "unreadCount": 3,
    "hasUnread": true
  }
}
```

- `deleted_at IS NULL`, `read_at IS NULL`인 현재 사용자 알림을 계산한다.
- 이동 대상이 만료된 알림도 읽지 않았다면 포함한다.
- 인증 실패는 `401 AUTHENTICATION_REQUIRED`, 서버 오류는 `500 INTERNAL_SERVER_ERROR`다.

## 4. 개별 알림 읽음

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `PUT` |
| URL | `/api/notifications/{notificationId}/read` |
| 인증·인가 | Access Token 본인 |
| 멱등성 | 이미 읽은 알림은 기존 읽음 시각을 반환하는 자연 멱등 PUT |

request body는 사용하지 않는다.

```json
{
  "code": "NOTIFICATION_READ",
  "message": "알림을 읽음 처리했습니다.",
  "data": {
    "notificationId": "16001",
    "read": true,
    "readAt": "2026-09-07T11:00:00.123456+09:00",
    "targetStatus": "EXPIRED",
    "navigable": false
  }
}
```

| HTTP 상태 | API 코드 | 조건 |
| ---: | --- | --- |
| `200 OK` | `NOTIFICATION_READ` | 최초 읽음 또는 기존 읽음 상태 반환 |
| `401 Unauthorized` | `AUTHENTICATION_REQUIRED` | Access Token이 없거나 유효하지 않음 |
| `404 Not Found` | `NOTIFICATION_NOT_FOUND` | 본인 알림이 아니거나 이미 소프트 삭제됨 |
| `500 Internal Server Error` | `INTERNAL_SERVER_ERROR` | 예상하지 못한 서버 내부 오류 |

### 처리 규칙

- 최초 요청에서만 현재 서울 시각으로 `read_at`을 기록한다. 반복 요청은 기존 시각을 변경하지 않는다.
- `targetStatus=EXPIRED`이면 `navigable=false`를 반환하며 클라이언트는 “더 이상 확인할 수 없는 항목입니다”를 표시한다.
- 활성 대상이면 `navigable=true`이고 목록 응답의 `target.type`, `target.id`를 사용해 이동한다.

## 5. 알림 모두 읽음

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `PUT` |
| URL | `/api/notifications/read-all` |
| 인증·인가 | Access Token 본인 |
| 멱등성 | 이미 모두 읽은 상태에서도 결과가 같은 자연 멱등 PUT |

request body는 사용하지 않는다.

```json
{
  "code": "ALL_NOTIFICATIONS_READ",
  "message": "모든 알림을 읽음 처리했습니다.",
  "data": {
    "updatedCount": 3,
    "readAt": "2026-09-07T11:05:00.123456+09:00",
    "unreadCount": 0
  }
}
```

- `deleted_at IS NULL`, `read_at IS NULL`인 현재 사용자 알림에 같은 읽음 시각을 기록한다.
- 이미 모두 읽었으면 `updatedCount=0`, `unreadCount=0`을 반환한다.
- 대상 만료 여부와 관계없이 모든 미읽음 알림을 처리한다.
- 인증 실패는 `401 AUTHENTICATION_REQUIRED`, 서버 오류는 `500 INTERNAL_SERVER_ERROR`다.

## 6. 당일 미전달 알림 조회

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `GET` |
| URL | `/api/notifications/pending-deliveries` |
| 인증·인가 | Access Token 본인 |
| 용도 | WebSocket 구독 완료 후 실시간으로 받지 못한 당일 알림 복구 |

request body는 사용하지 않는다.

```json
{
  "code": "PENDING_NOTIFICATION_DELIVERIES_RETRIEVED",
  "message": "미전달 알림을 조회했습니다.",
  "data": {
    "items": [
      {
        "notificationId": "16001",
        "type": "TRIP_WEATHER_ALERT",
        "title": "여행지에 많은 비가 예상돼요",
        "body": "여행 기간에 폭우가 예상됩니다.",
        "createdAt": "2026-10-13T10:00:00.123456+09:00"
      }
    ]
  }
}
```

| HTTP 상태 | API 코드 | 조건 |
| ---: | --- | --- |
| `200 OK` | `PENDING_NOTIFICATION_DELIVERIES_RETRIEVED` | 조회 성공, 빈 목록 포함 |
| `401 Unauthorized` | `AUTHENTICATION_REQUIRED` | Access Token이 없거나 유효하지 않음 |
| `500 Internal Server Error` | `INTERNAL_SERVER_ERROR` | 예상하지 못한 서버 내부 오류 |

### 처리 규칙

- `Asia/Seoul` 기준 당일에 생성된 현재 사용자의 알림 중 `delivered_at IS NULL`, `deleted_at IS NULL`인 행을 조회한다.
- `notification_type` 조건은 `TRIP_D1`, `TRIP_WEATHER_ALERT`다.
- 이 API는 조회만 수행하며 `delivered_at`을 변경하거나 WebSocket 이벤트를 다시 발행하지 않는다.
- 클라이언트는 알림을 화면에 반영한 후 개별 ACK API를 호출한다.

## 7. 알림 수신 ACK

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `PUT` |
| URL | `/api/notifications/{notificationId}/delivery-ack` |
| 인증·인가 | Access Token 본인 |
| 멱등성 | 이미 ACK된 알림도 기존 전달 시각을 변경하지 않고 성공 반환 |

request body는 사용하지 않는다.

```json
{
  "code": "NOTIFICATION_DELIVERY_ACKNOWLEDGED",
  "message": "알림 수신을 확인했습니다.",
  "data": {
    "notificationId": "16001",
    "deliveredAt": "2026-10-13T10:31:25.123456+09:00"
  }
}
```

| HTTP 상태 | API 코드 | 조건 |
| ---: | --- | --- |
| `200 OK` | `NOTIFICATION_DELIVERY_ACKNOWLEDGED` | 최초 ACK 또는 기존 ACK 상태 반환 |
| `401 Unauthorized` | `AUTHENTICATION_REQUIRED` | Access Token이 없거나 유효하지 않음 |
| `404 Not Found` | `NOTIFICATION_NOT_FOUND` | 알림이 없거나 현재 사용자의 알림이 아니거나 이미 소프트 삭제됨 |
| `500 Internal Server Error` | `INTERNAL_SERVER_ERROR` | 예상하지 못한 서버 내부 오류 |

### 처리 규칙

- `notificationId`와 인증 사용자 ID가 모두 일치하는 `deleted_at IS NULL` 알림만 처리한다.
- 최초 ACK에서만 현재 시각을 `delivered_at`에 기록하고, 반복 ACK는 기존 시각을 반환한다.
- 클라이언트가 `notificationId`를 이미 처리했더라도 화면에만 중복 노출하지 않고 ACK는 다시 호출한다.
- 다른 사용자의 알림은 존재 여부를 노출하지 않도록 `404 NOTIFICATION_NOT_FOUND`로 응답한다.

## 8. 실시간 알림 계약과 내부 전달 정책

- 클라이언트는 로그인 후 서비스 진입 시점에 WebSocket을 연결하고 `/user/queue/notifications`를 구독한다.
- Redis Pub/Sub 채널명은 소문자 `planit:notifications`로 고정하며, publisher와 모든 subscriber가 정확히 같은 값을 사용한다.
- Redis 내부 이벤트는 `recipientPublicId`, `notificationId`, `notificationType`, `title`, `body`, `createdAt`을 포함한다.
- 각 인스턴스는 Redis 이벤트를 구독하고, 자신이 보유한 WebSocket 연결에 수신 사용자가 있을 때만 사용자 destination으로 이벤트를 전송한다.
- WebSocket 응답은 내부 라우팅용 `recipientPublicId`를 제외하고 `notificationId`, `notificationType`, `title`, `body`, `createdAt`을 반환한다.
- 사용자 단위 연결 중 하나의 클라이언트 ACK를 전달 완료로 간주한다.
- 클라이언트는 `notification:{userPublicId}:{notificationId}` 키를 수신 시각으로부터 20시간 후인 `expiredAt`과 함께 localStorage에 저장해 중복 화면 노출을 방지한다.
- localStorage에 이미 있는 알림은 화면에 다시 표시하지 않지만, 이전 ACK 실패를 복구하기 위해 ACK API는 다시 호출한다.
- 클라이언트는 서비스 진입 시 `expiredAt`이 지난 알림 키를 일괄 삭제한다.

### WebSocket payload

```json
{
  "notificationId": "16001",
  "notificationType": "TRIP_WEATHER_ALERT",
  "title": "여행지에 많은 비가 예상돼요",
  "body": "여행 기간에 폭우가 예상됩니다.",
  "createdAt": "2026-10-13T10:00:00.123456+09:00"
}
```

### 내부 스케줄링 정책

- 상태 저장소는 JSON 자료형이 통합된 Redis Open Source 8.x를 사용하며 별도 RedisJSON 모듈을 설치하지 않는다.
- Redis JSON 키는 `planit:trip-remind:precheck:{targetDate}`를 사용하고 TTL은 20시간이다.
- 최초 저장은 `JSON.SET ... NX`와 `EXPIRE` 조합을 Lua Script로 원자적으로 수행한다. 생성 성공은 `CREATED`, 기존 키로 인한 미생성은 `ALREADY_EXISTS`를 반환하며, Redis 오류는 작업 실패로 처리하고 후속 단계로 진행하지 않는다.
- 상태는 `PRECHECKED`(외부 요인 조회 완료), `NOTI_GENERATED`(알림 DB 커밋 완료), `DONE`(Redis Pub/Sub 발행 시도 완료)을 사용한다.
- 외부 AI `/precheck`는 스케줄 실행당 1회만 호출하고 `@Retryable`을 사용하지 않는다. 장애 복구용 두 번째 스케줄이 Redis 상태를 보고 미완료 단계를 재실행한다.
- 첫 스케줄은 매일 `10:00`, 장애 복구 스케줄은 매일 `10:30`에 `Asia/Seoul` 기준으로 실행하고 동일한 ShedLock 이름을 사용한다. 초기 설정은 `lockAtMostFor=PT5M`, `lockAtLeastFor=PT1M`이다.
- ShedLock은 동일한 Redis의 `job-lock:planit:*` 키를 사용하며 RDB Lock 테이블은 사용하지 않는다.
- AI 요청은 body와 query parameter 없이 `GET ${AI_BASE_URL}/precheck`와 `X-Internal-Token: ${AI_INTERNAL_API_KEY}` 헤더를 사용한다. HTTP 200과 body의 `status_code=200`을 모두 확인한다.
- AI 응답의 `data`는 UTC ISO-8601 `generated_at`과 날짜별 `forecasts`를 포함한다. `forecasts`는 생성 시각의 서울 날짜 기준 `D+1`부터 `D+9`까지 날짜순으로 정확히 9개이며, 각 날짜는 백엔드 기준 지역 코드별 `has_issue`와 `reasons`를 포함한다.
- `status`는 AI 응답값이 아니다. 백엔드는 AI 응답의 `generated_at`과 `forecasts`를 Redis JSON으로 저장하면서 최초 상태 `PRECHECKED`를 추가한다.
- 지역별 `has_issue=false`, `reasons=[]`도 정상 precheck 완료 결과다. 날씨 제약은 날짜·지역 단위로 판정하고, 일정 재생성은 별도 AI 흐름에서 실행한다.
- DB 트랜잭션 커밋 후에만 `NOTI_GENERATED`로, 현재 작업의 미전달 알림 Publish 반복이 완료된 후에만 `DONE`으로 변경한다.
- `DONE`은 사용자 수신 완료가 아니라 Redis Pub/Sub 발행 시도 완료를 의미한다.

## 9. 알림 소프트 삭제

- 사용자용 삭제 API는 제공하지 않는다.
- 내부 정리 작업이 `created_at`으로부터 90일이 지난 알림의 `deleted_at`을 기록한다.
- 소프트 삭제 작업이 재실행돼도 이미 삭제된 행의 `deleted_at`을 변경하지 않는다.
- 소프트 삭제된 알림은 목록, 미읽음 수와 읽음 처리 대상에서 모두 제외한다.
- 물리 삭제 시점은 현재 계약에 포함하지 않는다.
