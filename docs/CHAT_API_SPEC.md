# PlanIt 지역 오픈 채팅 API 명세

## 1. 문서 상태와 책임 분리

- 이 문서의 HTTP API 7개는 `APPROVED`다.
- 실시간 메시지 송수신은 Native WebSocket 기반 Spring WebSocket + STOMP와 Simple Broker로 구현한다. 연결·메시지 destination과 STOMP `CONNECT` Bearer Token 인증을 포함한 실시간 계약은 `APPROVED`다.
- HTTP는 채팅방 목록, 정책 동의, 입퇴장, 과거 메시지와 이미지 업로드 준비를 담당한다.
- 실시간 인터페이스는 연결 이후의 새 텍스트·이미지 메시지와 차단·제재 통지를 담당한다.
- 실시간 연결이 끊긴 동안의 메시지는 HTTP 메시지 이력 API로 복구한다.
- 메시지 전송 요청은 UUID `clientMessageId`를 포함한다. 서버는 발신 사용자와 `clientMessageId` 조합으로 중복 저장을 방지하고, 처리 결과는 발신자 전용 애플리케이션 결과 이벤트로 통지한다.

## 2. 공통 채팅 정책

- 지역 채팅은 여행방 내부 채팅이 아니라 서비스 사용자가 참여하는 지역별 공개 채팅이다.
- 메시지는 앞뒤 공백 제거 후 1~1000자이며 빈 텍스트는 허용하지 않는다.
- 동일 메시지 반복, 과도하게 빠른 전송과 욕설·모욕 등 정책 위반 메시지는 `BLOCKED`로 저장하되 실시간 전송과 일반 이력 조회에서 제외한다.
- 발신자에게만 차단 사유, 현재 위반 횟수와 제재 정보를 전달한다.
- 위반 3회마다 제재를 한 번 적용하고 다음 제재 판단용 위반 횟수는 다시 0부터 계산한다. 위반·제재 이력과 제재 단계는 유지한다.
- 제재 기간은 `1일 → 4일 → 7일 → 14일 → 30일 → 60일`이며 이후에도 60일이다.
- 제재는 사용자 전체의 메시지 전송만 막는다. 제재 중에도 채팅방 입장·구독, 실시간 메시지 수신과 HTTP 메시지 이력 조회는 허용한다.
- 일반 공개 메시지와 위반·제재 근거인 `BLOCKED` 메시지는 별도의 기간 만료 삭제 없이 보관한다.
- 채팅 이미지는 JPEG, PNG, WebP만 허용하며 최대 크기는 5 MB다.
- 사용자 메시지 신고와 특정 사용자 차단 기능은 초기 범위에 포함하지 않는다.
- 애플리케이션 수준의 별도 STOMP frame 크기 제한은 초기 범위에 추가하지 않는다.
- 모든 채팅 시각은 서버와 DB에서 UTC로 저장하고 API·실시간 이벤트에서는 ISO 8601 UTC(`Z`) 문자열로 반환한다. 클라이언트는 기기 또는 브라우저의 시간대에 맞춰 표시만 변환하며 메시지 정렬에는 변환된 표시 시각을 사용하지 않는다.

## 3. 지역 채팅방 목록

### 기본 정보

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `GET` |
| URL | `/api/regional-chat-rooms` |
| 인증 | Access Token 필수 |
| 페이지네이션 | cursor, 최초 20개·추가 요청 10개 고정 |

### 요청

```http
GET /api/regional-chat-rooms?cursor={opaqueCursor}
Authorization: Bearer {accessToken}
```

| 위치 | 필드 | 타입 | 필수 | 제약·기본값 | 설명 |
| --- | --- | --- | --- | --- | --- |
| Query | `cursor` | string | N | 최초 요청 시 생략, 발급 후 30분 동안 유효 | 다음 페이지를 위한 불투명 cursor |

### 성공 응답

```json
{
  "code": "REGIONAL_CHAT_ROOMS_RETRIEVED",
  "message": "지역 채팅방 목록을 조회했습니다.",
  "data": {
    "items": [
      {
        "roomId": "3001",
        "regionId": "123",
        "name": "부산광역시 해운대구",
        "memberCount": 128,
        "activeUserCount": 24,
        "relatedToMyTrip": true,
        "joined": false,
        "canJoin": true
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
| `200 OK` | `REGIONAL_CHAT_ROOMS_RETRIEVED` | 최초 또는 다음 채팅방 목록 조회 성공 |
| `400 Bad Request` | `INVALID_CURSOR` | cursor 형식·서명·사용자 문맥이 유효하지 않거나 발급 후 30분이 지남 |
| `401 Unauthorized` | `AUTHENTICATION_REQUIRED` | Access Token이 없거나 유효하지 않음 |
| `500 Internal Server Error` | `INTERNAL_SERVER_ERROR` | 예상하지 못한 서버 내부 오류 |

### 처리 규칙

- `sub_regions`에 등록된 모든 하위 지역마다 지역 공개 채팅방 하나를 운영 데이터 또는 마이그레이션으로 미리 생성한다. 등록된 하위 지역은 모두 활성 지역으로 간주하며 사용자용 채팅방 생성·수정·삭제 API는 제공하지 않는다.
- 최초 요청은 20개, cursor를 포함한 추가 요청은 10개를 반환한다. 클라이언트가 조회 개수를 지정하는 `size` 파라미터는 사용하지 않는다.
- 현재 사용자가 활성 멤버로 참여하고 있고 현재 날짜가 여행 시작일과 종료일 사이인 여행의 지역 채팅방을 먼저 배치한다. `relatedToMyTrip`은 이 조건을 충족하면 `true`다.
- 같은 여행 관련 여부 안에서는 현재 사용자가 가입한 채팅방을 먼저 배치한다. `joined`는 채팅방 가입 이력이 활성 상태이면 `true`다.
- 같은 여행 관련 여부와 가입 상태 안에서는 각 페이지 요청 시점의 `activeUserCount` 내림차순, `memberCount` 내림차순으로 정렬한다.
- 두 사용자 수가 같으면 `광역 지역명 + 하위 지역명`의 가나다순으로 정렬하고, 지역명도 같으면 `roomId` 오름차순으로 순서를 고정한다.
- cursor는 사용자 문맥, 마지막 항목의 정렬 기준값과 발급·만료 시각을 서버만 해석할 수 있는 불투명 값으로 제공한다.
- `activeUserCount`, `memberCount`와 사용자별 여행·가입 상태를 페이지 요청 시점마다 다시 평가하므로 다음 페이지 요청 전에 값이 바뀌면 항목의 중복 또는 누락이 발생할 수 있다.
- 빈 목록은 `200 OK`, `items=[]`, `nextCursor=null`, `hasNext=false`로 반환한다.
- `canJoin`은 현재 정책에 동의했을 때 `true`다. 활성 제재는 입장을 막지 않으며 실제 입장 API에서도 정책 동의 여부를 다시 검증한다.

## 4. 현재 채팅 정책 조회

### 기본 정보

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `GET` |
| URL | `/api/chat-policy` |
| 인증 | Access Token 필수 |

### 성공 응답

```json
{
  "code": "CHAT_POLICY_RETRIEVED",
  "message": "현재 채팅 운영 정책을 조회했습니다.",
  "data": {
    "policyVersionId": "9001",
    "version": "2026-09-01",
    "title": "지역 채팅 운영 정책",
    "content": "지역 채팅 운영 정책 본문",
    "effectiveAt": "2026-08-31T15:00:00Z",
    "consented": false,
    "consentedAt": null
  }
}
```

| HTTP 상태 | API 코드 | 조건 |
| ---: | --- | --- |
| `200 OK` | `CHAT_POLICY_RETRIEVED` | 현재 `ACTIVE` 정책과 동의 상태 조회 성공 |
| `401 Unauthorized` | `AUTHENTICATION_REQUIRED` | Access Token이 없거나 유효하지 않음 |
| `503 Service Unavailable` | `ACTIVE_CHAT_POLICY_UNAVAILABLE` | 활성 정책이 없어 채팅을 제공할 수 없음 |
| `500 Internal Server Error` | `INTERNAL_SERVER_ERROR` | 예상하지 못한 서버 내부 오류 |

### 처리 규칙

- `content`는 HTML이나 Markdown이 아닌 평문이다. 클라이언트는 이를 HTML로 해석하거나 DOM에 직접 삽입하지 않고 일반 텍스트로 렌더링한다.
- 현재 `ACTIVE` 상태이면서 적용 시각이 지난 정책 하나를 반환한다.
- `consented`는 사용자가 반환된 정확한 `policyVersionId`에 동의했는지를 나타낸다.
- 새 정책 버전이 활성화되면 과거 버전 동의로 입장할 수 없으며 새 정책에 다시 동의해야 한다.
- 기존 참여자는 새 정책에 다시 동의하기 전에도 가입·연결·구독 상태와 읽기 권한을 유지하지만 메시지 전송은 `CHAT_POLICY_CONSENT_REQUIRED`로 거부한다.
- 새 정책 활성화 시 현재 모든 WebSocket 세션에 동의 필요 개인 이벤트를 전달하며, 동의가 완료되면 재연결 없이 메시지를 전송할 수 있다.
- 초기에는 정책 관리자 API를 제공하지 않는다. 운영 DB 마이그레이션 또는 내부 배포 작업으로 새 버전을 추가한다.
- 활성화 트랜잭션에서 기존 `ACTIVE` 정책을 `RETIRED`로 바꾸고 새 정책을 `ACTIVE`로 전환한다. 실패하면 기존 활성 정책을 유지한다.
- `effective_at`이 지난 하나의 `ACTIVE` 정책만 서비스에 적용하며 활성화된 정책 본문을 직접 수정하지 않고 변경 시 새 버전을 생성한다.

## 5. 채팅 정책 동의

### 기본 정보

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `PUT` |
| URL | `/api/chat-policy/consent` |
| 인증 | Access Token 필수 |
| 멱등성 | 동일 정책 버전 재동의 결과가 같은 자연 멱등 PUT |

### 요청

```json
{
  "policyVersionId": "9001"
}
```

### 성공 응답

```json
{
  "code": "CHAT_POLICY_CONSENT_RECORDED",
  "message": "채팅 운영 정책에 동의했습니다.",
  "data": {
    "policyVersionId": "9001",
    "consentedAt": "2026-09-07T11:00:00.123456Z"
  }
}
```

| HTTP 상태 | API 코드 | 조건 |
| ---: | --- | --- |
| `200 OK` | `CHAT_POLICY_CONSENT_RECORDED` | 최초 동의 또는 기존 동일 동의 반환 |
| `400 Bad Request` | `INVALID_REQUEST` | 정책 버전 ID 형식이 유효하지 않음 |
| `401 Unauthorized` | `AUTHENTICATION_REQUIRED` | Access Token이 없거나 유효하지 않음 |
| `409 Conflict` | `CHAT_POLICY_VERSION_NOT_ACTIVE` | 요청한 정책이 현재 활성 버전이 아님 |
| `500 Internal Server Error` | `INTERNAL_SERVER_ERROR` | 예상하지 못한 서버 내부 오류 |

## 6. 채팅방 입장

### 기본 정보

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `PUT` |
| URL | `/api/regional-chat-rooms/{roomId}/members/me` |
| 인증 | Access Token 필수 |
| 멱등성 | 이미 참여 중이면 기존 참여 상태를 반환하는 자연 멱등 PUT |

### 성공 응답

```json
{
  "code": "REGIONAL_CHAT_ROOM_JOINED",
  "message": "지역 채팅방에 입장했습니다.",
  "data": {
    "roomId": "3001",
    "joinedAt": "2026-09-07T11:01:00.123456Z"
  }
}
```

| HTTP 상태 | API 코드 | 조건 |
| ---: | --- | --- |
| `200 OK` | `REGIONAL_CHAT_ROOM_JOINED` | 최초 입장 또는 기존 활성 참여 반환 |
| `401 Unauthorized` | `AUTHENTICATION_REQUIRED` | Access Token이 없거나 유효하지 않음 |
| `404 Not Found` | `REGIONAL_CHAT_ROOM_NOT_FOUND` | 채팅방이 존재하지 않음 |
| `409 Conflict` | `CHAT_POLICY_CONSENT_REQUIRED` | 현재 활성 정책에 동의하지 않음 |
| `500 Internal Server Error` | `INTERNAL_SERVER_ERROR` | 예상하지 못한 서버 내부 오류 |

### 처리 규칙

- 과거에 퇴장한 참여 이력이 있으면 새 행을 중복 생성하지 않고 `joined_at`을 재입장 시각으로 갱신하고 `left_at=null`로 변경한다. 최초 가입 시각을 위한 별도 컬럼은 두지 않는다.
- 다시 입장한 사용자는 가입 전과 퇴장 기간을 포함한 해당 방의 전체 `VISIBLE` 메시지 이력을 조회할 수 있다.
- 활성 제재 중인 사용자도 현재 정책에 동의한 상태라면 입장할 수 있지만 메시지는 전송할 수 없다.

## 7. 채팅방 퇴장

### 기본 정보

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `DELETE` |
| URL | `/api/regional-chat-rooms/{roomId}/members/me` |
| 인증 | Access Token 필수 |
| 인가 | 해당 채팅방의 활성 참여자 |
| 멱등성 | 퇴장 상태를 만드는 자연 멱등 DELETE |

### 성공 응답

```json
{
  "code": "REGIONAL_CHAT_ROOM_LEFT",
  "message": "지역 채팅방에서 나갔습니다.",
  "data": {
    "roomId": "3001",
    "leftAt": "2026-09-07T11:10:00.123456Z"
  }
}
```

| HTTP 상태 | API 코드 | 조건 |
| ---: | --- | --- |
| `200 OK` | `REGIONAL_CHAT_ROOM_LEFT` | 퇴장 성공 또는 기존 퇴장 상태 반환 |
| `401 Unauthorized` | `AUTHENTICATION_REQUIRED` | Access Token이 없거나 유효하지 않음 |
| `404 Not Found` | `REGIONAL_CHAT_ROOM_NOT_FOUND` | 채팅방이 존재하지 않음 |
| `500 Internal Server Error` | `INTERNAL_SERVER_ERROR` | 예상하지 못한 서버 내부 오류 |

### 처리 규칙

- 참여 행을 물리 삭제하지 않고 `left_at`을 기록한다.
- 퇴장 이후에는 이력 조회와 새 메시지 송수신을 허용하지 않는다. 다시 입장하면 이후 기능을 다시 사용할 수 있다.
- 퇴장 성공 시 해당 사용자의 모든 WebSocket 세션에서 이 방의 구독을 즉시 무효화하고 `activeUserCount`에서 사용자를 제거한다.
- 현재 모든 세션에 `CHAT_ROOM_MEMBERSHIP_ENDED` 개인 이벤트를 전달하되 WebSocket 연결 자체는 유지한다. 사용자가 채팅 기능 전체를 벗어날 때 연결을 종료한다.
- 퇴장과 동시에 진행된 `SEND`도 메시지 처리 시점에 활성 가입 상태를 다시 검사하며, 퇴장이 먼저 반영됐으면 `REGIONAL_CHAT_MEMBER_REQUIRED`로 거부한다.

## 8. 메시지 이력 조회

### 기본 정보

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `GET` |
| URL | `/api/regional-chat-rooms/{roomId}/messages` |
| 인증 | Access Token 필수 |
| 인가 | 해당 채팅방의 활성 참여자 |
| 페이지네이션 | 과거 탐색 cursor 또는 누락 복구 `afterMessageId`, 요청당 20개 고정 |

### 요청

```http
GET /api/regional-chat-rooms/3001/messages?cursor={opaqueCursor}
Authorization: Bearer {accessToken}
```

재연결 후 누락 메시지 복구:

```http
GET /api/regional-chat-rooms/3001/messages?afterMessageId=10001
Authorization: Bearer {accessToken}
```

| Query | 필수 | 설명 |
| --- | :---: | --- |
| `cursor` | N | 최초 최신 조회에서 발급받은 과거 방향 cursor. 발급 후 30분 동안 유효 |
| `afterMessageId` | N | 마지막으로 정상 수신한 메시지 ID. 해당 메시지보다 새로운 메시지를 복구할 때 사용 |

`cursor`와 `afterMessageId`는 동시에 전달할 수 없다.

### 성공 응답

```json
{
  "code": "CHAT_MESSAGES_RETRIEVED",
  "message": "채팅 메시지를 조회했습니다.",
  "data": {
    "items": [
      {
        "messageId": "10001",
        "clientMessageId": "019b1234-5678-7000-8000-123456789abc",
        "messageType": "TEXT",
        "text": "경주역 근처 맛집 추천해주세요.",
        "image": null,
        "sender": {
          "publicId": "01991f6e-7300-7b21-a3cc-1436db3df95e",
          "userName": "플랜잇사용자",
          "profileImageUrl": "https://example.invalid/presigned-profile-image"
        },
        "createdAt": "2026-09-07T11:05:00.123456Z"
      }
    ],
    "page": {
      "nextCursor": "opaque-older-message-cursor",
      "nextAfterMessageId": null,
      "hasNext": true
    }
  }
}
```

이미지 메시지는 `text=null`이고 `image`에 `imageFileId`, 조회용 `url`, `thumbnailUrl`, `mimeType`을 반환한다. 아직 썸네일이 없으면 `thumbnailUrl=null`이다.

| HTTP 상태 | API 코드 | 조건 |
| ---: | --- | --- |
| `200 OK` | `CHAT_MESSAGES_RETRIEVED` | 최근 또는 과거 메시지 조회 성공 |
| `400 Bad Request` | `INVALID_CURSOR` | cursor 형식·사용자·채팅방 문맥이 유효하지 않거나 발급 후 30분이 지남 |
| `400 Bad Request` | `INVALID_AFTER_MESSAGE_ID` | 기준 메시지가 없거나 해당 채팅방의 조회 가능한 메시지가 아님 |
| `400 Bad Request` | `INVALID_MESSAGE_HISTORY_QUERY` | `cursor`와 `afterMessageId`를 동시에 전달함 |
| `401 Unauthorized` | `AUTHENTICATION_REQUIRED` | Access Token이 없거나 유효하지 않음 |
| `403 Forbidden` | `REGIONAL_CHAT_MEMBER_REQUIRED` | 활성 참여자가 아님 |
| `404 Not Found` | `REGIONAL_CHAT_ROOM_NOT_FOUND` | 채팅방이 존재하지 않음 |
| `500 Internal Server Error` | `INTERNAL_SERVER_ERROR` | 예상하지 못한 서버 내부 오류 |

### 처리 규칙

- 최초 요청은 최신 `VISIBLE` 메시지 20개, cursor 요청은 그보다 오래된 메시지 20개를 반환한다.
- `afterMessageId` 요청은 기준 메시지보다 새로운 `VISIBLE` 메시지를 오래된 순서부터 최대 20개 반환한다. 더 남아 있으면 응답의 마지막 `messageId`를 `nextAfterMessageId`로 반환하고 `hasNext=true`로 설정한다.
- 최신·과거 조회에서는 `nextAfterMessageId=null`이고, 누락 복구 조회에서는 `nextCursor=null`이다.
- 응답 `items`는 화면 표시를 위해 오래된 메시지부터 최신 메시지 순으로 정렬한다.
- 메시지 순서는 서버가 생성한 `createdAt` 오름차순, 시각이 같으면 `messageId` 오름차순으로 고정한다.
- `BLOCKED` 메시지는 반환하지 않는다. 기간 경과만으로 일반 메시지를 조회에서 제외하지 않는다.
- cursor는 사용자·기준 메시지·채팅방 문맥과 발급·만료 시각을 서버만 해석할 수 있는 불투명 값으로 제공하며 발급 후 30분 동안 유효하다.
- 현재 활성 참여자라면 가입 전이나 과거 퇴장 기간에 작성된 메시지를 포함해 해당 방의 전체 `VISIBLE` 이력을 조회할 수 있다.
- 조회용 이미지 URL은 권한 확인 후 발급하며 5분 동안 유효하다.

## 9. 채팅 이미지 업로드 요청

### 기본 정보

| 항목 | 내용 |
| --- | --- |
| 상태 | `APPROVED` |
| Method | `POST` |
| URL | `/api/regional-chat-rooms/{roomId}/image-upload-requests` |
| 인증 | Access Token 필수 |
| 인가 | 해당 채팅방의 활성 참여자 |
| 멱등성 | 별도 멱등성 Key를 사용하지 않으며 요청마다 새 업로드 준비 결과 생성 |

### 요청

```json
{
  "originalFilename": "gyeongju.webp",
  "mimeType": "image/webp",
  "sizeBytes": 3145728
}
```

### 성공 응답

```json
{
  "code": "CHAT_IMAGE_UPLOAD_PREPARED",
  "message": "채팅 이미지 업로드를 준비했습니다.",
  "data": {
    "uploadUrl": "https://example.invalid/presigned-upload-url",
    "uploadKey": "temporary/users/019abc/chat/019def.webp",
    "uploadUrlExpiresAt": "2026-09-07T11:20:00.123456Z"
  }
}
```

| HTTP 상태 | API 코드 | 조건 |
| ---: | --- | --- |
| `200 OK` | `CHAT_IMAGE_UPLOAD_PREPARED` | 업로드 URL과 임시 객체 Key 발급 성공 |
| `400 Bad Request` | `INVALID_REQUEST` | 파일명, MIME type 또는 크기가 유효하지 않음 |
| `401 Unauthorized` | `AUTHENTICATION_REQUIRED` | Access Token이 없거나 유효하지 않음 |
| `403 Forbidden` | `REGIONAL_CHAT_MEMBER_REQUIRED` | 활성 참여자가 아님 |
| `404 Not Found` | `REGIONAL_CHAT_ROOM_NOT_FOUND` | 채팅방이 존재하지 않음 |
| `413 Content Too Large` | `CHAT_IMAGE_TOO_LARGE` | 원본 파일이 5 MB를 초과함 |
| `415 Unsupported Media Type` | `UNSUPPORTED_IMAGE_TYPE` | JPEG·PNG·WebP가 아님 |
| `500 Internal Server Error` | `INTERNAL_SERVER_ERROR` | 예상하지 못한 서버 내부 오류 |

### 처리 규칙

- 업로드 URL은 10분 동안 유효하다.
- `Idempotency-Key`는 사용하지 않는다. 같은 파일 정보로 업로드 준비 API를 다시 호출해도 새로운 `uploadKey`와 Presigned URL을 발급하며 사용하지 않은 임시 객체는 24시간 Lifecycle로 정리한다.
- `uploadKey`는 서버가 현재 사용자와 `CHAT_MESSAGE` 용도에 맞는 S3 임시 영역 경로로 생성하며, 클라이언트가 임의로 변경하지 않는다.
- 클라이언트가 S3 임시 영역에 업로드한 뒤 실시간 이미지 메시지 전송 payload에 `uploadKey`를 전달한다.
- 활성 제재 중에도 업로드 준비는 허용하지만 실제 이미지 메시지 `SEND`는 거부한다. 전송되지 않은 임시 객체는 기존 S3 Lifecycle 정책에 따라 정리한다.
- 서버는 로그인 사용자와 `uploadKey`의 소유 경로·용도를 확인하고 S3 객체 존재 여부, 크기와 실제 파일 형식을 검증한 뒤 `image_files.image_purpose=CHAT_MESSAGE`로 확정한다.
- 형식은 JPEG(`image/jpeg`), PNG(`image/png`), WebP(`image/webp`), 크기는 최대 5,242,880 byte다.
- 미제출 임시 객체는 업로드 후 24시간이 지나면 S3 Lifecycle 삭제 대상이 된다.

## 10. 실시간 메시지 계약

| 항목 | 상태 | 현재 결정 |
| --- | --- | --- |
| 전송 기술 | `APPROVED` | Native WebSocket 기반 Spring WebSocket + STOMP |
| 메시지 브로커 | `APPROVED` | Spring Simple Broker |
| WebSocket 연결 경로 | `APPROVED` | `/ws` |
| 인증 전달 방식 | `APPROVED` | STOMP `CONNECT`의 `Authorization: Bearer {accessToken}` |
| Access Token 만료 | `APPROVED` | 연결 종료 후 토큰 갱신·재연결·재구독·HTTP 이력 복구 |
| Heartbeat | `APPROVED` | 클라이언트·서버 모두 10초 간격 |
| 네트워크 단절 자동 재연결 | `APPROVED` | 제공하지 않음 |
| 연결·구독 상태 저장 | `APPROVED` | Redis를 사용하지 않고 현재 애플리케이션 인스턴스 메모리에서 관리 |
| 사용자별 동시 연결 | `APPROVED` | 탭·기기별 다중 연결을 제한 없이 허용 |
| 허용 Origin | `APPROVED` | 개발 환경 `http://localhost:5173`만 허용, 배포 Origin은 확정 전까지 허용하지 않음 |
| 로그아웃 | `APPROVED` | 채팅방 가입 상태는 유지하고 현재 STOMP 연결·구독만 종료 |
| WebSocket 연결 범위 | `APPROVED` | 채팅 기능 진입 시 연결하고 채팅 기능을 완전히 벗어나면 종료 |
| 방 구독 범위 | `APPROVED` | 현재 화면에서 보고 있는 채팅방 하나만 구독 |
| 미구독 중 메시지 | `APPROVED` | 실시간 전달하지 않고 방 재진입 시 HTTP 이력으로 조회 |
| 메시지 전송 destination | `APPROVED` | `/app/regional-chat-rooms/{roomId}/messages` |
| 방 메시지 구독 destination | `APPROVED` | `/topic/regional-chat-rooms/{roomId}/messages` |
| 개인 이벤트 구독 destination | `APPROVED` | `/user/queue/chat-events` |
| 메시지 시각·정렬 | `APPROVED` | UTC `createdAt` 오름차순, 동률이면 `messageId` 오름차순 |
| 반복·과속 제한 | `APPROVED` | 최소 300ms, 10초당 최대 10개, 동일 정규화 텍스트 30초 내 재전송 차단 |
| 금지 표현 판정 | `APPROVED` | RDB `chat_prohibited_terms`의 활성 금칙어 사전으로 판정 |
| 이미지 메시지 확정 | `APPROVED` | 객체 검증·영구 저장·DB Commit 후 공개, 썸네일은 비동기 생성 |
| 애플리케이션 결과 이벤트 | `APPROVED` | 발신자에게만 `CHAT_MESSAGE_RESULT` 전달 |
| 구독 결과 이벤트 | `APPROVED` | 구독 요청 세션에 `CHAT_ROOM_SUBSCRIPTION_RESULT` 전달 |
| 공개 메시지 이벤트 | `APPROVED` | 승인된 방 구독자에게 `CHAT_MESSAGE_CREATED` 전달 |
| 메시지 중복 방지 | `APPROVED` | 발신 사용자와 `clientMessageId` 조합으로 중복 저장 방지 |
| 동일 사용자 동시 전송 | `APPROVED` | 서로 다른 세션·`clientMessageId` 요청을 별도 Lock이나 Queue 없이 독립 처리 |
| 재연결 복구 | `APPROVED` | HTTP 메시지 이력 API로 누락 구간 조회 |

실시간 구현은 다음 의미 계약을 유지한다.

- STOMP `CONNECT` 단계에서 Access Token을 검증하고 인증 사용자를 WebSocket 세션에 연결한다. 인증 전에는 `SUBSCRIBE`와 `SEND`를 허용하지 않는다.
- Access Token이 만료되면 연결을 종료한다. 클라이언트는 HTTP로 Access Token을 갱신한 뒤 재연결·재구독하고 HTTP 메시지 이력 API로 누락 메시지를 복구한다.
- 클라이언트와 서버는 10초 간격의 STOMP Heartbeat를 협상한다. Simple Broker의 Heartbeat는 애플리케이션 내부 `TaskScheduler`로 처리한다.
- 네트워크 단절이나 Heartbeat 타임아웃 이후 자동 재연결은 제공하지 않는다. 클라이언트는 연결 끊김 상태를 표시하고 사용자의 명시적인 재진입·재시도 시 새 연결을 생성한다.
- Simple Broker의 연결·구독 상태와 Spring이 발급한 WebSocket/STOMP `sessionId`는 현재 애플리케이션 인스턴스 메모리에서만 관리하며 Redis에 저장하지 않는다.
- 이 구성은 단일 애플리케이션 인스턴스를 전제로 한다. 서버 재시작 시 연결·구독 상태가 사라지며, 다중 인스턴스 간 연결·구독 정보 공유를 제공하지 않는다.
- 같은 사용자의 여러 탭·기기 연결을 허용하며 새 연결이 기존 연결을 종료하지 않는다.
- 개발 환경에서는 WebSocket handshake의 Origin이 `http://localhost:5173`인 요청만 허용한다. 배포 Origin은 배포 주소 확정 후 별도로 추가한다.
- 로그아웃 시 클라이언트는 현재 STOMP 연결을 종료하고 Access Token을 제거한다. `regional_chat_room_members`의 가입 상태와 `joinedAt`은 변경하지 않으며, 다시 로그인해 연결하면 가입된 채팅방을 다시 구독할 수 있다.
- 클라이언트는 채팅 기능에 진입할 때 WebSocket을 연결하고 채팅 기능을 완전히 벗어날 때 연결을 종료한다.
- 현재 화면에서 보고 있는 채팅방 하나만 `SUBSCRIBE`한다. 다른 방으로 이동할 때 기존 방을 `UNSUBSCRIBE`한 뒤 새 방을 `SUBSCRIBE`한다.
- 가입했지만 현재 구독하지 않은 채팅방의 메시지는 실시간으로 전달하지 않는다. 해당 방에 다시 들어갈 때 HTTP 메시지 이력 API로 누락 메시지를 조회한다.
- 방 메시지 `SUBSCRIBE`는 해당 채팅방의 활성 참여자에게만 허용하며 활성 제재 여부는 검사하지 않는다.
- 구독 성공·실패는 요청한 세션의 `/user/queue/chat-events`에 `CHAT_ROOM_SUBSCRIPTION_RESULT`로 전달한다. 실패해도 WebSocket 연결은 종료하지 않으며 승인된 구독만 실시간 입장 상태와 `activeUserCount`에 반영한다.
- 메시지 `SEND`는 해당 채팅방의 활성 참여자이면서 제재 중이 아니고 현재 활성 정책에 동의한 사용자에게만 허용한다.
- 새 정책 동의가 필요하면 연결·구독과 읽기는 유지하고 `SEND`만 `CHAT_POLICY_CONSENT_REQUIRED`로 거부한다. 새 정책 활성화 시 현재 모든 연결 세션에 동의 필요 이벤트를 전달하고 동의 후에는 재연결 없이 전송을 허용한다.
- 텍스트와 이미지 메시지는 한 메시지에 동시에 포함하지 않는다.
- 이미지 메시지는 업로드 요청 API에서 받은 `uploadKey`를 사용한다.
- 텍스트·이미지 메시지 전송 payload는 클라이언트가 생성한 UUIDv7 `clientMessageId`를 필수로 포함한다.
- 서버가 생성한 UTC `createdAt`과 `messageId`를 메시지 순서의 유일한 기준으로 사용한다. 클라이언트 시각은 순서 결정에 사용하지 않는다.
- `VISIBLE` 메시지는 Commit 이후 방 destination에 `CHAT_MESSAGE_CREATED`로 발행한다. 발신자도 해당 방을 구독 중이면 같은 공개 이벤트를 수신하며 개인 결과 이벤트와 공개 이벤트의 도착 순서는 보장하지 않는다.
- 클라이언트는 `messageId`와 `clientMessageId`를 사용해 낙관적 표시 또는 두 이벤트 수신으로 인한 중복 렌더링을 방지한다.
- 정책 위반 메시지는 `BLOCKED`로 저장하고 다른 사용자에게 전달하지 않는다.
- 제재가 시작되면 개인 이벤트로 종료 시각과 남은 시간을 알린다. 연결과 구독은 유지해 읽기를 허용하고 이후 메시지 전송만 `CHAT_SANCTION_ACTIVE`로 거부한다.
- 동일 사용자가 같은 `clientMessageId`와 같은 payload를 재전송하면 새 메시지를 저장하지 않고 최초 처리 결과를 다시 전달한다.
- 동일 사용자가 같은 `clientMessageId`를 다른 payload에 재사용하면 `CLIENT_MESSAGE_ID_REUSED`로 거부한다.
- 동일 사용자가 여러 세션에서 서로 다른 `clientMessageId`로 동시에 전송한 요청은 별도 사용자 단위 Lock이나 Queue 없이 독립적으로 처리한다.
- 동시 요청 사이의 처리 순서는 보장하지 않으며, 최종 화면 순서는 서버가 저장한 UTC `createdAt`, `messageId`로 결정한다.
- 초기 구현은 `(sender_user_id, client_message_id)` UNIQUE 제약에 의한 중복 방지만 보장한다. 동시 위반 횟수 계산과 제재 생성의 추가 직렬화는 제공하지 않는다.
- 실제 동시 전송량과 정합성 문제가 확인되면 사용자별 인메모리 Lock·Queue, DB Row Lock·낙관적 Lock 또는 메시지 브로커 파티셔닝을 검토한다.

### 메시지 전송 payload

텍스트 메시지:

```json
{
  "clientMessageId": "019b1234-5678-7000-8000-123456789abc",
  "messageType": "TEXT",
  "text": "경주 맛집 추천 부탁드립니다."
}
```

이미지 메시지:

```json
{
  "clientMessageId": "019b1234-5678-7000-8000-123456789abc",
  "messageType": "IMAGE",
  "uploadKey": "temporary/users/019abc/chat/019def.webp"
}
```

- `TEXT`는 `text`만, `IMAGE`는 `uploadKey`만 허용한다. 타입과 맞지 않는 필드를 포함하거나 두 콘텐츠를 동시에 포함하면 `INVALID_CHAT_MESSAGE_PAYLOAD`로 거부한다.
- 텍스트는 앞뒤 공백을 제거하고 줄바꿈을 제외한 연속 공백을 하나로 변환한다. 줄바꿈은 보존하며 저장·전송과 반복 판정에 동일한 정규화 결과를 사용한다.
- 정규화한 텍스트는 Unicode 코드 포인트 기준 1자 이상 1,000자 이하여야 한다. 공백만 있거나 0자가 되면 `INVALID_CHAT_MESSAGE_PAYLOAD`, 1,000자를 초과하면 `CHAT_MESSAGE_TOO_LONG`으로 거부한다.
- 사용자별 메시지 간격이 300ms 미만이거나 최근 10초 동안 10개를 초과하면 `RATE_LIMIT_EXCEEDED`로 차단한다.
- 동일한 정규화 텍스트를 30초 안에 다시 전송하면 `DUPLICATE_MESSAGE`로 차단한다.
- 이미지 메시지도 300ms 및 10초당 10개 제한에 포함하지만 동일 텍스트 반복 판정에는 포함하지 않는다.
- 과속 판정용 최근 전송 시각은 Redis 없이 현재 애플리케이션 인스턴스 메모리에서 관리한다. 서버 재시작 시 판정용 단기 상태는 초기화되지만 DB의 위반·제재 이력은 유지한다.
- `RATE_LIMIT_EXCEEDED`, `DUPLICATE_MESSAGE`, `PROHIBITED_EXPRESSION`만 정책 위반으로 누적한다.
- 인증·가입·정책 동의 실패, 활성 제재 중 전송, 잘못된 payload, 본문 길이 초과, 잘못된 이미지 업로드 Key와 서버·객체 저장소 오류는 정책 위반 횟수에 포함하지 않는다.

### 금지 표현 판정

- 금칙어 원본은 `chat_prohibited_terms.term`, 판정용 정규화 값은 `normalized_term`에 저장한다.
- `is_active=true`인 항목만 사용하며 `EXACT`는 전체 문자열 일치, `CONTAINS`는 부분 문자열 포함으로 판정한다.
- 메시지와 금칙어에 동일한 공백·영문 소문자 정규화를 적용한 뒤 비교한다.
- 금칙어 사전은 RDB를 기준정보로 사용하고 단일 애플리케이션 인스턴스의 로컬 메모리에 활성 목록을 적재해 판정한다. Redis와 별도 캐시 서버는 사용하지 않는다.
- 서버 시작 시 활성 금칙어 전체를 최초 적재하고, 이후 60초마다 DB에서 다시 조회한 불변 목록으로 원자적으로 교체한다. 갱신 중에는 기존 목록으로 계속 판정한다.
- 주기적 갱신에 실패하면 기존 목록을 유지하고 오류를 기록한다. 서버 시작 시 최초 적재에 실패하면 금칙어 검사를 우회하지 않고 채팅 메시지 전송 기능을 비활성화한다.
- DB 변경은 최대 60초 후 판정에 반영될 수 있다. 금칙어 규모나 메시지 처리량 때문에 단순 비교가 병목이 될 때 다중 문자열 검색 구조를 검토한다.
- 금지 표현으로 차단된 메시지는 `BLOCKED`로 저장하고 `PROHIBITED_EXPRESSION` 위반 이력을 기록한다.
- 초기에는 금칙어 관리자 API를 제공하지 않는다. 운영 마이그레이션이나 승인된 관리 SQL로 추가·비활성화하며, 원문 수정이나 물리 삭제 대신 기존 행을 `is_active=false`로 변경하고 새 행을 추가한다.
- 위반 이력에는 금칙어 FK나 일치 문자열 스냅샷을 저장하지 않고 `reason_code=PROHIBITED_EXPRESSION`만 기록한다.

### 제재 만료 판정

- 활성 제재는 DB의 `starts_at <= 현재 UTC 시각 < ends_at` 조건으로 판정하며 서버 재시작과 무관하게 적용한다.
- 별도 만료 스케줄러 없이 제재 조회 또는 메시지 전송 시 만료된 `ACTIVE` 행을 `EXPIRED`로 갱신한다.
- 초기에는 관리자 수동 제재 해제 기능을 제공하지 않으며 제재 상태는 `ACTIVE`, `EXPIRED`만 사용한다.

### 이미지 메시지 확정

- 서버는 `uploadKey`의 사용자 소유 경로와 `CHAT_MESSAGE` 용도를 확인한다.
- 하나의 `uploadKey`는 하나의 이미지 메시지에만 사용할 수 있다. 이미지 메시지와 영구 객체 저장이 Commit된 뒤 임시 객체를 사용 완료 처리하며 같은 Key의 재사용은 `CHAT_IMAGE_ALREADY_USED`로 거부한다.
- 다른 사용자의 Key는 `INVALID_CHAT_IMAGE_UPLOAD`, 임시 객체가 존재하지 않거나 24시간이 지난 Key는 `CHAT_IMAGE_UPLOAD_EXPIRED`로 거부한다. DB Commit 전에 처리가 실패한 Key는 임시 객체가 유효한 동안 같은 이미지 메시지 요청에 다시 사용할 수 있다.
- S3 임시 객체의 존재 여부, 실제 크기, MIME type과 파일 형식을 검증한다.
- 검증된 원본의 EXIF 방향을 반영하고 GPS 등 메타데이터를 제거한 영구 객체를 저장한다.
- 영구 객체와 `image_files`, `chat_messages`, `image_chat_messages` 저장이 완료된 뒤 DB 트랜잭션을 Commit한다.
- Commit 이후에만 방 구독자에게 이미지 메시지를 발행하고 발신자에게 `ACCEPTED` 결과 이벤트를 전달한다.
- 객체 검증이나 영구 저장에 실패하면 채팅 메시지를 저장·발행하지 않고 `REJECTED` 결과를 전달한다. 미확정 임시 객체는 24시간 후 S3 Lifecycle로 삭제한다.
- 원본 확정은 동기 처리하고 최대 512×512 WebP 썸네일은 비동기로 생성한다. 생성 전에는 `thumbnailUrl=null`이다.

### 애플리케이션 결과 이벤트

- 결과 이벤트 자체는 DB에 저장하지 않는다. 정상·차단 메시지와 위반·제재 이력만 각 도메인 테이블에 저장한다.
- 결과 이벤트는 발신자 전용 `/user/queue/chat-events`로만 전달한다. Spring이 현재 인바운드 메시지의 STOMP `sessionId`를 사용해 메시지를 보낸 연결 하나에만 전달하며, 별도 Redis나 세션 테이블에 저장하지 않는다.
- `ACCEPTED`는 메시지가 `VISIBLE`로 저장된 결과, `BLOCKED`는 메시지가 `BLOCKED`로 저장됐지만 방에 발행되지 않은 결과, `REJECTED`는 메시지를 저장하지 않고 거부한 결과다.
- DB 트랜잭션이 완료된 후 결과 이벤트를 발행한다. 이벤트가 유실되더라도 저장된 공개 메시지는 HTTP 메시지 이력 API로 복구한다.

구독 성공 예시:

```json
{
  "eventType": "CHAT_ROOM_SUBSCRIPTION_RESULT",
  "status": "ACCEPTED",
  "code": "CHAT_ROOM_SUBSCRIBED",
  "message": "채팅방 실시간 구독을 시작했습니다.",
  "roomId": "3001",
  "occurredAt": "2026-09-17T12:00:00.123456Z",
  "data": null
}
```

비가입자의 구독은 같은 이벤트의 `status=REJECTED`, `code=REGIONAL_CHAT_MEMBER_REQUIRED`로 전달한다. 구독 결과 이벤트는 DB에 저장하지 않고 구독을 요청한 STOMP 세션 하나에만 전달한다.

공개 메시지 예시:

```json
{
  "eventType": "CHAT_MESSAGE_CREATED",
  "roomId": "3001",
  "occurredAt": "2026-09-17T12:00:01.123456Z",
  "data": {
    "messageId": "10001",
    "clientMessageId": "019b1234-5678-7000-8000-123456789abc",
    "messageType": "TEXT",
    "text": "경주 맛집 추천 부탁드립니다.",
    "image": null,
    "sender": {
      "publicId": "01991f6e-7300-7b21-a3cc-1436db3df95e",
      "userName": "플랜잇사용자",
      "profileImageUrl": "https://example.invalid/presigned-profile-image"
    },
    "createdAt": "2026-09-17T12:00:01.123456Z"
  }
}
```

`CHAT_MESSAGE_CREATED.data`는 HTTP 메시지 이력 항목과 같은 구조를 사용한다. 메시지에는 발신 사용자 ID만 영속화하고 응답과 이벤트의 `sender`는 조회·발행 시점의 현재 닉네임과 현재 프로필 이미지로 구성한다. 프로필이 바뀌면 과거 메시지에도 최신 정보가 표시되며 별도 프로필 스냅샷 컬럼은 두지 않는다. 발신 사용자의 `deleted_at`에 값이 있으면 `sender`에는 `userName="탈퇴한 사용자"`만 반환하고 `publicId`와 `profileImageUrl` 필드는 포함하지 않는다. 기존 메시지와 내부 `sender_user_id`는 삭제하지 않는다.

정책 재동의 필요 예시:

```json
{
  "eventType": "CHAT_POLICY_CONSENT_REQUIRED",
  "status": "REQUIRED",
  "code": "CHAT_POLICY_CONSENT_REQUIRED",
  "message": "변경된 채팅 운영 정책에 동의해야 메시지를 전송할 수 있습니다.",
  "occurredAt": "2026-09-17T12:00:00.123456Z",
  "data": {
    "policyVersionId": "12"
  }
}
```

정책 활성화에 따른 동의 필요 이벤트는 사용자의 현재 모든 WebSocket 세션에 전달하며 DB에 저장하지 않는다.

채팅방 퇴장 예시:

```json
{
  "eventType": "CHAT_ROOM_MEMBERSHIP_ENDED",
  "status": "ENDED",
  "code": "REGIONAL_CHAT_ROOM_LEFT",
  "message": "채팅방에서 퇴장했습니다.",
  "roomId": "3001",
  "occurredAt": "2026-09-17T12:10:00.123456Z",
  "data": null
}
```

퇴장 이벤트는 사용자의 현재 모든 WebSocket 세션에 전달하며 DB에 별도 저장하지 않는다. 각 세션은 해당 방 구독을 제거하지만 WebSocket 연결은 유지한다.

정상 처리 예시:

```json
{
  "eventType": "CHAT_MESSAGE_RESULT",
  "status": "ACCEPTED",
  "code": "CHAT_MESSAGE_ACCEPTED",
  "message": "메시지가 전송되었습니다.",
  "clientMessageId": "019b1234-5678-7000-8000-123456789abc",
  "roomId": "3001",
  "occurredAt": "2026-09-17T11:05:00.123456Z",
  "data": {
    "messageId": "10001",
    "createdAt": "2026-09-17T11:05:00.123456Z"
  }
}
```

정책 위반 차단 예시:

```json
{
  "eventType": "CHAT_MESSAGE_RESULT",
  "status": "BLOCKED",
  "code": "CHAT_MESSAGE_BLOCKED",
  "message": "채팅 정책 위반으로 메시지가 전송되지 않았습니다.",
  "clientMessageId": "019b1234-5678-7000-8000-123456789abc",
  "roomId": "3001",
  "occurredAt": "2026-09-17T11:05:00.123456Z",
  "data": {
    "messageId": "10001",
    "reasonCode": "PROHIBITED_EXPRESSION",
    "violationCount": 2
  }
}
```

요청 거부 예시:

```json
{
  "eventType": "CHAT_MESSAGE_RESULT",
  "status": "REJECTED",
  "code": "REGIONAL_CHAT_MEMBER_REQUIRED",
  "message": "채팅방에 참여한 사용자만 메시지를 전송할 수 있습니다.",
  "clientMessageId": "019b1234-5678-7000-8000-123456789abc",
  "roomId": "3001",
  "occurredAt": "2026-09-17T11:05:00.123456Z",
  "data": null
}
```

제재 시작 예시:

```json
{
  "eventType": "CHAT_SANCTION_UPDATED",
  "status": "ACTIVE",
  "code": "CHAT_SANCTION_APPLIED",
  "message": "채팅 메시지 전송이 일시적으로 제한되었습니다.",
  "occurredAt": "2026-09-17T11:05:00.123456Z",
  "data": {
    "sanctionLevel": 1,
    "startsAt": "2026-09-17T11:05:00.123456Z",
    "endsAt": "2026-09-18T11:05:00.123456Z",
    "remainingSeconds": 86400
  }
}
```

제재 중 전송 거부 예시:

```json
{
  "eventType": "CHAT_MESSAGE_RESULT",
  "status": "REJECTED",
  "code": "CHAT_SANCTION_ACTIVE",
  "message": "채팅 메시지 전송이 제한된 상태입니다.",
  "clientMessageId": "019b1234-5678-7000-8000-123456789abc",
  "roomId": "3001",
  "occurredAt": "2026-09-17T11:06:00.123456Z",
  "data": {
    "endsAt": "2026-09-18T11:05:00.123456Z",
    "remainingSeconds": 86340
  }
}
```

### 가입 상태와 실시간 입장 상태

| 상태 | 판단 기준 | 저장 위치 | 로그아웃·연결 종료 시 처리 |
| --- | --- | --- | --- |
| 채팅방 가입 상태 | `regional_chat_room_members.left_at IS NULL` | RDB | 유지 |
| 현재 실시간 입장 상태 | 인증된 STOMP 세션이 방 destination을 `SUBSCRIBE` 중 | 애플리케이션 인스턴스 메모리 | 제거 |

- 가입 상태는 사용자가 명시적으로 채팅방 퇴장 API를 호출할 때만 종료한다.
- 실시간 입장 상태는 STOMP `SUBSCRIBE`, `UNSUBSCRIBE`, `DISCONNECT`와 Heartbeat 타임아웃에 따라 갱신한다.
- 동일 사용자가 여러 세션으로 같은 방을 구독하더라도 현재 실시간 입장 사용자 수를 계산할 때는 사용자 한 명으로 중복 제거한다.
- `memberCount`는 별도 DB 컬럼 없이 `regional_chat_room_members.left_at IS NULL`인 고유 사용자를 요청 시 집계한다.
- `activeUserCount`는 별도 DB 컬럼 없이 현재 인스턴스 메모리의 방별 STOMP 구독 상태에서 고유 사용자를 요청 시 집계한다.
- 현재 실시간 입장 상태는 `roomId → userId → sessionId 집합` 구조로 관리한다. 한 사용자의 마지막 세션이 제거될 때만 해당 방의 `activeUserCount`에서 제외한다.

### 최초 입장 시 이력과 실시간 메시지 결합

- 클라이언트는 먼저 `/user/queue/chat-events`를 구독한 뒤 현재 방 destination을 구독한다.
- `CHAT_ROOM_SUBSCRIPTION_RESULT`의 `ACCEPTED`를 확인한 다음 최신 HTTP 메시지 이력을 조회한다.
- HTTP 조회가 끝날 때까지 수신한 `CHAT_MESSAGE_CREATED` 이벤트는 클라이언트가 임시 보관한다.
- HTTP 이력과 임시 보관한 실시간 메시지를 `messageId`로 중복 제거하고 서버 `createdAt`, `messageId` 오름차순으로 정렬해 표시한다.
- 구독이 승인된 후 이력을 조회하므로 이력 요청 도중 생성된 메시지는 실시간 이벤트로 보완하며, 동일 메시지가 양쪽에 포함돼도 한 번만 표시한다.

### 기술 선택 근거와 향후 Netty 검토

- 초기 오픈채팅 서비스는 Spring WebSocket + STOMP로 구현한다. Spring의 메시지 처리 기능과 기존 비즈니스 로직을 활용해 구현과 운영의 복잡도를 낮춘다.
- Netty는 초기 구현에 직접 도입하지 않는다. 추후 부하 테스트나 출시 후 모니터링에서 동시 연결 수, 초당 메시지 수, 방별 수신자 수, 메시지 전달 지연과 서버 자원 사용량을 측정한다.
- 부하가 증가하면 DB 처리, 메시지 배포, 네트워크 처리 등 실제 병목을 먼저 확인한다. 네트워크 처리 계층의 병목이나 세밀한 연결·버퍼·스레드 제어의 필요성이 확인되면 Netty 도입을 검토한다.
- 채팅 서버 분리 여부는 Netty 도입과 별도로 판단한다. Netty 사용이 서버 분리를 요구하지 않으며, Spring WebSocket + STOMP 구성도 별도 채팅 애플리케이션으로 분리할 수 있다.
- 이 결정은 구현 방향에 대한 합의이며, 구현 완료나 부하 테스트를 통한 성능 검증을 의미하지 않는다.
