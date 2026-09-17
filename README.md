# PlanIt 문서

PlanIt의 공통 기획, 비즈니스 규칙, API 계약과 데이터 모델 문서를 한곳에서 관리한다.

## 문서 목록

### 프로젝트와 요구사항

- [프로젝트 개요](./PROJECT_OVERVIEW.md)
- [최종 비즈니스 규칙](./FIGMA_FINAL_BUSINESS_RULES.md)
- [Figma V1·V2·V3 분석](./FIGMA_FINAL_V1_V2_V3_ANALYSIS.md)

### API 계약

- [API 명세 진입점](./docs/API_SPEC.md)
- [전체 API 목록](./docs/API_INVENTORY.md)
- [API 명세 작성 규약](./docs/API_SPEC_GUIDELINES.md)
- [여행 API](./docs/TRIP_API_SPEC.md)
- [일정 API](./docs/SCHEDULE_API_SPEC.md)
- [지역 오픈 채팅 API](./docs/CHAT_API_SPEC.md)
- [커뮤니티 API](./docs/COMMUNITY_API_SPEC.md)
- [포토 미션 API](./docs/PHOTO_MISSION_API_SPEC.md)
- [알림 API](./docs/NOTIFICATION_API_SPEC.md)

### 데이터 모델

- [통합 DDL](./erd/planit_consolidated.sql)
- `erd/planit_v3_slice*.sql` 파일은 현행 기준이 아닌 이전 단계 기록이다.

### 개발 환경 기록

- [프론트엔드 설정 과정](./frontend/SETUP.md)
- [백엔드 기술 스택](./backend/TECH_STACK.md)

## 관리 기준

- 공통 요구사항, API 계약과 데이터 모델은 이 저장소의 문서를 기준으로 관리한다.
- 프론트엔드와 백엔드 저장소에는 실행 방법과 해당 코드에 직접 적용되는 작업 규칙을 남긴다.
- 구현 여부와 실행 검증 여부를 구분하고, 구현되지 않은 동작을 문서에 확정된 동작처럼 기록하지 않는다.
