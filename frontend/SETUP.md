# 프론트엔드 개발 환경 설정 과정

이 문서는 `frontend` 프로젝트를 어떤 순서와 이유로 만들었는지 정리한 문서입니다.

## 1. 기술 스택 선택

대화에서 아래 3가지를 먼저 확인했습니다.

| 항목 | 선택 |
| --- | --- |
| 프레임워크 | React |
| 언어 | TypeScript |
| 스타일링 | CSS Modules (순수 CSS) |

여기에 더해 웹 애플리케이션에 일반적으로 필요한 아래 항목을 기본값으로 추가했습니다.

> **업데이트**: 처음에는 Sass로 세팅했었는데, SCSS 문법을 모른다고 하셔서 **순수 CSS(CSS Modules)** 로 전환했습니다. CSS Modules는 문법이 일반 CSS와 완전히 같고, 파일마다 클래스 이름이 자동으로 겹치지 않게 관리되는 기능만 추가된 것입니다.

- **런타임**: Node.js 24 LTS — Vite 개발 서버, npm 패키지 설치, 타입 검사와 프로덕션 빌드에 사용합니다.
- **빌드 도구**: Vite — React + TypeScript 조합의 개발 서버와 프로덕션 빌드를 담당합니다.
- **라우팅**: react-router-dom — 페이지가 여러 개인 웹 애플리케이션을 만들 예정이라 가정하고 추가했습니다.
- **HTTP 통신**: axios — 백엔드 API 연동이 필요할 것으로 예상해 기본 클라이언트를 미리 만들어 두었습니다.
- **린트/포맷**: oxlint(Vite 템플릿 기본 포함) + Prettier — 코드 스타일 일관성을 위해 추가했습니다.

## 2. 프로젝트 생성 명령어

```bash
npm create vite@latest frontend -- --template react-ts
cd frontend
npm install
npm install prettier --save-dev
npm install react-router-dom axios
```

## 3. 기본 보일러플레이트 정리

Vite 템플릿이 생성한 기본 파일 중 프로젝트에서 쓰지 않을 것들을 정리했습니다.

- `src/App.css`, `src/index.css` 삭제 → CSS Modules 기반 스타일 체계로 전환
- `src/App.tsx` → Router 셸 역할을 하도록 작성
- `src/assets/react.svg`, `hero.png`, `vite.svg` 삭제 → 불필요한 샘플 이미지 제거

## 4. 폴더 구조 설계

```
src/
├── api/          axios 인스턴스, API 호출 함수
├── assets/       이미지, 폰트 등 정적 리소스
├── components/   재사용 가능한 공통 컴포넌트
├── hooks/        커스텀 훅
├── pages/        라우트 단위 페이지 (페이지별 폴더 + .module.css)
├── routes/       라우트 정의
├── styles/       전역 스타일, 변수
├── utils/        공통 유틸 함수
├── App.tsx       Router 최상단 셸
├── main.tsx      진입점
└── vite-env.d.ts Vite 환경 변수 타입
```

각 폴더를 나눈 이유:

- **pages 폴더별 구조**: 페이지마다 폴더를 만들고 그 안에 `.tsx`와 `.module.css`를 같이 두면, 페이지가 늘어나도 어떤 스타일이 어떤 페이지 것인지 헷갈리지 않습니다.
- **styles의 CSS 변수**: `variables.css`에서 `:root { --color-primary: ...; }` 형태로 색상·스페이싱 값을 선언해두면, 다른 CSS 파일에서 `var(--color-primary)`로 가져다 쓸 수 있습니다. 값이 바뀌면 한 곳만 고치면 됩니다.
- **api 폴더**: `client.ts`의 axios 인스턴스를 공통으로 사용하고 기능별 API 파일을 추가할 수 있습니다.

## 5. 만든 파일들과 역할

| 파일 | 역할 |
| --- | --- |
| `src/styles/variables.css` | 색상, 스페이싱, 폰트 크기 등 디자인 토큰 (CSS 변수 `:root`) |
| `src/styles/global.css` | reset 및 전역 기본 스타일 |
| `src/api/client.ts` | axios 인스턴스, 요청/응답 인터셉터 |
| `src/hooks/useWindowSize.ts` | 화면 크기 감지 예시 커스텀 훅 |
| `src/routes/index.tsx` | `<Routes>` 정의 (현재는 `/` → Home만 등록) |
| `src/pages/Home/Home.tsx` | 첫 페이지 (화면 설계서 전달 시 여기서부터 구현 예정) |
| `src/App.tsx` | `BrowserRouter`로 감싸는 최상단 셸 |
| `tsconfig.app.json` | 브라우저 애플리케이션 TypeScript 설정 |
| `tsconfig.node.json` | Vite 설정 파일 TypeScript 설정 |
| `.nvmrc` | 팀에서 사용할 Node.js 24 버전 지정 |
| `.env.example` | API 서버 주소 등 환경 변수 예시 |
| `.prettierrc.json` | Prettier 포맷 규칙 |
| `README.md` | 프로젝트 소개와 협업 규칙 |

## 6. 동작 확인

```bash
npm run build
```
`npm run build`는 TypeScript 타입 검사 후 Vite 프로덕션 빌드를 실행합니다.

## 7. Git-flow 적용

- `master`: 배포 가능한 코드
- `develop`: 다음 배포를 위한 개발 코드
- `feature/*`: 기능 개발 브랜치
- `release/*`: 배포 준비 브랜치
- `hotfix/*`: 배포 버전의 긴급 수정 브랜치

최초 환경 설정은 다음 커밋 메시지로 기록합니다.

```bash
git commit -m "chore: 프론트엔드 개발 환경 초기 설정"
```

새 기능은 `develop`에서 `feature/*` 브랜치를 만든 뒤 Pull Request를 통해 `develop`에 반영합니다.

## 8. 다음 단계

화면 설계서가 전달되면:
1. 설계서의 화면 단위로 작업 범위를 나눕니다.
2. `develop`에서 기능 단위 `feature/*` 브랜치를 생성합니다.
3. `pages/`에 페이지 폴더, 필요 시 `components/`에 공통 컴포넌트를 추가하며 구현합니다.
