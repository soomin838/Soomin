# RezeroAgent (윈도우 우선)

윈도우 환경에서 동작하는 블로그 자동화 에이전트입니다.
목표는 고품질 기술형 글을 자동 생성/발행하고, 무료 티어 한도 내에서 안정적으로 운영하는 것입니다.

## 가장 간단한 사용법(터미널 없이)

1. 바탕화면의 `RezeroAgentInstaller.exe` 실행
2. 설치 완료 후 시작 메뉴의 `RezeroAgent` 클릭
3. 앱 UI에서 API 키/설정을 입력하고 저장

참고:
- 이 경로는 `설치형 인스톨러` 방식입니다(포터블 아님).
- 앱 실행/설정에 터미널이 필수는 아닙니다.
- 운영 실행은 반드시 설치된 경로(`C:\Program Files\RezeroAgent\RezeroAgent.exe`)를 사용하세요.
- `dist\RezeroAgent.exe`는 테스트/검증용 빌드 산출물입니다.

## 핵심 특징

- 윈도우 우선 실행 (`main.py`)
- 소스 수집: Stack Exchange / Hacker News / GitHub 이슈
- 완전 무료 모드: `budget.free_mode=true`일 때 Gemini API 미사용(로컬 규칙 기반 생성)
- 품질 게이트: 발행 전 자동 QA 점수화(구조/단어수/링크/출처/버스티니스/금지어)
- LLM Judge: 발행 전 Gemini 기반 2차 심사(점수 미달 시 자동 보류)
- 글 구조: AEA(Authority-Evidence-Action) 기반 장문 구조(문제정의/근거/실행체크리스트/의사결정가이드)
- 이미지: 스크린샷 + 차트 (포스트당 최소 10장 목표)
- 이미지: 생성형 API 기반(기본 `Pollinations`) 포스트당 5장(썸네일 1 + 본문 4)
- 썸네일/본문 기본 모델: `gptimage` (전체 이미지 단일 모델)
- 썸네일 검증: Gemini Vision OCR로 핵심 단어 오타 검수(최대 3회 재생성, 실패 시 no-text 썸네일)
- 이미지 호스팅: Google Cloud Storage 퍼블릭 URL 기반 `<img src="...">` 삽입 (Drive 직링크 기본 비활성)
- 중단 작업 자동 재개: `collect_done/draft_done/images_done` WIP 초안을 Blogger에 단계별 갱신하고, 다음 워크플로우에서 이어서 처리
- 스크린샷 연출: 영어(EN-US) 우선 로딩 + 핵심 영역 자동 크롭(16:9) + 포커스 프레임
- 문단 선별 후 이미지 배치: 문단별 프롬프트 생성 및 문단 근처 삽입
- 법적 리스크 완화: 출처/라이선스 자동 표기 + 원문 유사도 차단
- 예산 가드: 일일 포스트 수/일일 Gemini 호출 수 제한
- 중복 방지: 최근 주제 + 의미 유사도 기반 후보 제외
- 내부 링크 자동화: 최근 발행 글 중 연관 문서 3개 자동 삽입
- 실패 시 윈도우 알림 팝업

## 주요 파일

- `main.py`: 에이전트 실행 루프
- `core/scout.py`: 데이터 수집
- `core/brain.py`: 주제 선정/본문 생성
- `core/visual.py`: 이미지 생성 파이프라인
- `core/publisher.py`: Blogger 발행
- `core/workflow.py`: 전체 오케스트레이션
- `config/settings.yaml`: 운영 설정

## 설치

```powershell
pip install -r requirements.txt
python -m playwright install chromium
```

## 설정

`config/settings.yaml`에서 아래를 채우세요.

- `gemini.api_key` (완전 무료 모드에서는 비워도 동작)
- `blogger.blog_id`
- 필요 시 `sources.*` 임계치
- 필요 시 `visual.*`(이미지 개수/생성형 이미지 ON/OFF/모델)
- 필요 시 `visual.image_provider` (`pollinations` 또는 `gemini`)
- 필요 시 `visual.pollinations_api_key`, `visual.pollinations_thumbnail_model`, `visual.pollinations_content_model`
- 기본값은 `publish.image_hosting_backend: blogger_media` (Blogger 미디어 URL 우선)
- 필요 시 `publish.gcs_bucket_name` (Blogger 미디어 실패 시 GCS 백업 업로드용)
- 필요 시 `publish.gcs_public_base_url` (GCS 커스텀 CDN 도메인 사용 시)
- 필요 시 `quality.*`(발행 최소 점수/강제 보류/금지어 등)

OAuth 파일 준비:

- `config/client_secrets.json`
- `config/blogger_token.json`
- `config/service_account.json` (선택: Indexing API 또는 GCS 백업 업로드 사용 시)

앱 UI에서 더 간단하게 설정하려면:

1. `Settings` 열기
2. 연결 방식 선택
3. `JSON 업로드` 또는 `Google 로그인` 또는 `토큰 직접 연결`
4. 필요한 값 확인 후 `Save` 클릭

## 실행

일반 실행:

```powershell
python main.py
```

초기 설정 마법사(필수 키 입력/저장, UI 창):

```powershell
python main.py --setup
```

참고:
- 필수값이 비어 있으면 실행 시 자동으로 설정 UI 창이 뜹니다.
- 한 번 저장하면 다음 실행부터 자동 사용됩니다.

즉시 1회 강제 실행 후 계속 루프 유지:

```powershell
python main.py --once
```

6파트 QA 점검 리포트 생성:

```powershell
python scripts/run_sixpart_qa.py
```

심화 자체점검 문서:
- `QA_66_SELF_REVIEW.md`

운영 QA 로그(72시간/발행/장애/업데이트) 리포트:

```powershell
python scripts/qa_log_report.py
```

로그 파일:
- `storage/logs/qa_runtime.jsonl` (원본 이벤트 로그)
- `storage/logs/qa_runtime_report.json` (요약 리포트)

## 작업 스케줄러

로그온 시 1개 인스턴스로 상주시작 등록:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/register_task.ps1
```

해제:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/unregister_task.ps1
```

## 배포

exe 빌드:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/build_windows.ps1 -NoInstaller
```

빠른 exe 빌드(개발용, 캐시 재사용/의존성 재설치 생략):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/build_windows_fast.ps1
```

빠른 빌드 동작:
- 기본은 `증분 exe 빌드`입니다(캐시 재사용, clean 생략).
- 인스톨러는 기본적으로 생성하지 않으며, `installer.iss`가 바뀐 경우에만 자동 재생성됩니다.
- 강제로 인스톨러까지 만들려면:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/build_windows_fast.ps1 -WithInstaller
```

설치형 exe 포함 빌드(Inno Setup 필요):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/build_windows.ps1
```

백그라운드 빌드(터미널 점유 최소화):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/build_windows_background.ps1
```

빠른 백그라운드 빌드(개발용):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/build_windows_fast_background.ps1
```

앱 패치형 적용(재빌드 없이 재시작만):

1. `%APPDATA%\RezeroAgent\patches\runtime_patch.py` 생성
   - 템플릿: `patches/runtime_patch.py.example`
2. 패치 코드 작성 후 앱 재시작
3. 재시작 스크립트:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/restart_agent.ps1
```

## 운영 주의

- 애드센스 수익이 있으면 실무상 상업적 이용으로 간주될 수 있습니다.
- 본문 하단 출처/라이선스 표기를 유지하세요.
- 약관 위반 가능 수집 방식은 사용하지 않는 것을 권장합니다.
- 완전 무료 운영을 원하면 `budget.free_mode: true`를 유지하세요(LLM API 호출 0건).
- 품질 자동 보류를 유지하려면 `quality.strict_mode: true`를 권장합니다.
- 스크린샷 필수 모드: Chrome/Edge/Playwright Chromium 순서로 자동 탐지 시도하며, 모두 실패하면 원인과 시도 경로를 오류로 표시합니다.
- 예약 발행: `publish.use_blogger_schedule: true`면 Blogger에 미래 시각으로 예약 발행됩니다.
- 무료 모드 글도 장문 AEA 구조(심화 분석 섹션 포함)로 확장됩니다.
- 주제 자동 확장: `topic_growth.enabled: true`면 Gemini로 합법/전체연령 검증을 통과한 새 주제를 하루 1개(`daily_new_topics`)씩 `storage/seeds/topics.jsonl`에 추가합니다.

## Google Search Links (Beta) 수동 활성화 가이드

1. Blogger 관리자 화면 진입
2. `설정(Settings)` -> `검색 환경설정(Search preferences)` 이동
3. 베타 실험 항목에서 `Google Search Links` 토글 활성화
4. 저장 후 5~10분 대기, 블로그 새로고침으로 반영 확인

참고:
- 해당 항목은 계정/지역/시점에 따라 노출되지 않을 수 있습니다.
- 베타 UI가 보이지 않으면 Blogger 실험 기능(테스트 그룹) 상태를 먼저 확인하세요.

## 이미지 업로드 백엔드 체크리스트

1. 권장 기본: `publish.image_hosting_backend: blogger_media`
2. Blogger 재로그인 후 토큰 갱신(`Google 로그인`)
3. 실패 대비 백업을 쓰려면 아래 GCS 항목 추가
4. `publish.gcs_bucket_name` 입력
5. `config/service_account.json` 배치
6. 서비스 계정에 Storage Object Admin(또는 업로드+공개 권한) 부여
7. 버킷 퍼블릭 읽기 정책(또는 CDN 공개 경로) 설정

GCS 전용 모드가 필요하면:

1. `publish.image_hosting_backend: gcs`
2. `publish.gcs_bucket_name` 입력
3. `config/service_account.json` 배치
4. 서비스 계정에 Storage Object Admin(또는 업로드+공개 권한) 부여
5. 버킷 퍼블릭 읽기 정책(또는 CDN 공개 경로) 설정
