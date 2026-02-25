# RezeroAgent Git Data Map (for GPT)

이 문서는 GPT가 레포만 보고 현재 상태를 빠르게 파악하도록 만든 안내서입니다.

## 1) 코드 위치
- `main.py`: 앱 엔트리/메인 UI/워크플로우 연결
- `core/workflow.py`: 전체 실행 파이프라인(수집→생성→QA→이미지→발행)
- `core/quality.py`: 품질 게이트/하드 실패 규칙/QA 타이밍
- `core/visual.py`: 이미지 생성(Pollinations/Gemini fallback 포함)
- `core/publisher.py`: Blogger 업로드/썸네일 preflight/발행 검증
- `core/settings.py`: 설정 스키마와 normalize 규칙
- `config/settings.yaml`: 런타임 설정값

## 2) 런타임 스냅샷 위치
- `reports/runtime_snapshot/latest/`

여기에는 AppData의 핵심 로그/DB/설정 사본이 들어갑니다.

### 핵심 파일
- `reports/runtime_snapshot/latest/MANIFEST.md`
- `reports/runtime_snapshot/latest/README.md`
- `reports/runtime_snapshot/latest/logs/*.jsonl`
- `reports/runtime_snapshot/latest/db/DATABASE_SUMMARY.md`
- `reports/runtime_snapshot/latest/db/*.sqlite`
- `reports/runtime_snapshot/latest/meta/settings.yaml`

## 3) 로그 해석 우선순위
1. `logs/workflow_perf.jsonl`: 단계별 병목
2. `logs/qa_timing.jsonl`: QA 체크별 시간
3. `logs/visual_pipeline.jsonl`: 이미지 생성/쿼터/재시도
4. `logs/publisher_upload.jsonl`: 업로드 응답 코드/본문 일부
5. `logs/thumbnail_gate.jsonl`: 썸네일 게이트 실패 원인

## 4) 스냅샷 갱신 방법
다음 스크립트를 실행하면 최신 AppData 기반 스냅샷이 생성됩니다.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/export_runtime_snapshot.ps1
```

## 5) 보안 정책
- 토큰/시크릿 파일(`*token*`, `*secret*`, `blogger_token.json`, `service_account.json`)은 자동 제외됩니다.
- 로그/DB에는 운영 데이터가 포함될 수 있으므로 공개 레포 공유 시 주의가 필요합니다.
