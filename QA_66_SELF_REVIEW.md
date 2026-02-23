# RezeroAgent 66문항 자체 Q/A (6파트 x 11문항)

## Part 1. Blogger 실발행/이미지/출처
1. Q: 발행 전 필수값 검증이 있는가? A: 부족했다. Action: preflight 추가.
2. Q: Blog ID 형식 오류를 실행 전에 막는가? A: 미흡. Action: 정규식 검증 추가.
3. Q: 토큰 JSON 누락 필드를 잡는가? A: 미흡. Action: 필수필드 검사 추가.
4. Q: 출처 블록 누락 시 발행 보류 가능한가? A: 가능. Action: quality gate 유지.
5. Q: 링크 최소 개수 확인하는가? A: 가능. Action: QA 점수 반영.
6. Q: 이미지 10장 정책은 과한가? A: 상황 의존. Action: 설정값으로 유지.
7. Q: 이미지가 문단에 분산 배치되는가? A: 가능. Action: 기존 분산 로직 유지.
8. Q: 앵커 매칭 실패 시 자연스러운가? A: 보완 필요. Action: 문단 균등 분산 유지.
9. Q: 출처 라이선스 문구는 들어가는가? A: 들어감. Action: 유지.
10. Q: 원문 유사도 차단이 있는가? A: 있음. Action: 0.68 기준 유지.
11. Q: 발행 로그에 품질 정보가 남는가? A: 가능. Action: qa-score 라벨 유지.

## Part 2. 인증/입력값 검증
12. Q: 무료모드에서도 API 키 강제인가? A: 예(문제). Action: 조건부 필수로 수정.
13. Q: 무료모드 + 이미지생성 OFF에서 키 없이 저장 가능한가? A: 가능하도록 수정.
14. Q: 무료모드 + 이미지생성 ON이면 키 필수인가? A: 예. Action: 강제 검증.
15. Q: API 키 형식 오류를 즉시 알리는가? A: 예. Action: 한글 경고 유지.
16. Q: Blog ID 비숫자 입력을 막는가? A: 예. Action: 유지.
17. Q: 토큰 경로 비어있을 때 막는가? A: 예. Action: 유지.
18. Q: 상대경로 토큰도 해석 가능한가? A: 예. Action: ROOT 기준 해석 유지.
19. Q: 실행 직전 전체 설정 검증이 있는가? A: 없었다. Action: preflight 실행 추가.
20. Q: preflight 오류를 UI에 보여주는가? A: 예. Action: Warning 박스 추가.
21. Q: setup/일반실행 모두 검증 경로가 동일한가? A: 일반실행 보강. Action: 적용.
22. Q: 잘못된 설정으로 무한실패 진입을 줄였는가? A: 예. Action: 실행 전 차단.

## Part 3. 무료모드/비용 0 운영
23. Q: free_mode=true면 LLM 호출 0건인가? A: 목표상 예. Action: 기존 로직 유지.
24. Q: free_mode UI에서 제어 가능한가? A: 아니었다. Action: 체크박스 추가.
25. Q: 무료모드와 온보딩 필수값이 충돌하는가? A: 충돌. Action: 동적 필수값 변경.
26. Q: free_mode=false면 키 필수인가? A: 예. Action: 유지.
27. Q: 이미지 생성 활성 시 키 필수인가? A: 예. Action: 유지.
28. Q: 모델 테스트 호출은 비용 리스크가 큰가? A: 낮음(쿼터 소모). Action: 안내 유지.
29. Q: daily budget guard는 있는가? A: 있음. Action: 유지.
30. Q: 무료모드에서 dry_run 사용 가능한가? A: 가능. Action: 유지.
31. Q: 무료모드 글 품질은 충분한가? A: 추가 QA 필요. Action: quality gate 강화 완료.
32. Q: 무료모드 fallback이 명확한가? A: 예. Action: 로컬 생성 사용.
33. Q: 무료모드 설명이 문서에 있는가? A: 있음. Action: README 유지.

## Part 4. 실패 알림/복구/루프 유지
34. Q: 실패 시 앱이 종료되는가? A: 아니오. Action: 루프 유지.
35. Q: 실패 시 사용자 알림이 있는가? A: 예(Windows MessageBox).
36. Q: 재시도 백오프가 있는가? A: 예. Action: 지수 백오프 유지.
37. Q: 백오프 상한이 있는가? A: 예. Action: 설정값 유지.
38. Q: 실패 원인이 UI 로그에 남는가? A: 예. Action: 유지.
39. Q: 실패 후 다음 실행 시간 계산이 맞는가? A: 예. Action: 유지.
40. Q: 강제 실행 플래그는 리셋되는가? A: 예. Action: 유지.
41. Q: 오류가 반복될 때 원인 파악이 쉬운가? A: 보통. Action: preflight로 사전 차단 강화.
42. Q: 설정 오류와 런타임 오류를 구분하는가? A: 개선됨. Action: preflight 분리.
43. Q: 네트워크 오류 처리 메시지가 명확한가? A: 있음. Action: 유지.
44. Q: 실패 시 데이터 손실 없는가? A: 로그 저장됨. Action: 유지.

## Part 5. 품질 게이트/글쓰기 QA
45. Q: 글 길이 최소 검증이 있는가? A: 예.
46. Q: 헤딩 구조 검증이 있는가? A: 예.
47. Q: 리스트 기반 실천 항목 검증이 있는가? A: 예.
48. Q: 외부 링크 최소 기준이 있는가? A: 예.
49. Q: 권위 링크 allow-list 검증이 있는가? A: 예.
50. Q: 출처 블록 검증이 있는가? A: 예.
51. Q: 금지 AI 마커 검증이 있는가? A: 예.
52. Q: 버스티니스 검증이 있는가? A: 예.
53. Q: 미달 시 자동 보정이 되는가? A: 예.
54. Q: strict_mode에서 발행 보류 가능한가? A: 예.
55. Q: 품질 점수가 로그/라벨에 반영되는가? A: 예.

## Part 6. 설치/업데이트/지속운영
56. Q: 재설치 시 설정이 유지되는가? A: 예(APPDATA 저장).
57. Q: 번들 기본파일 자동 복사되는가? A: 예.
58. Q: tzdata 누락 이슈 방어가 있는가? A: 예(safe_tz + 패키징).
59. Q: 실행 파일 업데이트 경로가 명확한가? A: 예(installer 재실행).
60. Q: 필수 폴더 자동 생성되는가? A: 예(initialize_runtime_home).
61. Q: 로그 DB/JSON 이중 저장되는가? A: 예.
62. Q: 작업 스케줄러 연동 가능한가? A: 예.
63. Q: setup UI로 터미널 없이 설정 가능한가? A: 예.
64. Q: 모델 선택 오류 가능성을 줄였는가? A: 예(모델 테스트).
65. Q: 설정 파손 시 복구 난이도는? A: 중간. Action: preflight로 조기 감지.
66. Q: 초보 사용자 실수 방지 수준은 충분한가? A: 개선 중. Action: 이번 반영으로 강화.

## 이번 66문항 결과로 실제 반영한 코드
- `core/preflight.py` 추가: 실행 전 설정 무결성 검사.
- `main.py` 수정: Free Mode UI 추가, 조건부 API 키 검증, preflight 오류 팝업.
- `core/onboarding.py` 수정: 무료모드 기준 동적 필수값 판정.
- 기존 품질게이트(`core/quality.py`, `core/workflow.py`)와 결합해 발행 전 자동 QA 강화.
