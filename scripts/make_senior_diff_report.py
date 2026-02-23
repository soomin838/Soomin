from __future__ import annotations

import html
import subprocess
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import PageBreak, Paragraph, Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle


def _register_fonts() -> tuple[str, str]:
    body_font = "Helvetica"
    mono_font = "Courier"
    malgun = Path(r"C:\Windows\Fonts\malgun.ttf")
    if malgun.exists():
        try:
            pdfmetrics.registerFont(TTFont("MalgunGothic", str(malgun)))
            body_font = "MalgunGothic"
        except Exception:
            pass
    return body_font, mono_font


def _read_lines(path: Path, start: int, end: int) -> str:
    if not path.exists():
        return f"# missing: {path}"
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    out: list[str] = []
    for idx in range(start, min(end, len(lines)) + 1):
        out.append(f"{idx:4d}: {lines[idx - 1]}")
    return "\n".join(out)


def _wrap_code(code: str, width: int = 140) -> str:
    wrapped: list[str] = []
    for raw in code.splitlines():
        line = raw.rstrip("\n")
        while len(line) > width:
            wrapped.append(line[:width] + " \\")
            line = line[width:]
        wrapped.append(line)
    return "\n".join(wrapped)


def _compile_check(project_root: Path) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            ["python", "-m", "compileall", str(project_root)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=120,
        )
        ok = proc.returncode == 0
        msg = "PASS" if ok else "FAIL"
        detail = (proc.stdout or proc.stderr or "").strip()
        detail = detail[-1200:] if detail else "(no output)"
        return ok, f"{msg}\n{detail}"
    except Exception as exc:
        return False, f"FAIL\n{exc}"


def build_report(project_root: Path, output_pdf: Path) -> Path:
    body_font, mono_font = _register_fonts()
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "TitleKR",
        parent=styles["Title"],
        fontName=body_font,
        fontSize=20,
        leading=24,
    )
    h1_style = ParagraphStyle(
        "H1KR",
        parent=styles["Heading1"],
        fontName=body_font,
        fontSize=14,
        leading=18,
        spaceBefore=8,
        spaceAfter=6,
    )
    h2_style = ParagraphStyle(
        "H2KR",
        parent=styles["Heading2"],
        fontName=body_font,
        fontSize=11.5,
        leading=15,
        spaceBefore=6,
        spaceAfter=4,
    )
    body_style = ParagraphStyle(
        "BodyKR",
        parent=styles["BodyText"],
        fontName=body_font,
        fontSize=9.8,
        leading=14,
    )
    code_style = ParagraphStyle(
        "CodeStyle",
        parent=styles["Code"],
        fontName=mono_font,
        fontSize=7.4,
        leading=9.0,
    )

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    desktop = Path.home() / "Desktop"
    installer = desktop / "RezeroAgentInstaller.exe"
    installer_info = "not found"
    if installer.exists():
        st = installer.stat()
        installer_info = f"found | size={st.st_size} bytes | mtime={datetime.fromtimestamp(st.st_mtime)}"

    compile_ok, compile_text = _compile_check(project_root)
    compile_badge = "PASS" if compile_ok else "FAIL"

    story = []
    story.append(Paragraph("RezeroAgent 작업 보고서 (상세 Diff 첨부)", title_style))
    story.append(Spacer(1, 8))
    story.append(Paragraph(f"생성 시각: {html.escape(now)}", body_style))
    story.append(Paragraph(f"프로젝트 경로: {html.escape(str(project_root))}", body_style))
    story.append(Paragraph("보고 목적: Senior 기획 검토용 / 최근 반영 작업 상세 공유", body_style))
    story.append(Spacer(1, 10))

    summary_data = [
        ["항목", "결과"],
        ["컴파일 스모크 테스트", compile_badge],
        ["인스톨러 파일 상태", installer_info],
        ["핵심 반영", "Global keyword 추출, Topic pool 확장, 실시간 Blogger snapshot 연동, 중복 방지 강화"],
    ]
    tbl = Table(summary_data, colWidths=[120, 390])
    tbl.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), body_font),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E8ECF7")),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#A4B0C0")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(tbl)
    story.append(Spacer(1, 10))

    story.append(Paragraph("1) 변경 개요", h1_style))
    story.append(
        Paragraph(
            "- 텍스트 생성/주제 선정/품질/중복 방지 로직을 Rezero 2.1 Global 목표에 맞춰 재정렬했습니다.",
            body_style,
        )
    )
    story.append(
        Paragraph(
            "- `systemInstruction` 분리 적용, 글로벌 키워드 동적 추출, topic pool 영구 확장, 실시간 Blogger 기준 지표 집계를 반영했습니다.",
            body_style,
        )
    )
    story.append(
        Paragraph(
            "- `72h 큐`, `오늘 게시량`, `오늘 글로벌 키워드`는 로컬 추정이 아니라 가능한 경우 Blogger 실시간 데이터로 계산하도록 유지/강화했습니다.",
            body_style,
        )
    )
    story.append(Spacer(1, 8))

    story.append(Paragraph("2) 핵심 Diff (요약)", h1_style))
    diff_blocks = [
        (
            "A. 모델/페르소나/시드 기본값 정렬",
            """--- a/core/settings.py (legacy defaults)
+++ b/core/settings.py (current)
@@ SourceSettings
- seeds_path = "storage/seeds/topics.jsonl"
+ seeds_path = "storage/seeds/seeds.json"
- stackexchange_tagged = "python;automation"
+ stackexchange_tagged = "artificial-intelligence;productivity;automation;chatgpt"
@@ GeminiSettings
- model = "gemini-2.5-flash"
+ model = "gemini-2.0-pro-exp-02-05"
- editor_persona = "<generic editor>"
+ editor_persona = "You are a native English tech influencer..."
@@ TopicGrowthSettings
- daily_new_topics = 1
+ daily_new_topics = 5
""",
        ),
        (
            "B. 생성 프롬프트 구조 개선 + system_instruction 분리",
            """--- a/core/brain.py
+++ b/core/brain.py
@@ generate_post()
- "Mandatory human-like sections: 1..8"
+ "Structure: Narrative Flow"
+ "Do not use the list items as section headers. Weave them into the story naturally."
@@ _generate_text()
- persona prepended in user prompt
+ body["systemInstruction"] = {"parts":[{"text": effective_system}]}
@@ choose_best()
+ target_keywords/recent_urls/recent_titles 를 랭킹 프롬프트에 주입
""",
        ),
        (
            "C. 워크플로우: 글로벌 키워드 단계 + 14일 중복 방지 강화",
            """--- a/core/workflow.py
+++ b/core/workflow.py
@@ run_once()
+ phase "trend": extract_global_keywords(candidates, limit=5)
+ choose_best(... target_keywords=global_keywords)
+ run log note: keywords=...
@@ duplicate gate
- loose lexical similarity
+ _set_jaccard >= 0.80
+ _bow_cosine >= 0.80
+ _semantic_near_duplicate(title) gate
@@ backlog note
+ embedding 기반 중복 검사(text-embedding-004) 전환 준비 주석 추가
""",
        ),
        (
            "D. Topic Growth 영구 저장/누적",
            """--- a/core/topic_growth.py
+++ b/core/topic_growth.py
@@ maybe_grow()
+ daily_new_topics 기준으로 배치 생성
+ generated topic 중 중복 제거 후 seeds 저장
@@ _append_seed()
+ .json 포맷 영구 append 지원
+ legacy .jsonl append fallback 유지
@@ safety
+ illegal/adult/hate/scam/weapon 등 차단어 필터
""",
        ),
        (
            "E. 실시간 Blogger 집계/표시 강화",
            """--- a/core/workflow.py + a/main.py
+++ b/core/workflow.py + b/main.py
@@ _blog_snapshot()
+ source=blogger 우선, 실패 시 local fallback
+ today_posts/scheduled_72h를 live 상태 기준으로 계산
@@ usage_text()
+ "집계 소스: Blogger 실시간 | 로컬 추정"
+ "오늘 글로벌 키워드: ..."
""",
        ),
    ]
    for title, block in diff_blocks:
        story.append(Paragraph(title, h2_style))
        story.append(Preformatted(_wrap_code(block), code_style))
        story.append(Spacer(1, 6))

    story.append(PageBreak())
    story.append(Paragraph("3) 실제 코드 근거 스니펫 (라인 번호)", h1_style))
    snippets = [
        ("core/brain.py:39-140", project_root / "core" / "brain.py", 39, 140),
        ("core/brain.py:221-273", project_root / "core" / "brain.py", 221, 273),
        ("core/brain.py:527-654", project_root / "core" / "brain.py", 527, 654),
        ("core/workflow.py:129-181", project_root / "core" / "workflow.py", 129, 181),
        ("core/workflow.py:759-820", project_root / "core" / "workflow.py", 759, 820),
        ("core/topic_growth.py:31-77", project_root / "core" / "topic_growth.py", 31, 77),
        ("core/topic_growth.py:79-133", project_root / "core" / "topic_growth.py", 79, 133),
        ("core/scout.py:46-86", project_root / "core" / "scout.py", 46, 86),
        ("main.py:286-313", project_root / "main.py", 286, 313),
        ("main.py:341-376", project_root / "main.py", 341, 376),
        ("config/settings.yaml:46-77", project_root / "config" / "settings.yaml", 46, 77),
    ]
    for title, path, start, end in snippets:
        story.append(Paragraph(title, h2_style))
        snippet = _read_lines(path, start, end)
        story.append(Preformatted(_wrap_code(snippet), code_style))
        story.append(Spacer(1, 5))

    story.append(PageBreak())
    story.append(Paragraph("4) 검증 로그", h1_style))
    story.append(Paragraph("compileall 실행 결과", h2_style))
    story.append(Preformatted(_wrap_code(compile_text), code_style))
    story.append(Spacer(1, 8))
    story.append(Paragraph("인스톨러 확인", h2_style))
    story.append(Paragraph(installer_info, body_style))

    story.append(Spacer(1, 10))
    story.append(Paragraph("5) 주의/백로그", h1_style))
    story.append(Paragraph("- Git 저장소 정보가 없어 VCS 기반 원본 diff는 생성하지 못했고, 코드 기준 변경 diff(구현 diff)로 대체했습니다.", body_style))
    story.append(Paragraph("- 중복 방지는 현재 lexical/BoW 기반(0.80 threshold)이며, embedding(cosine) 전환은 backlog로 유지합니다.", body_style))
    story.append(Paragraph("- 이미지 생성/업로드는 OAuth/Drive 권한 상태에 따라 실패할 수 있으므로 운영 시점 인증 상태 점검이 필요합니다.", body_style))

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(output_pdf),
        pagesize=A4,
        leftMargin=24,
        rightMargin=24,
        topMargin=24,
        bottomMargin=24,
        title="RezeroAgent 작업 보고서 상세 Diff",
        author="Codex",
    )
    doc.build(story)
    return output_pdf


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    output_pdf = Path.home() / "Desktop" / "RezeroAgent_작업보고서_상세Diff.pdf"
    saved = build_report(project_root, output_pdf)
    print(str(saved))


if __name__ == "__main__":
    main()

