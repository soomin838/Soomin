from __future__ import annotations

import re
from datetime import datetime
from html import escape, unescape
from pathlib import Path
from typing import Iterable

from .publisher import BlogPostItem


def export_blog_posts_pdf(
    posts: Iterable[BlogPostItem],
    output_path: Path,
    blog_id: str,
    source_label: str = "Blogger Live API",
) -> Path:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer
    except Exception as exc:
        raise RuntimeError("PDF 내보내기를 위해 reportlab 패키지가 필요합니다.") from exc

    output_path.parent.mkdir(parents=True, exist_ok=True)
    post_list = list(posts)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        name="TitleKR",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
    )
    h_style = ParagraphStyle(
        name="HeadingKR",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=13,
        leading=16,
    )
    meta_style = ParagraphStyle(
        name="MetaKR",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=9.5,
        leading=13,
    )
    body_style = ParagraphStyle(
        name="BodyKR",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10.5,
        leading=15,
    )

    story = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    story.append(Paragraph("RezeroAgent Blog Export", title_style))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"Generated: {escape(now)}", meta_style))
    story.append(Paragraph(f"Blog ID: {escape(blog_id or '-')}", meta_style))
    story.append(Paragraph(f"Source: {escape(source_label)}", meta_style))
    story.append(Paragraph(f"Posts: {len(post_list)}", meta_style))
    story.append(Spacer(1, 12))

    if not post_list:
        story.append(Paragraph("No posts found for the selected filter.", body_style))
    else:
        for idx, post in enumerate(post_list, start=1):
            story.append(Paragraph(f"{idx}. {escape(post.title or '(untitled)')}", h_style))
            story.append(
                Paragraph(
                    " | ".join(
                        [
                            f"status={escape(post.status or '-')}",
                            f"published={escape(post.published or '-')}",
                            f"updated={escape(post.updated or '-')}",
                        ]
                    ),
                    meta_style,
                )
            )
            if post.url:
                story.append(Paragraph(f"URL: {escape(post.url)}", meta_style))
            story.append(Spacer(1, 6))

            text = _html_to_text(post.content or "")
            if text:
                for chunk in _to_paragraph_chunks(text):
                    story.append(Paragraph(escape(chunk), body_style))
                    story.append(Spacer(1, 4))
            else:
                story.append(Paragraph("(본문 없음 또는 본문 미포함 조회)", body_style))

            if idx < len(post_list):
                story.append(PageBreak())

    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        leftMargin=28,
        rightMargin=28,
        topMargin=24,
        bottomMargin=24,
    )
    doc.build(story)
    return output_path


def _html_to_text(html: str) -> str:
    txt = html or ""
    txt = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", txt)
    txt = re.sub(r"(?i)<br\s*/?>", "\n", txt)
    txt = re.sub(r"(?i)</(p|div|section|article|h1|h2|h3|h4|h5|h6|li|tr)>", "\n", txt)
    txt = re.sub(r"(?i)<li\b[^>]*>", "- ", txt)
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = unescape(txt)
    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    txt = re.sub(r"[ \t]+\n", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    txt = re.sub(r"[ \t]{2,}", " ", txt)
    return txt.strip()


def _to_paragraph_chunks(text: str, max_chunk: int = 1400) -> list[str]:
    paras = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    out: list[str] = []
    for para in paras:
        if len(para) <= max_chunk:
            out.append(para)
            continue
        start = 0
        while start < len(para):
            out.append(para[start:start + max_chunk].strip())
            start += max_chunk
    return out

