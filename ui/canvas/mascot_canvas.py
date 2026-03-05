from __future__ import annotations

import math
from PySide6.QtCore import QTimer, Qt, QRectF, QPointF
from PySide6.QtGui import QPainter, QColor, QRadialGradient, QPen, QLinearGradient, QPainterPath, QFont
from PySide6.QtWidgets import QWidget, QSizePolicy


class MascotCanvas(QWidget):
    """
    Rezy 카와이 슬라임 마스코트:
    둥글고 귀여운 캐릭터가 상태에 따라 표정과 애니메이션이 바뀜.
    """

    # ── Color Palettes per state ──
    _PALETTES = {
        "idle":    {"body": QColor(180, 155, 230), "cheek": QColor(255, 180, 200, 90), "eye": QColor(60, 40, 80)},
        "running": {"body": QColor(160, 140, 255), "cheek": QColor(255, 160, 220, 110), "eye": QColor(50, 30, 100)},
        "success": {"body": QColor(140, 220, 190), "cheek": QColor(255, 200, 210, 100), "eye": QColor(40, 80, 60)},
        "error":   {"body": QColor(240, 140, 140), "cheek": QColor(255, 100, 100, 80), "eye": QColor(100, 30, 30)},
        "paused":  {"body": QColor(160, 150, 170), "cheek": QColor(200, 180, 200, 60), "eye": QColor(80, 70, 90)},
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(140, 140)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self._status = "idle"
        self._frame = 0
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._animate)
        self._timer.start(1000 // 30)  # 30 FPS — 부드럽지만 가벼움

    def set_state(self, status: str, percent: int = 0):
        self._status = str(status).lower()
        self.update()

    def _animate(self):
        self._frame += 1
        self.update()

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)

        w = self.width()
        h = self.height()
        cx = w / 2.0
        cy = h / 2.0
        base_size = min(w, h) * 0.35

        pal = self._PALETTES.get(self._status, self._PALETTES["idle"])

        # ── Bounce Animation ──
        if "error" in self._status:
            # 떨림
            bounce_y = math.sin(self._frame * 0.4) * 3
            bounce_x = math.cos(self._frame * 0.5) * 2
        elif "running" in self._status:
            # 빠른 통통
            bounce_y = abs(math.sin(self._frame * 0.12)) * base_size * 0.15
            bounce_x = 0
        elif "success" in self._status:
            # 환호 점프
            bounce_y = abs(math.sin(self._frame * 0.08)) * base_size * 0.2
            bounce_x = 0
        else:
            # 느린 둥둥
            bounce_y = math.sin(self._frame * 0.04) * base_size * 0.06
            bounce_x = 0

        body_cx = cx + bounce_x
        body_cy = cy - bounce_y

        # ── Soft Glow (배경 발광) ──
        glow_size = base_size * 2.2
        glow_color = QColor(pal["body"])
        glow_color.setAlpha(35)
        glow = QRadialGradient(body_cx, body_cy, glow_size)
        glow.setColorAt(0, glow_color)
        glow.setColorAt(1, QColor(0, 0, 0, 0))
        p.setBrush(glow)
        p.setPen(Qt.PenStyle.NoPen)
        p.drawEllipse(QRectF(body_cx - glow_size, body_cy - glow_size, glow_size * 2, glow_size * 2))

        # ── Body (둥근 슬라임) ──
        # 약간 세로로 눌린 타원
        bw = base_size * 1.1
        bh = base_size * 0.95
        body_rect = QRectF(body_cx - bw, body_cy - bh * 0.6, bw * 2, bh * 1.6)

        body_grad = QLinearGradient(body_cx, body_cy - bh, body_cx, body_cy + bh)
        lighter = QColor(pal["body"])
        lighter.setAlpha(255)
        darker = QColor(pal["body"].red() - 30, pal["body"].green() - 30, pal["body"].blue() - 10, 255)
        body_grad.setColorAt(0, lighter)
        body_grad.setColorAt(1, darker)

        p.setBrush(body_grad)
        p.setPen(Qt.PenStyle.NoPen)
        p.drawEllipse(body_rect)

        # ── Highlight (몸체 하이라이트) ──
        hl_size = base_size * 0.35
        hl_cx = body_cx - bw * 0.25
        hl_cy = body_cy - bh * 0.15
        hl_grad = QRadialGradient(hl_cx, hl_cy, hl_size)
        hl_grad.setColorAt(0, QColor(255, 255, 255, 90))
        hl_grad.setColorAt(1, QColor(255, 255, 255, 0))
        p.setBrush(hl_grad)
        p.drawEllipse(QRectF(hl_cx - hl_size, hl_cy - hl_size, hl_size * 2, hl_size * 2))

        # ── Eyes ──
        eye_y = body_cy + bh * 0.05
        eye_gap = bw * 0.32
        eye_size = base_size * 0.18

        self._draw_eye(p, body_cx - eye_gap, eye_y, eye_size, pal)
        self._draw_eye(p, body_cx + eye_gap, eye_y, eye_size, pal)

        # ── Mouth ──
        mouth_y = eye_y + eye_size * 1.8
        self._draw_mouth(p, body_cx, mouth_y, base_size * 0.12)

        # ── Cheeks (볼터치) ──
        cheek_y = eye_y + eye_size * 1.0
        cheek_size = base_size * 0.14
        cheek_x_offset = bw * 0.55

        p.setBrush(pal["cheek"])
        p.setPen(Qt.PenStyle.NoPen)
        p.drawEllipse(QRectF(body_cx - cheek_x_offset - cheek_size, cheek_y - cheek_size * 0.6, cheek_size * 2, cheek_size * 1.2))
        p.drawEllipse(QRectF(body_cx + cheek_x_offset - cheek_size, cheek_y - cheek_size * 0.6, cheek_size * 2, cheek_size * 1.2))

        # ── Antenna (안테나 + 별) ──
        ant_x = body_cx + bw * 0.15
        ant_bottom = body_cy - bh * 0.5
        ant_top = ant_bottom - base_size * 0.55
        # 안테나가 따라서 흔들림
        sway = math.sin(self._frame * 0.06) * base_size * 0.08

        pen = QPen(QColor(pal["body"].red() + 20, pal["body"].green() + 20, pal["body"].blue() + 30, 200), max(2, base_size * 0.04))
        pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        p.setPen(pen)
        p.drawLine(QPointF(ant_x, ant_bottom), QPointF(ant_x + sway, ant_top))

        # 별 (★)
        self._draw_star(p, ant_x + sway, ant_top - base_size * 0.06, base_size * 0.1)

        p.end()

    def _draw_eye(self, p: QPainter, cx: float, cy: float, size: float, pal: dict):
        """상태별 눈 그리기"""
        eye_color = pal["eye"]

        if "error" in self._status:
            # ㅠㅠ 눈 — 아래로 처진 반원
            p.setPen(QPen(eye_color, max(2, size * 0.2), Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            p.setBrush(Qt.BrushStyle.NoBrush)
            p.drawArc(QRectF(cx - size * 0.7, cy - size * 0.4, size * 1.4, size * 1.2), 0, 180 * 16)
        elif "success" in self._status:
            # ♥ 눈 — 하트 모양
            p.setPen(Qt.PenStyle.NoPen)
            p.setBrush(QColor(255, 100, 130))
            heart = QPainterPath()
            s = size * 0.8
            heart.moveTo(cx, cy + s * 0.5)
            heart.cubicTo(cx - s, cy - s * 0.3, cx - s * 0.5, cy - s, cx, cy - s * 0.4)
            heart.cubicTo(cx + s * 0.5, cy - s, cx + s, cy - s * 0.3, cx, cy + s * 0.5)
            p.drawPath(heart)
        elif "running" in self._status:
            # ★ 반짝 눈 — 빛나는 원
            p.setPen(Qt.PenStyle.NoPen)
            p.setBrush(eye_color)
            p.drawEllipse(QRectF(cx - size * 0.6, cy - size * 0.6, size * 1.2, size * 1.2))
            # 하이라이트 두 개
            p.setBrush(QColor(255, 255, 255, 230))
            p.drawEllipse(QRectF(cx - size * 0.35, cy - size * 0.4, size * 0.35, size * 0.35))
            p.setBrush(QColor(255, 255, 255, 160))
            p.drawEllipse(QRectF(cx + size * 0.05, cy + size * 0.05, size * 0.2, size * 0.2))
        else:
            # ◕ 기본 눈 — 큰 동그란 눈
            p.setPen(Qt.PenStyle.NoPen)
            p.setBrush(eye_color)
            p.drawEllipse(QRectF(cx - size * 0.55, cy - size * 0.55, size * 1.1, size * 1.1))
            # 하이라이트
            p.setBrush(QColor(255, 255, 255, 210))
            p.drawEllipse(QRectF(cx - size * 0.3, cy - size * 0.35, size * 0.35, size * 0.35))
            if "paused" in self._status:
                # 졸린 눈 — 반쯤 감긴 눈꺼풀
                lid_color = QColor(self._PALETTES["paused"]["body"])
                lid_color.setAlpha(180)
                p.setBrush(lid_color)
                p.drawRect(QRectF(cx - size * 0.6, cy - size * 0.6, size * 1.2, size * 0.55))

    def _draw_mouth(self, p: QPainter, cx: float, cy: float, size: float):
        """상태별 입 그리기"""
        if "success" in self._status:
            # 활짝 웃는 입
            p.setPen(QPen(QColor(80, 40, 60), max(1.5, size * 0.15), Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            p.setBrush(Qt.BrushStyle.NoBrush)
            p.drawArc(QRectF(cx - size * 1.5, cy - size * 1.2, size * 3, size * 2.4), -20 * 16, -140 * 16)
        elif "error" in self._status:
            # 울먹 입 — 물결 모양
            p.setPen(QPen(QColor(100, 40, 40), max(1.5, size * 0.12), Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            p.setBrush(Qt.BrushStyle.NoBrush)
            path = QPainterPath()
            path.moveTo(cx - size, cy)
            path.cubicTo(cx - size * 0.3, cy + size * 0.6, cx + size * 0.3, cy - size * 0.3, cx + size, cy)
            p.drawPath(path)
        else:
            # 기본 — 작은 미소
            p.setPen(QPen(QColor(80, 50, 80, 180), max(1.5, size * 0.12), Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            p.setBrush(Qt.BrushStyle.NoBrush)
            p.drawArc(QRectF(cx - size * 0.8, cy - size * 0.6, size * 1.6, size * 1.2), -30 * 16, -120 * 16)

    def _draw_star(self, p: QPainter, cx: float, cy: float, size: float):
        """안테나 위의 별"""
        # 반짝임 효과
        twinkle = 0.8 + math.sin(self._frame * 0.1) * 0.2
        s = size * twinkle

        star_color = QColor(255, 230, 140, 220)
        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(star_color)

        path = QPainterPath()
        for i in range(5):
            angle = math.radians(-90 + i * 72)
            inner_angle = math.radians(-90 + i * 72 + 36)
            if i == 0:
                path.moveTo(cx + s * math.cos(angle), cy + s * math.sin(angle))
            else:
                path.lineTo(cx + s * math.cos(angle), cy + s * math.sin(angle))
            path.lineTo(cx + s * 0.4 * math.cos(inner_angle), cy + s * 0.4 * math.sin(inner_angle))
        path.closeSubpath()
        p.drawPath(path)

        # 별 발광
        glow = QRadialGradient(cx, cy, s * 1.5)
        glow.setColorAt(0, QColor(255, 240, 180, 50))
        glow.setColorAt(1, QColor(255, 240, 180, 0))
        p.setBrush(glow)
        p.drawEllipse(QRectF(cx - s * 1.5, cy - s * 1.5, s * 3, s * 3))
