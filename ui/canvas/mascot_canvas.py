from __future__ import annotations

import math
from PySide6.QtCore import QTimer, Qt, QRectF, QPointF
from PySide6.QtGui import (
    QPainter, QColor, QRadialGradient, QPen,
    QLinearGradient, QPainterPath, QFont, QConicalGradient,
)
from PySide6.QtWidgets import QWidget, QSizePolicy


class MascotCanvas(QWidget):
    """
    Rezy — 초귀여운 카와이 슬라임 마스코트.
    통통한 물방울 몸 + 거대한 반짝이 눈 + 볼터치 + 표정 풍부.
    """

    # ── Color Palettes per state ──
    _PALETTES = {
        "idle": {
            "body_top": QColor(210, 185, 255),
            "body_bot": QColor(160, 130, 220),
            "cheek": QColor(255, 160, 190, 120),
            "eye": QColor(45, 25, 70),
            "iris": QColor(140, 100, 220),
            "star": QColor(255, 230, 140, 240),
        },
        "running": {
            "body_top": QColor(180, 160, 255),
            "body_bot": QColor(130, 100, 240),
            "cheek": QColor(255, 140, 200, 140),
            "eye": QColor(35, 20, 85),
            "iris": QColor(120, 80, 255),
            "star": QColor(160, 220, 255, 255),
        },
        "success": {
            "body_top": QColor(160, 235, 210),
            "body_bot": QColor(110, 200, 170),
            "cheek": QColor(255, 180, 200, 130),
            "eye": QColor(30, 65, 50),
            "iris": QColor(80, 200, 150),
            "star": QColor(255, 200, 255, 240),
        },
        "error": {
            "body_top": QColor(255, 170, 170),
            "body_bot": QColor(220, 120, 120),
            "cheek": QColor(255, 100, 100, 100),
            "eye": QColor(90, 25, 25),
            "iris": QColor(200, 80, 80),
            "star": QColor(255, 180, 140, 200),
        },
        "paused": {
            "body_top": QColor(190, 180, 210),
            "body_bot": QColor(150, 140, 170),
            "cheek": QColor(200, 170, 200, 80),
            "eye": QColor(70, 60, 85),
            "iris": QColor(130, 120, 160),
            "star": QColor(200, 200, 220, 160),
        },
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(160, 160)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self._status = "idle"
        self._frame = 0
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._animate)
        self._timer.start(1000 // 30)  # 30 FPS

    def set_state(self, status: str, percent: int = 0):
        self._status = str(status).lower()
        self.update()

    def _animate(self):
        self._frame += 1
        self.update()

    # ─────────────────────────────────────────────
    # Paint
    # ─────────────────────────────────────────────
    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)

        w = self.width()
        h = self.height()
        cx = w / 2.0
        cy = h / 2.0
        base = min(w, h) * 0.38  # 살짝 더 크게

        pal = self._PALETTES.get(self._status, self._PALETTES["idle"])

        # ── Bounce ──
        if "error" in self._status:
            by = math.sin(self._frame * 0.45) * 3
            bx = math.cos(self._frame * 0.55) * 2
            squish = 1.0
        elif "running" in self._status:
            phase = self._frame * 0.14
            by = abs(math.sin(phase)) * base * 0.18
            bx = 0
            squish = 1.0 - abs(math.sin(phase)) * 0.08
        elif "success" in self._status:
            by = abs(math.sin(self._frame * 0.09)) * base * 0.22
            bx = 0
            squish = 1.0 - abs(math.sin(self._frame * 0.09)) * 0.06
        else:
            by = math.sin(self._frame * 0.04) * base * 0.05
            bx = 0
            squish = 1.0

        bcx = cx + bx
        bcy = cy - by

        # ── Ambient Glow ──
        g_size = base * 2.5
        gc = QColor(pal["body_top"])
        gc.setAlpha(30)
        glow = QRadialGradient(bcx, bcy + base * 0.2, g_size)
        glow.setColorAt(0, gc)
        glow.setColorAt(1, QColor(0, 0, 0, 0))
        p.setBrush(glow)
        p.setPen(Qt.PenStyle.NoPen)
        p.drawEllipse(QRectF(bcx - g_size, bcy - g_size + base * 0.2, g_size * 2, g_size * 2))

        # ── Shadow (땅에 그림자) ──
        shadow_w = base * 0.9
        shadow_h = base * 0.12
        shadow_y = cy + base * 0.75
        p.setBrush(QColor(20, 15, 30, 50))
        p.drawEllipse(QRectF(cx - shadow_w, shadow_y, shadow_w * 2, shadow_h * 2))

        # ── Body (통통한 물방울 형태) ──
        bw = base * 1.15
        bh_base = base * 1.05 * squish
        bh_top = bh_base * 0.55
        bh_bottom = bh_base * 0.65

        body_path = QPainterPath()
        # 위쪽은 둥글게, 아래쪽은 약간 넓게 (물방울 느낌)
        body_path.moveTo(bcx, bcy - bh_top)
        body_path.cubicTo(
            bcx + bw * 1.15, bcy - bh_top * 0.7,
            bcx + bw * 1.2, bcy + bh_bottom * 0.4,
            bcx + bw * 0.85, bcy + bh_bottom
        )
        body_path.cubicTo(
            bcx + bw * 0.4, bcy + bh_bottom * 1.15,
            bcx - bw * 0.4, bcy + bh_bottom * 1.15,
            bcx - bw * 0.85, bcy + bh_bottom
        )
        body_path.cubicTo(
            bcx - bw * 1.2, bcy + bh_bottom * 0.4,
            bcx - bw * 1.15, bcy - bh_top * 0.7,
            bcx, bcy - bh_top
        )

        body_grad = QLinearGradient(bcx, bcy - bh_top, bcx, bcy + bh_bottom)
        body_grad.setColorAt(0.0, pal["body_top"])
        body_grad.setColorAt(0.6, pal["body_bot"])
        body_grad.setColorAt(1.0, QColor(
            max(0, pal["body_bot"].red() - 25),
            max(0, pal["body_bot"].green() - 25),
            max(0, pal["body_bot"].blue() - 10), 255
        ))

        p.setBrush(body_grad)
        p.setPen(Qt.PenStyle.NoPen)
        p.drawPath(body_path)

        # ── Body Highlight (윤기 있는 하이라이트) ──
        hl_w = base * 0.45
        hl_h = base * 0.35
        hl_cx = bcx - bw * 0.28
        hl_cy = bcy - bh_top * 0.15
        hl_grad = QRadialGradient(hl_cx, hl_cy, hl_w)
        hl_grad.setColorAt(0, QColor(255, 255, 255, 120))
        hl_grad.setColorAt(0.5, QColor(255, 255, 255, 40))
        hl_grad.setColorAt(1, QColor(255, 255, 255, 0))
        p.setBrush(hl_grad)
        p.drawEllipse(QRectF(hl_cx - hl_w, hl_cy - hl_h, hl_w * 2, hl_h * 2))

        # ── 작은 하이라이트 (오른쪽 상단) ──
        hl2_r = base * 0.1
        hl2_cx = bcx + bw * 0.35
        hl2_cy = bcy - bh_top * 0.25
        p.setBrush(QColor(255, 255, 255, 80))
        p.drawEllipse(QRectF(hl2_cx - hl2_r, hl2_cy - hl2_r, hl2_r * 2, hl2_r * 2))

        # ── Eyes (거대 반짝 눈) ──
        eye_y = bcy + bh_base * 0.02
        eye_gap = bw * 0.42
        eye_size = base * 0.28  # 훨씬 크게!
        self._draw_eye(p, bcx - eye_gap, eye_y, eye_size, pal)
        self._draw_eye(p, bcx + eye_gap, eye_y, eye_size, pal)

        # ── Mouth ──
        mouth_y = eye_y + eye_size * 1.6
        self._draw_mouth(p, bcx, mouth_y, base * 0.13)

        # ── Cheeks (큰 볼터치) ──
        cheek_y = eye_y + eye_size * 0.9
        cheek_w = base * 0.2
        cheek_h = base * 0.11
        cheek_x_off = bw * 0.7

        cheek_grad_l = QRadialGradient(bcx - cheek_x_off, cheek_y, cheek_w)
        cheek_grad_l.setColorAt(0, pal["cheek"])
        cheek_grad_l.setColorAt(1, QColor(pal["cheek"].red(), pal["cheek"].green(), pal["cheek"].blue(), 0))
        p.setBrush(cheek_grad_l)
        p.drawEllipse(QRectF(bcx - cheek_x_off - cheek_w, cheek_y - cheek_h, cheek_w * 2, cheek_h * 2))

        cheek_grad_r = QRadialGradient(bcx + cheek_x_off, cheek_y, cheek_w)
        cheek_grad_r.setColorAt(0, pal["cheek"])
        cheek_grad_r.setColorAt(1, QColor(pal["cheek"].red(), pal["cheek"].green(), pal["cheek"].blue(), 0))
        p.setBrush(cheek_grad_r)
        p.drawEllipse(QRectF(bcx + cheek_x_off - cheek_w, cheek_y - cheek_h, cheek_w * 2, cheek_h * 2))

        # ── Tiny Arms (짧은 팔) ──
        self._draw_arms(p, bcx, bcy, bw, bh_bottom, base, pal)

        # ── Antenna (안테나 + 별) ──
        ant_x = bcx + bw * 0.12
        ant_bottom = bcy - bh_top * 0.9
        ant_top = ant_bottom - base * 0.5
        sway = math.sin(self._frame * 0.07) * base * 0.1

        pen_color = QColor(pal["body_top"])
        pen_color.setAlpha(200)
        pen = QPen(pen_color, max(2, base * 0.04))
        pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        p.setPen(pen)
        # 약간 휘어진 안테나 (곡선으로)
        ant_path = QPainterPath()
        ant_path.moveTo(ant_x, ant_bottom)
        ant_path.cubicTo(
            ant_x + sway * 0.3, ant_bottom - base * 0.2,
            ant_x + sway * 0.8, ant_top + base * 0.15,
            ant_x + sway, ant_top,
        )
        p.setBrush(Qt.BrushStyle.NoBrush)
        p.drawPath(ant_path)

        # 별 (★)
        self._draw_star(p, ant_x + sway, ant_top - base * 0.05, base * 0.12, pal)

        # ── 반짝이 파티클 (running/success 일 때) ──
        if self._status in ("running", "success"):
            self._draw_sparkles(p, bcx, bcy, base, pal)

        p.end()

    # ─────────────────────────────────────────────
    # Eyes — 크고 반짝이는 애니메이션 눈
    # ─────────────────────────────────────────────
    def _draw_eye(self, p: QPainter, cx: float, cy: float, size: float, pal: dict):
        if "error" in self._status:
            # ㅠㅠ 울먹 눈 — 아래로 처진 반원 + 눈물
            p.setPen(QPen(pal["eye"], max(2, size * 0.18), Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            p.setBrush(Qt.BrushStyle.NoBrush)
            p.drawArc(QRectF(cx - size * 0.8, cy - size * 0.5, size * 1.6, size * 1.4), 10 * 16, 160 * 16)
            # 눈물
            tear_y = cy + size * 0.6
            tear_alpha = int(140 + math.sin(self._frame * 0.15) * 60)
            p.setPen(Qt.PenStyle.NoPen)
            p.setBrush(QColor(140, 200, 255, max(0, min(255, tear_alpha))))
            p.drawEllipse(QRectF(cx + size * 0.3, tear_y, size * 0.25, size * 0.35))
            return

        if "success" in self._status:
            # ♥ 하트 눈
            p.setPen(Qt.PenStyle.NoPen)
            p.setBrush(QColor(255, 90, 130))
            s = size * 0.85
            heart = QPainterPath()
            heart.moveTo(cx, cy + s * 0.5)
            heart.cubicTo(cx - s * 1.1, cy - s * 0.2, cx - s * 0.5, cy - s * 1.0, cx, cy - s * 0.35)
            heart.cubicTo(cx + s * 0.5, cy - s * 1.0, cx + s * 1.1, cy - s * 0.2, cx, cy + s * 0.5)
            p.drawPath(heart)
            return

        # 기본 눈 — 큰 동그란 눈 + iris + 큰 하이라이트
        # 눈 흰자
        p.setPen(Qt.PenStyle.NoPen)
        white_w = size * 1.1
        white_h = size * 1.15
        p.setBrush(QColor(255, 255, 255, 245))
        p.drawEllipse(QRectF(cx - white_w * 0.55, cy - white_h * 0.55, white_w, white_h))

        # 홍채 (iris)
        iris_r = size * 0.65
        # 약간 시선 이동
        look_x = math.sin(self._frame * 0.025) * size * 0.06
        look_y = math.cos(self._frame * 0.02) * size * 0.04
        iris_cx = cx + look_x
        iris_cy = cy + look_y + size * 0.05

        iris_grad = QRadialGradient(iris_cx, iris_cy - iris_r * 0.3, iris_r)
        iris_grad.setColorAt(0.0, pal["iris"])
        iris_grad.setColorAt(0.7, pal["eye"])
        iris_grad.setColorAt(1.0, QColor(pal["eye"].red(), pal["eye"].green(), pal["eye"].blue(), 255))
        p.setBrush(iris_grad)
        p.drawEllipse(QRectF(iris_cx - iris_r, iris_cy - iris_r, iris_r * 2, iris_r * 2))

        # 동공 (pupil)
        pupil_r = iris_r * 0.55
        p.setBrush(QColor(15, 8, 25))
        p.drawEllipse(QRectF(iris_cx - pupil_r, iris_cy - pupil_r, pupil_r * 2, pupil_r * 2))

        # 큰 하이라이트 (왼쪽 위)
        hl_r = size * 0.3
        p.setBrush(QColor(255, 255, 255, 240))
        p.drawEllipse(QRectF(cx - size * 0.32, cy - size * 0.38, hl_r, hl_r))

        # 작은 하이라이트 (오른쪽 아래)
        hl2_r = size * 0.15
        p.setBrush(QColor(255, 255, 255, 200))
        p.drawEllipse(QRectF(cx + size * 0.12, cy + size * 0.1, hl2_r, hl2_r))

        # Paused → 졸린 눈꺼풀 (반쯤 감김)
        if "paused" in self._status:
            lid_color = QColor(pal["body_top"])
            lid_color.setAlpha(200)
            p.setBrush(lid_color)
            blink_h = size * 0.5 + math.sin(self._frame * 0.03) * size * 0.1
            p.drawEllipse(QRectF(cx - white_w * 0.6, cy - white_h * 0.65, white_w * 1.2, blink_h))

        # Running → 반짝이는 별 하이라이트
        if "running" in self._status:
            star_alpha = int(180 + math.sin(self._frame * 0.2) * 75)
            self._draw_mini_star(p, cx - size * 0.2, cy - size * 0.25, size * 0.12, star_alpha)

    # ─────────────────────────────────────────────
    # Mouth
    # ─────────────────────────────────────────────
    def _draw_mouth(self, p: QPainter, cx: float, cy: float, size: float):
        if "success" in self._status:
            # 활짝 웃는 D자 입
            p.setPen(Qt.PenStyle.NoPen)
            p.setBrush(QColor(80, 40, 50, 180))
            mouth = QPainterPath()
            mouth.moveTo(cx - size * 1.2, cy)
            mouth.cubicTo(
                cx - size * 0.8, cy + size * 2.5,
                cx + size * 0.8, cy + size * 2.5,
                cx + size * 1.2, cy,
            )
            mouth.cubicTo(
                cx + size * 0.6, cy + size * 0.5,
                cx - size * 0.6, cy + size * 0.5,
                cx - size * 1.2, cy,
            )
            p.drawPath(mouth)
            # 혀
            p.setBrush(QColor(240, 130, 140))
            p.drawEllipse(QRectF(cx - size * 0.4, cy + size * 0.6, size * 0.8, size * 0.7))
        elif "error" in self._status:
            # 울먹 물결 입
            p.setPen(QPen(QColor(100, 40, 40, 200), max(1.5, size * 0.13), Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            p.setBrush(Qt.BrushStyle.NoBrush)
            path = QPainterPath()
            path.moveTo(cx - size, cy)
            path.cubicTo(cx - size * 0.3, cy + size * 0.7, cx + size * 0.3, cy - size * 0.4, cx + size, cy + size * 0.2)
            p.drawPath(path)
        elif "running" in self._status:
            # 집중 — 작은 'ω' 입
            p.setPen(QPen(QColor(60, 30, 70, 200), max(1.5, size * 0.12), Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            p.setBrush(Qt.BrushStyle.NoBrush)
            w_path = QPainterPath()
            w_path.moveTo(cx - size * 0.7, cy)
            w_path.cubicTo(cx - size * 0.4, cy + size * 0.8, cx, cy - size * 0.2, cx, cy + size * 0.3)
            w_path.cubicTo(cx, cy - size * 0.2, cx + size * 0.4, cy + size * 0.8, cx + size * 0.7, cy)
            p.drawPath(w_path)
        else:
            # 기본 — 부드러운 미소
            p.setPen(QPen(QColor(70, 40, 70, 160), max(1.5, size * 0.12), Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            p.setBrush(Qt.BrushStyle.NoBrush)
            p.drawArc(QRectF(cx - size * 0.9, cy - size * 0.7, size * 1.8, size * 1.4), -30 * 16, -120 * 16)

    # ─────────────────────────────────────────────
    # Arms (짧고 귀여운 팔)
    # ─────────────────────────────────────────────
    def _draw_arms(self, p: QPainter, cx: float, cy: float, bw: float, bh: float, base: float, pal: dict):
        arm_color = QColor(pal["body_bot"])
        arm_color.setAlpha(220)
        pen = QPen(arm_color, max(3, base * 0.06))
        pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        p.setPen(pen)
        p.setBrush(Qt.BrushStyle.NoBrush)

        sway = math.sin(self._frame * 0.06) * base * 0.04

        # 왼팔
        arm_l = QPainterPath()
        lx = cx - bw * 0.95
        ly = cy + bh * 0.15
        arm_l.moveTo(lx, ly)
        arm_l.cubicTo(lx - base * 0.2 + sway, ly + base * 0.15, lx - base * 0.25, ly + base * 0.3, lx - base * 0.15 + sway, ly + base * 0.35)
        p.drawPath(arm_l)

        # 오른팔
        arm_r = QPainterPath()
        rx = cx + bw * 0.95
        ry = cy + bh * 0.15
        arm_r.moveTo(rx, ry)
        arm_r.cubicTo(rx + base * 0.2 - sway, ry + base * 0.15, rx + base * 0.25, ry + base * 0.3, rx + base * 0.15 - sway, ry + base * 0.35)
        p.drawPath(arm_r)

    # ─────────────────────────────────────────────
    # Star (안테나 위 별)
    # ─────────────────────────────────────────────
    def _draw_star(self, p: QPainter, cx: float, cy: float, size: float, pal: dict):
        twinkle = 0.85 + math.sin(self._frame * 0.12) * 0.15
        s = size * twinkle

        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(pal["star"])

        path = QPainterPath()
        for i in range(5):
            angle = math.radians(-90 + i * 72)
            inner_angle = math.radians(-90 + i * 72 + 36)
            if i == 0:
                path.moveTo(cx + s * math.cos(angle), cy + s * math.sin(angle))
            else:
                path.lineTo(cx + s * math.cos(angle), cy + s * math.sin(angle))
            path.lineTo(cx + s * 0.38 * math.cos(inner_angle), cy + s * 0.38 * math.sin(inner_angle))
        path.closeSubpath()
        p.drawPath(path)

        # 별 발광
        glow = QRadialGradient(cx, cy, s * 2)
        glow.setColorAt(0, QColor(pal["star"].red(), pal["star"].green(), pal["star"].blue(), 60))
        glow.setColorAt(1, QColor(pal["star"].red(), pal["star"].green(), pal["star"].blue(), 0))
        p.setBrush(glow)
        p.drawEllipse(QRectF(cx - s * 2, cy - s * 2, s * 4, s * 4))

    # ─────────────────────────────────────────────
    # Mini Star (눈 속 별 하이라이트)
    # ─────────────────────────────────────────────
    def _draw_mini_star(self, p: QPainter, cx: float, cy: float, size: float, alpha: int):
        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(QColor(255, 255, 255, max(0, min(255, alpha))))
        path = QPainterPath()
        for i in range(4):
            angle = math.radians(self._frame * 2 + i * 90)
            inner = math.radians(self._frame * 2 + i * 90 + 45)
            if i == 0:
                path.moveTo(cx + size * math.cos(angle), cy + size * math.sin(angle))
            else:
                path.lineTo(cx + size * math.cos(angle), cy + size * math.sin(angle))
            path.lineTo(cx + size * 0.3 * math.cos(inner), cy + size * 0.3 * math.sin(inner))
        path.closeSubpath()
        p.drawPath(path)

    # ─────────────────────────────────────────────
    # Sparkles (반짝이 파티클)
    # ─────────────────────────────────────────────
    def _draw_sparkles(self, p: QPainter, cx: float, cy: float, base: float, pal: dict):
        p.setPen(Qt.PenStyle.NoPen)
        for i in range(6):
            angle = self._frame * 0.02 + i * math.pi / 3
            dist = base * (1.3 + math.sin(self._frame * 0.08 + i * 1.5) * 0.3)
            sx = cx + math.cos(angle) * dist
            sy = cy + math.sin(angle) * dist * 0.6
            alpha = int(100 + math.sin(self._frame * 0.15 + i * 2) * 80)
            s_size = base * (0.04 + math.sin(self._frame * 0.1 + i) * 0.02)
            p.setBrush(QColor(255, 255, 255, max(0, min(255, alpha))))
            p.drawEllipse(QRectF(sx - s_size, sy - s_size, s_size * 2, s_size * 2))
