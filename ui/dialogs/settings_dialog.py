from __future__ import annotations

import json
import re
import shutil
import threading
import webbrowser
from dataclasses import dataclass
from pathlib import Path

import requests
import yaml
from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QScrollArea,
    QSpinBox,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from ui.widgets.glass_card import GlassCard
from ui.widgets.motion_button import MotionButton
from core.ollama_manager import OllamaManager
from core.settings import LocalLLMSettings


@dataclass
class SettingsDialogContext:
    root: Path
    settings_path: Path
    client_secrets_path: Path
    blogger_token_path: Path
    gemini_usage_dashboard: str
    gemini_rate_limit_doc: str
    gemini_pricing_doc: str
    gemini_quota_console: str


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _save_yaml(path: Path, data: dict) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")


def _load_ui_preferences(root: Path) -> dict:
    path = root / "storage" / "ui" / "preferences.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _save_ui_preferences(root: Path, payload: dict) -> None:
    path = root / "storage" / "ui" / "preferences.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _nested_get(data: dict, dotted: str) -> str:
    cur = data
    parts = dotted.split(".")
    for p in parts[:-1]:
        cur = cur.get(p, {}) if isinstance(cur, dict) else {}
    val = cur.get(parts[-1], "") if isinstance(cur, dict) else ""
    return str(val)


def _nested_set(data: dict, dotted: str, value) -> None:
    cur = data
    parts = dotted.split(".")
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value


def _is_valid_gemini_key(value: str) -> bool:
    return bool(re.fullmatch(r"AIza[0-9A-Za-z_-]{20,}", value.strip()))


def _is_valid_blogger_blog_id(value: str) -> bool:
    return bool(re.fullmatch(r"\d{8,30}", value.strip()))


def _validate_blogger_token_file(root: Path, path_value: str) -> tuple[bool, str]:
    p = Path(path_value)
    if not p.is_absolute():
        p = root / p
    if not p.exists():
        return False, "blogger_token.json 파일을 찾을 수 없습니다."
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return False, "blogger_token.json 형식이 올바른 JSON이 아닙니다."
    required = ["client_id", "client_secret", "refresh_token", "token_uri"]
    missing = [k for k in required if not data.get(k)]
    if missing:
        return False, f"blogger_token.json 필수 항목 누락: {', '.join(missing)}"
    return True, ""


class SettingsDialog(QDialog):
    cache_size_ready = Signal(str)

    def __init__(self, context: SettingsDialogContext, on_saved=None, required_only: bool = False):
        super().__init__()
        self.context = context
        self.on_saved = on_saved
        self.required_only = required_only
        self.data = _load_yaml(self.context.settings_path)
        self.pref = _load_ui_preferences(self.context.root)
        self.setWindowTitle("RezeroAgent 설정")
        self.resize(1120, 780)
        self.setMinimumSize(980, 680)
        self._conn_grid = None
        self._conn_buttons: list[MotionButton] = []
        self._build_ui()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        header = GlassCard()
        h = QVBoxLayout(header)
        title = QLabel("연결 및 자동화 설정")
        title.setObjectName("Title")
        sub = QLabel("연결 방식을 선택하세요: JSON 업로드 / Google 로그인 / 토큰 직접 연결")
        sub.setObjectName("Subtitle")
        h.addWidget(title)
        h.addWidget(sub)
        root.addWidget(header)

        conn = GlassCard()
        ch = QGridLayout(conn)
        ch.setContentsMargins(14, 12, 14, 12)
        ch.setHorizontalSpacing(10)
        ch.setVerticalSpacing(10)
        self._conn_grid = ch

        b1 = MotionButton("JSON 업로드")
        b1.clicked.connect(self.upload_client_secret)
        b2 = MotionButton("Google 로그인")
        b2.clicked.connect(self.google_login)
        b3 = MotionButton("토큰 직접 연결")
        b3.clicked.connect(self.browse_token)
        b4 = MotionButton("모델 테스트")
        b4.clicked.connect(self.test_models)
        b5 = MotionButton("무료 쿼터 안내")
        b5.clicked.connect(self.show_quota_guide)

        self._conn_buttons = [b1, b2, b3, b4, b5]
        for idx, b in enumerate(self._conn_buttons):
            ch.addWidget(b, idx // 3, idx % 3)
        ch.setColumnStretch(0, 1)
        ch.setColumnStretch(1, 1)
        ch.setColumnStretch(2, 1)
        root.addWidget(conn)

        body = GlassCard()
        body_layout = QHBoxLayout(body)
        body_layout.setContentsMargins(10, 10, 10, 10)
        body_layout.setSpacing(10)

        self.tab_list = QListWidget()
        self.tab_list.setFixedWidth(230)
        self.tab_list.currentRowChanged.connect(self._on_tab_changed)
        body_layout.addWidget(self.tab_list, 0)

        self.stacked = QStackedWidget()
        body_layout.addWidget(self.stacked, 1)
        self._build_tabs()
        root.addWidget(body, 1)

        actions = QHBoxLayout()
        actions.addStretch(1)
        cancel = MotionButton("취소")
        save = MotionButton("저장")
        save.setObjectName("PrimaryBtn")
        cancel.clicked.connect(self.reject)
        save.clicked.connect(self.save)
        actions.addWidget(cancel)
        actions.addWidget(save)
        root.addLayout(actions)
        self._apply_settings_responsive_layout()

        if self.tab_list.count() > 0:
            self.tab_list.setCurrentRow(0)

    def _build_tabs(self) -> None:
        self._build_model_tab()
        self._build_image_tab()
        self._build_blogger_tab()
        self._build_indexing_tab()
        self._build_automation_tab()
        self._build_advanced_tab()

    def _add_tab(self, title: str, widget: QWidget) -> None:
        self.tab_list.addItem(QListWidgetItem(title))
        self.stacked.addWidget(widget)

    def _build_model_tab(self) -> None:
        page = QWidget()
        form = QFormLayout(page)
        form.setSpacing(10)
        self.gemini_key = QLineEdit(_nested_get(self.data, "gemini.api_key"))
        self.model_combo = QComboBox()
        self.model_combo.setEditable(False)
        current_model = _nested_get(self.data, "gemini.model").strip() or "gemini-2.0-flash"
        self.model_combo.addItem(current_model)
        model_box = QWidget()
        model_row = QHBoxLayout(model_box)
        model_row.setContentsMargins(0, 0, 0, 0)
        model_row.setSpacing(8)
        btn_models = MotionButton("모델 목록 불러오기")
        btn_models.clicked.connect(self.test_models)
        model_row.addWidget(self.model_combo, 1)
        model_row.addWidget(btn_models)
        self.free_mode = QCheckBox()
        self.free_mode.setChecked(_nested_get(self.data, "budget.free_mode").strip().lower() in {"1", "true", "yes", "on"})
        self.dry_run = QCheckBox()
        self.dry_run.setChecked(_nested_get(self.data, "budget.dry_run").strip().lower() in {"1", "true", "yes", "on"})
        form.addRow("Gemini API Key", self.gemini_key)
        form.addRow("Gemini 모델", model_box)
        form.addRow("Free Mode (비용 0원)", self.free_mode)
        form.addRow("Dry Run", self.dry_run)
        self._add_tab("모델", self._wrap_scroll(page))

    def _build_image_tab(self) -> None:
        page = QWidget()
        form = QFormLayout(page)
        form.setSpacing(10)
        self.image_provider = QComboBox()
        self.image_provider.addItems(["library"])
        self.image_provider.setCurrentText("library")
        self.image_provider.setEnabled(False)
        self.enable_img = QCheckBox()
        self.enable_img.setChecked(False)
        self.enable_img.setEnabled(False)
        self.gemini_image_model = QLineEdit(
            (_nested_get(self.data, "visual.gemini_image_model") or "models/imagen-3.0-generate-001").strip()
        )
        self.gemini_image_model.setPlaceholderText("models/imagen-3.0-generate-001")
        target_default = (_nested_get(self.data, "visual.target_images_per_post") or "5").strip()
        if not target_default.isdigit():
            target_default = "5"
        self.target_images = QLineEdit(target_default)
        self.target_images.setReadOnly(True)
        form.addRow("이미지 공급자", self.image_provider)
        form.addRow("이미지 생성 사용 여부", self.enable_img)
        form.addRow("Gemini 이미지 모델", self.gemini_image_model)
        form.addRow("게시글당 목표 이미지", self.target_images)
        self._add_tab("이미지", self._wrap_scroll(page))

    def _build_blogger_tab(self) -> None:
        page = QWidget()
        form = QFormLayout(page)
        form.setSpacing(10)
        self.blog_id = QLineEdit(_nested_get(self.data, "blogger.blog_id"))
        self.token_path = QLineEdit(_nested_get(self.data, "blogger.credentials_path") or "config/blogger_token.json")
        form.addRow("Blogger Blog ID", self.blog_id)
        form.addRow("blogger_token.json", self.token_path)
        self._add_tab("블로거", self._wrap_scroll(page))

    def _build_indexing_tab(self) -> None:
        page = QWidget()
        form = QFormLayout(page)
        form.setSpacing(10)
        self.search_console_enabled = QCheckBox()
        self.search_console_enabled.setChecked(
            _nested_get(self.data, "integrations.search_console_enabled").strip().lower() in {"1", "true", "yes", "on"}
        )
        self.search_console_site_url = QLineEdit((_nested_get(self.data, "integrations.search_console_site_url") or "").strip())
        self.search_console_site_url.setPlaceholderText("예: https://yourblog.com/")
        form.addRow("Search Console 연동", self.search_console_enabled)
        form.addRow("Search Console 사이트 URL", self.search_console_site_url)
        self._add_tab("인덱싱", self._wrap_scroll(page))
    def _build_automation_tab(self) -> None:
        page = QWidget()
        form = QFormLayout(page)
        form.setSpacing(10)

        self.posts_to_generate_per_day = QSpinBox()
        self.posts_to_generate_per_day.setRange(1, 10)
        self.posts_to_generate_per_day.setValue(int(_nested_get(self.data, "publishing.posts_to_generate_per_day") or "3"))

        self.posts_to_publish_per_day = QSpinBox()
        self.posts_to_publish_per_day.setRange(0, 10)
        self.posts_to_publish_per_day.setValue(int(_nested_get(self.data, "publishing.posts_to_publish_per_day") or "2"))

        self.preset_combo = QComboBox()
        self.preset_combo.addItems(["Safe", "Balanced", "Aggressive"])
        self.preset_combo.setCurrentText("Balanced")
        self.preset_combo.currentTextChanged.connect(self._apply_publishing_preset)

        self.buffer_target_days = QSpinBox(); self.buffer_target_days.setRange(1, 14)
        self.buffer_min_days = QSpinBox(); self.buffer_min_days.setRange(1, 14)
        self.buffer_target_days.setValue(int(_nested_get(self.data, "publishing.buffer_target_days") or "5"))
        self.buffer_min_days.setValue(int(_nested_get(self.data, "publishing.buffer_min_days") or "3"))

        self.time_window_start = QLineEdit((_nested_get(self.data, "publishing.time_window_start") or "09:00").strip())
        self.time_window_end = QLineEdit((_nested_get(self.data, "publishing.time_window_end") or "23:00").strip())

        self.randomness_level = QComboBox(); self.randomness_level.addItems(["low", "medium", "high"])
        self.randomness_level.setCurrentText((_nested_get(self.data, "publishing.randomness_level") or "medium").strip().lower())

        self.min_gap_minutes = QSpinBox(); self.min_gap_minutes.setRange(30, 720)
        self.min_gap_minutes.setValue(int(_nested_get(self.data, "publishing.min_gap_minutes") or "180"))

        self.quiet_hours_enabled = QCheckBox()
        self.quiet_hours_enabled.setChecked(_nested_get(self.data, "publishing.quiet_hours_enabled").strip().lower() in {"1", "true", "yes", "on"})
        self.quiet_hours_start = QLineEdit((_nested_get(self.data, "publishing.quiet_hours_start") or "02:00").strip())
        self.quiet_hours_end = QLineEdit((_nested_get(self.data, "publishing.quiet_hours_end") or "07:00").strip())

        self.monthly_rotation_enabled = QCheckBox()
        self.monthly_rotation_enabled.setChecked(_nested_get(self.data, "topics.monthly_rotation_enabled").strip().lower() not in {"0", "false", "no", "off"})
        self.rotation_order = QLineEdit((_nested_get(self.data, "topics.rotation_order") or "windows,mac,iphone,galaxy").replace("[", "").replace("]", "").replace("'", ""))

        self.enforce_english_only = QCheckBox()
        self.enforce_english_only.setChecked(_nested_get(self.data, "content.enforce_english_only").strip().lower() not in {"0", "false", "no", "off"})

        self.integrations_enabled = QCheckBox()
        self.integrations_enabled.setChecked(_nested_get(self.data, "integrations.enabled").strip().lower() in {"1", "true", "yes", "on"})
        self.adsense_enabled = QCheckBox()
        self.adsense_enabled.setChecked(_nested_get(self.data, "integrations.adsense_enabled").strip().lower() in {"1", "true", "yes", "on"})
        self.analytics_enabled = QCheckBox()
        self.analytics_enabled.setChecked(_nested_get(self.data, "integrations.analytics_enabled").strip().lower() in {"1", "true", "yes", "on"})
        self.ga4_property_id = QLineEdit((_nested_get(self.data, "integrations.ga4_property_id") or "").strip())
        self.ga4_property_id.setPlaceholderText("예: 123456789")
        self.integrations_refresh = QSpinBox()
        self.integrations_refresh.setRange(3, 180)
        self.integrations_refresh.setValue(int(_nested_get(self.data, "integrations.refresh_minutes") or "15"))

        self.cache_size_label = QLabel("계산 중...")
        self.cache_size_label.setObjectName("Subtitle")
        self.cache_size_ready.connect(self.cache_size_label.setText)
        self.clear_cache_btn = MotionButton("캐시 비우기")
        self.clear_cache_btn.clicked.connect(self._clear_image_cache)

        form.addRow("일일 생성 글 수", self.posts_to_generate_per_day)
        form.addRow("일일 발행 글 수", self.posts_to_publish_per_day)
        form.addRow("프리셋", self.preset_combo)
        form.addRow("버퍼 목표(일)", self.buffer_target_days)
        form.addRow("버퍼 최소(일)", self.buffer_min_days)
        form.addRow("발행 가능 시작", self.time_window_start)
        form.addRow("발행 가능 종료", self.time_window_end)
        form.addRow("랜덤 강도", self.randomness_level)
        form.addRow("최소 간격(분)", self.min_gap_minutes)
        form.addRow("Quiet hours 사용", self.quiet_hours_enabled)
        form.addRow("Quiet 시작", self.quiet_hours_start)
        form.addRow("Quiet 종료", self.quiet_hours_end)
        form.addRow("월별 디바이스 로테이션", self.monthly_rotation_enabled)
        form.addRow("로테이션 순서(콤마)", self.rotation_order)
        form.addRow("영어만 허용", self.enforce_english_only)

        cache_row = QWidget()
        cache_layout = QHBoxLayout(cache_row)
        cache_layout.setContentsMargins(0, 0, 0, 0)
        cache_layout.setSpacing(8)
        cache_layout.addWidget(self.cache_size_label, 1)
        cache_layout.addWidget(self.clear_cache_btn)
        form.addRow("이미지 캐시", cache_row)

        form.addRow("통합 API 위젯 사용", self.integrations_enabled)
        form.addRow("AdSense 연동", self.adsense_enabled)
        form.addRow("Analytics 연동", self.analytics_enabled)
        form.addRow("GA4 Property ID", self.ga4_property_id)
        form.addRow("API 위젯 갱신(분)", self.integrations_refresh)
        self._refresh_cache_size_async()
        self._add_tab("자동화", self._wrap_scroll(page))

    def _apply_publishing_preset(self, name: str) -> None:
        key = str(name or "").strip().lower()
        if key == "safe":
            self.posts_to_generate_per_day.setValue(2)
            self.posts_to_publish_per_day.setValue(1)
            self.randomness_level.setCurrentText("low")
            self.min_gap_minutes.setValue(240)
        elif key == "aggressive":
            self.posts_to_generate_per_day.setValue(5)
            self.posts_to_publish_per_day.setValue(3)
            self.randomness_level.setCurrentText("high")
            self.min_gap_minutes.setValue(120)
        else:
            self.posts_to_generate_per_day.setValue(3)
            self.posts_to_publish_per_day.setValue(2)
            self.randomness_level.setCurrentText("medium")
            self.min_gap_minutes.setValue(180)

    def _cache_size_text(self) -> str:
        cache_dir = self.context.root / "storage" / "image_cache"
        total = 0
        if cache_dir.exists():
            for p in cache_dir.rglob("*"):
                try:
                    if p.is_file():
                        total += int(p.stat().st_size)
                except Exception:
                    continue
        return f"{(total / (1024 * 1024)):.1f} MB"

    def _clear_image_cache(self) -> None:
        cache_dir = self.context.root / "storage" / "image_cache"
        removed = 0
        if cache_dir.exists():
            for p in cache_dir.rglob("*"):
                try:
                    if p.is_file():
                        p.unlink(missing_ok=True)
                        removed += 1
                except Exception:
                    continue
        self.cache_size_label.setText("0.0 MB")
        self._refresh_cache_size_async()
        QMessageBox.information(self, "캐시 정리", f"{removed}개 파일을 삭제했습니다.")

    def _refresh_cache_size_async(self) -> None:
        def worker() -> None:
            text = self._cache_size_text()
            self.cache_size_ready.emit(text)

        threading.Thread(target=worker, daemon=True).start()
    def _build_advanced_tab(self) -> None:
        page = QWidget()
        form = QFormLayout(page)
        form.setSpacing(10)
        self.animation_intensity = QComboBox()
        self.animation_intensity.addItems(["high", "medium", "off"])
        current_intensity = str((self.pref or {}).get("animation_intensity", "high") or "high").strip().lower()
        if current_intensity not in {"high", "medium", "off"}:
            current_intensity = "high"
        self.animation_intensity.setCurrentText(current_intensity)
        info = QLabel(
            "UI 애니메이션 강도\n"
            "high: 고감도 모션 / medium: 절충 / off: 모션 끔"
        )
        info.setWordWrap(True)
        info.setObjectName("Subtitle")
        form.addRow("애니메이션 강도", self.animation_intensity)
        form.addRow("", info)

        self.local_llm_enabled = QCheckBox()
        self.local_llm_enabled.setChecked(
            _nested_get(self.data, "local_llm.enabled").strip().lower() not in {"0", "false", "no", "off"}
        )
        self.local_llm_model = QLineEdit((_nested_get(self.data, "local_llm.model") or "qwen2.5:3b").strip())
        self.local_llm_base_url = QLineEdit((_nested_get(self.data, "local_llm.base_url") or "http://127.0.0.1:11434").strip())
        self.local_llm_status = QLabel("대기")
        self.local_llm_status.setObjectName("Subtitle")

        llm_btn_box = QWidget()
        llm_btn_row = QHBoxLayout(llm_btn_box)
        llm_btn_row.setContentsMargins(0, 0, 0, 0)
        llm_btn_row.setSpacing(8)
        self.btn_install_ollama = MotionButton("Install Ollama")
        self.btn_pull_ollama = MotionButton("Pull model")
        self.btn_test_ollama = MotionButton("Test local LLM")
        self.btn_install_ollama.clicked.connect(self._ollama_install)
        self.btn_pull_ollama.clicked.connect(self._ollama_pull_model)
        self.btn_test_ollama.clicked.connect(self._ollama_test)
        llm_btn_row.addWidget(self.btn_install_ollama)
        llm_btn_row.addWidget(self.btn_pull_ollama)
        llm_btn_row.addWidget(self.btn_test_ollama)

        form.addRow("Local LLM 사용", self.local_llm_enabled)
        form.addRow("Local LLM 모델", self.local_llm_model)
        form.addRow("Local LLM URL", self.local_llm_base_url)
        form.addRow("Local LLM 상태", self.local_llm_status)
        form.addRow("Local LLM 관리", llm_btn_box)
        self._add_tab("고급", self._wrap_scroll(page))

    def _local_llm_settings_from_ui(self) -> LocalLLMSettings:
        return LocalLLMSettings(
            enabled=bool(getattr(self, "local_llm_enabled", QCheckBox()).isChecked()),
            provider="ollama",
            model=str(getattr(self, "local_llm_model", QLineEdit("qwen2.5:3b")).text() or "qwen2.5:3b").strip(),
            base_url=str(getattr(self, "local_llm_base_url", QLineEdit("http://127.0.0.1:11434")).text() or "http://127.0.0.1:11434").strip(),
            num_ctx=2048,
            num_thread=2,
            max_loaded_models=1,
            num_parallel=1,
            install_if_missing=True,
            pull_model_if_missing=True,
            request_timeout_sec=60,
            max_calls_per_post=2,
        )

    def _make_ollama_manager(self) -> OllamaManager:
        return OllamaManager(
            root=self.context.root,
            settings=self._local_llm_settings_from_ui(),
            log_path=self.context.root / "storage" / "logs" / "ollama_manager.jsonl",
        )

    def _ollama_install(self) -> None:
        manager = self._make_ollama_manager()
        ok, reason = manager.install_if_needed()
        if ok:
            self.local_llm_status.setText("Installed")
            QMessageBox.information(self, "Ollama", "Ollama 설치 확인 완료")
            return
        self.local_llm_status.setText(f"Install failed: {reason}")
        QMessageBox.warning(self, "Ollama", f"설치 상태: {reason}")

    def _ollama_pull_model(self) -> None:
        manager = self._make_ollama_manager()
        ok_server, reason_server = manager.ensure_server_running()
        if not ok_server:
            self.local_llm_status.setText(f"Server failed: {reason_server}")
            QMessageBox.warning(self, "Ollama", f"서버 상태 오류: {reason_server}")
            return
        ok, reason = manager.pull_model_if_needed()
        if ok:
            self.local_llm_status.setText("Model Ready")
            QMessageBox.information(self, "Ollama", "모델 준비 완료")
            return
        self.local_llm_status.setText(f"Pull failed: {reason}")
        QMessageBox.warning(self, "Ollama", f"모델 준비 실패: {reason}")

    def _ollama_test(self) -> None:
        manager = self._make_ollama_manager()
        ok, reason = manager.ping()
        if ok:
            self.local_llm_status.setText("Running")
            QMessageBox.information(self, "Ollama", "local LLM 연결 정상")
            return
        self.local_llm_status.setText(f"Not running: {reason}")
        QMessageBox.warning(self, "Ollama", f"연결 실패: {reason}")

    def _wrap_scroll(self, widget: QWidget) -> QWidget:
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(widget)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        return scroll

    def _on_tab_changed(self, index: int) -> None:
        if index < 0 or index >= self.stacked.count():
            return
        self.stacked.setCurrentIndex(index)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._apply_settings_responsive_layout()

    def _apply_settings_responsive_layout(self) -> None:
        if self._conn_grid is None:
            return
        width = max(1, int(self.width()))
        if width < 920:
            cols = 1
        elif width < 1200:
            cols = 2
        else:
            cols = 3
        while self._conn_grid.count():
            item = self._conn_grid.takeAt(0)
            if item is None:
                continue
        for idx, btn in enumerate(self._conn_buttons):
            self._conn_grid.addWidget(btn, idx // cols, idx % cols)
        for c in range(4):
            self._conn_grid.setColumnStretch(c, 0)
        for c in range(cols):
            self._conn_grid.setColumnStretch(c, 1)

    def upload_client_secret(self) -> None:
        selected, _ = QFileDialog.getOpenFileName(self, "Select client_secret JSON", "", "JSON Files (*.json);;All Files (*)")
        if not selected:
            return
        try:
            self.context.client_secrets_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(selected, self.context.client_secrets_path)
        except Exception as exc:
            QMessageBox.critical(self, "업로드 실패", str(exc))
            return
        QMessageBox.information(self, "완료", "client_secrets.json 업로드가 완료되었습니다.")

    def browse_token(self) -> None:
        selected, _ = QFileDialog.getOpenFileName(self, "Select blogger_token.json", "", "JSON Files (*.json);;All Files (*)")
        if not selected:
            return
        try:
            rel = str(Path(selected).resolve().relative_to(self.context.root)).replace("\\", "/")
        except Exception:
            rel = str(Path(selected).resolve())
        self.token_path.setText(rel)

    def google_login(self) -> None:
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow
            from googleapiclient.discovery import build
        except Exception as exc:
            QMessageBox.critical(self, "모듈 오류", f"Google 인증 모듈을 사용할 수 없습니다.\n{exc}")
            return

        if not self.context.client_secrets_path.exists():
            QMessageBox.information(self, "JSON 필요", "먼저 client_secret JSON 파일을 업로드해 주세요.")
            return

        try:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(self.context.client_secrets_path),
                scopes=[
                    "https://www.googleapis.com/auth/blogger",
                    "https://www.googleapis.com/auth/drive.file",
                    "https://www.googleapis.com/auth/indexing",
                    "https://www.googleapis.com/auth/adsense.readonly",
                    "https://www.googleapis.com/auth/analytics.readonly",
                    "https://www.googleapis.com/auth/webmasters.readonly",
                ],
            )
            creds = flow.run_local_server(port=0)
            self.context.blogger_token_path.write_text(creds.to_json(), encoding="utf-8")
            self.token_path.setText("config/blogger_token.json")
            service = build("blogger", "v3", credentials=creds)
            blogs = service.blogs().listByUser(userId="self").execute().get("items", []) or []
        except Exception as exc:
            QMessageBox.critical(self, "Google 로그인 실패", str(exc))
            return

        if not blogs:
            QMessageBox.warning(self, "블로그 없음", "이 계정에서 Blogger 블로그를 찾지 못했습니다.")
            return

        if len(blogs) == 1:
            self.blog_id.setText(str(blogs[0].get("id", "")))
            QMessageBox.information(self, "연결 완료", "Blog ID가 자동 입력되었습니다.")
            return

        labels = [f"{b.get('name', 'Untitled')} ({b.get('id', '')})" for b in blogs]
        selected, ok = QInputDialog.getItem(self, "블로그 선택", "연결할 블로그를 선택하세요.", labels, 0, False)
        if ok and selected:
            idx = labels.index(selected)
            self.blog_id.setText(str(blogs[idx].get("id", "")))
            QMessageBox.information(self, "연결 완료", "선택한 블로그를 연결했습니다.")

    def save(self) -> None:
        api_key = self.gemini_key.text().strip()
        blog_id = self.blog_id.text().strip()
        token = self.token_path.text().strip()

        if not self.required_only:
            integrations_enabled = bool(self.integrations_enabled.isChecked())
            adsense_enabled = bool(self.adsense_enabled.isChecked())
            analytics_enabled = bool(self.analytics_enabled.isChecked())
            search_console_enabled = bool(self.search_console_enabled.isChecked())
            ga4_property_id = self.ga4_property_id.text().strip()
            search_console_site_url = self.search_console_site_url.text().strip()
            integrations_refresh = int(self.integrations_refresh.value())
        else:
            integrations_enabled = _nested_get(self.data, "integrations.enabled").strip().lower() in {"1", "true", "yes", "on"}
            adsense_enabled = _nested_get(self.data, "integrations.adsense_enabled").strip().lower() in {"1", "true", "yes", "on"}
            analytics_enabled = _nested_get(self.data, "integrations.analytics_enabled").strip().lower() in {"1", "true", "yes", "on"}
            search_console_enabled = _nested_get(self.data, "integrations.search_console_enabled").strip().lower() in {"1", "true", "yes", "on"}
            ga4_property_id = (_nested_get(self.data, "integrations.ga4_property_id") or "").strip()
            search_console_site_url = (_nested_get(self.data, "integrations.search_console_site_url") or "").strip()
            try:
                integrations_refresh = int(_nested_get(self.data, "integrations.refresh_minutes") or "15")
            except Exception:
                integrations_refresh = 15

        free_mode = bool(self.free_mode.isChecked()) if not self.required_only else (
            _nested_get(self.data, "budget.free_mode").strip().lower() in {"1", "true", "yes", "on"}
        )
        enable_img = False
        gen_per_day = int(getattr(self, "posts_to_generate_per_day", QSpinBox()).value() if hasattr(self, "posts_to_generate_per_day") else int(_nested_get(self.data, "publishing.posts_to_generate_per_day") or "3"))
        pub_per_day = int(getattr(self, "posts_to_publish_per_day", QSpinBox()).value() if hasattr(self, "posts_to_publish_per_day") else int(_nested_get(self.data, "publishing.posts_to_publish_per_day") or "2"))
        if pub_per_day > gen_per_day:
            QMessageBox.warning(self, "입력 오류", "일일 발행 글 수는 일일 생성 글 수보다 클 수 없습니다.")
            return

        gemini_required = (not free_mode) or enable_img
        if gemini_required:
            if not api_key:
                QMessageBox.warning(self, "입력 필요", "Gemini API Key는 필수입니다.")
                return
            if not _is_valid_gemini_key(api_key):
                QMessageBox.warning(self, "형식 오류", "Gemini API Key 형식이 올바르지 않습니다.\n예: AIza로 시작하는 Google API Key")
                return
        elif api_key and (not _is_valid_gemini_key(api_key)):
            QMessageBox.warning(self, "형식 오류", "입력한 Gemini API Key 형식이 올바르지 않습니다.")
            return
        if not blog_id:
            QMessageBox.warning(self, "입력 필요", "Blogger Blog ID는 필수입니다.")
            return
        if not _is_valid_blogger_blog_id(blog_id):
            QMessageBox.warning(self, "형식 오류", "Blogger Blog ID 형식이 올바르지 않습니다.\n숫자만 입력해 주세요.")
            return
        if not token:
            QMessageBox.warning(self, "입력 필요", "blogger_token.json 경로는 필수입니다.")
            return
        ok_token, token_msg = _validate_blogger_token_file(self.context.root, token)
        if not ok_token:
            QMessageBox.warning(self, "토큰 오류", token_msg)
            return

        selected_model = self.model_combo.currentText().strip() or _nested_get(self.data, "gemini.model").strip()
        _nested_set(self.data, "gemini.api_key", api_key)
        if selected_model:
            _nested_set(self.data, "gemini.model", selected_model)
            _nested_set(self.data, "visual.gemini_prompt_model", selected_model)
        _nested_set(self.data, "visual.image_provider", "library")
        _nested_set(self.data, "visual.pollinations_enabled", False)
        _nested_set(self.data, "visual.pollinations_api_key", "")
        _nested_set(self.data, "visual.pollinations_thumbnail_model", "")
        _nested_set(self.data, "visual.pollinations_content_model", "")
        _nested_set(
            self.data,
            "visual.gemini_image_model",
            (self.gemini_image_model.text().strip() or "models/imagen-3.0-generate-001"),
        )
        _nested_set(self.data, "blogger.blog_id", blog_id)
        _nested_set(self.data, "blogger.credentials_path", token)
        _nested_set(self.data, "integrations.enabled", integrations_enabled)
        _nested_set(self.data, "integrations.adsense_enabled", adsense_enabled)
        _nested_set(self.data, "integrations.analytics_enabled", analytics_enabled)
        _nested_set(self.data, "integrations.search_console_enabled", search_console_enabled)
        _nested_set(self.data, "integrations.ga4_property_id", ga4_property_id)
        _nested_set(self.data, "integrations.search_console_site_url", search_console_site_url)
        _nested_set(self.data, "integrations.refresh_minutes", max(3, integrations_refresh))
        _nested_set(self.data, "budget.dry_run", bool(self.dry_run.isChecked()))
        _nested_set(self.data, "budget.free_mode", bool(self.free_mode.isChecked()))
        _nested_set(self.data, "visual.enable_gemini_image_generation", False)
        _nested_set(self.data, "visual.target_images_per_post", 5)
        _nested_set(self.data, "visual.max_banner_images", 1)
        _nested_set(self.data, "visual.max_inline_images", 4)
        _nested_set(self.data, "visual.cache_dir", "storage/image_cache")
        _nested_set(self.data, "visual.fallback_banner", "assets/fallback/banner.png")
        _nested_set(self.data, "visual.fallback_inline", "assets/fallback/inline.png")
        _nested_set(self.data, "images.provider", "library")
        _nested_set(self.data, "images.banner_count", 1)
        _nested_set(self.data, "images.inline_count", 4)
        _nested_set(self.data, "images.cache_dir", "storage/image_cache")
        _nested_set(self.data, "images.fallback_banner", "assets/fallback/banner.png")
        _nested_set(self.data, "images.fallback_inline", "assets/fallback/inline.png")
        _nested_set(self.data, "images.pollinations.model", "")
        _nested_set(self.data, "images.pollinations.size", "")
        _nested_set(self.data, "images.pollinations.timeout_sec", 0)
        _nested_set(self.data, "llm.provider", "gemini")
        _nested_set(self.data, "llm.enable_image_generation", False)
        _nested_set(self.data, "llm.enable_refine_loop", False)
        _nested_set(self.data, "llm.enable_judge_post", False)
        local_llm = self._local_llm_settings_from_ui()
        _nested_set(self.data, "local_llm.enabled", bool(local_llm.enabled))
        _nested_set(self.data, "local_llm.provider", str(local_llm.provider))
        _nested_set(self.data, "local_llm.model", str(local_llm.model or "qwen2.5:3b"))
        _nested_set(self.data, "local_llm.base_url", str(local_llm.base_url or "http://127.0.0.1:11434"))
        _nested_set(self.data, "local_llm.num_ctx", int(local_llm.num_ctx))
        _nested_set(self.data, "local_llm.num_thread", int(local_llm.num_thread))
        _nested_set(self.data, "local_llm.max_loaded_models", int(local_llm.max_loaded_models))
        _nested_set(self.data, "local_llm.num_parallel", int(local_llm.num_parallel))
        _nested_set(self.data, "local_llm.install_if_missing", bool(local_llm.install_if_missing))
        _nested_set(self.data, "local_llm.pull_model_if_missing", bool(local_llm.pull_model_if_missing))
        _nested_set(self.data, "local_llm.request_timeout_sec", int(local_llm.request_timeout_sec))
        _nested_set(self.data, "local_llm.max_calls_per_post", int(local_llm.max_calls_per_post))

        _nested_set(self.data, "content.language", "en-US")
        _nested_set(self.data, "content.enforce_english_only", bool(getattr(self, "enforce_english_only", QCheckBox()).isChecked() if hasattr(self, "enforce_english_only") else True))
        _nested_set(self.data, "publishing.posts_to_generate_per_day", gen_per_day)
        _nested_set(self.data, "publishing.posts_to_publish_per_day", pub_per_day)
        _nested_set(self.data, "publishing.buffer_target_days", int(getattr(self, "buffer_target_days", QSpinBox()).value() if hasattr(self, "buffer_target_days") else 5))
        _nested_set(self.data, "publishing.buffer_min_days", int(getattr(self, "buffer_min_days", QSpinBox()).value() if hasattr(self, "buffer_min_days") else 3))
        _nested_set(self.data, "publishing.time_window_start", str(getattr(self, "time_window_start", QLineEdit("09:00")).text() if hasattr(self, "time_window_start") else "09:00").strip())
        _nested_set(self.data, "publishing.time_window_end", str(getattr(self, "time_window_end", QLineEdit("23:00")).text() if hasattr(self, "time_window_end") else "23:00").strip())
        _nested_set(self.data, "publishing.randomness_level", str(getattr(self, "randomness_level", QComboBox()).currentText() if hasattr(self, "randomness_level") else "medium").strip().lower())
        _nested_set(self.data, "publishing.min_gap_minutes", int(getattr(self, "min_gap_minutes", QSpinBox()).value() if hasattr(self, "min_gap_minutes") else 180))
        _nested_set(self.data, "publishing.quiet_hours_enabled", bool(getattr(self, "quiet_hours_enabled", QCheckBox()).isChecked() if hasattr(self, "quiet_hours_enabled") else True))
        _nested_set(self.data, "publishing.quiet_hours_start", str(getattr(self, "quiet_hours_start", QLineEdit("02:00")).text() if hasattr(self, "quiet_hours_start") else "02:00").strip())
        _nested_set(self.data, "publishing.quiet_hours_end", str(getattr(self, "quiet_hours_end", QLineEdit("07:00")).text() if hasattr(self, "quiet_hours_end") else "07:00").strip())
        _nested_set(self.data, "topics.monthly_rotation_enabled", bool(getattr(self, "monthly_rotation_enabled", QCheckBox()).isChecked() if hasattr(self, "monthly_rotation_enabled") else True))
        rotation_text = str(getattr(self, "rotation_order", QLineEdit("windows,mac,iphone,galaxy")).text() if hasattr(self, "rotation_order") else "windows,mac,iphone,galaxy")
        rotation_order = [x.strip().lower() for x in re.split(r"[,\s]+", rotation_text) if x.strip()]
        if not rotation_order:
            rotation_order = ["windows", "mac", "iphone", "galaxy"]
        _nested_set(self.data, "topics.rotation_order", rotation_order)
        _nested_set(self.data, "keywords.db_path", "storage/keywords.sqlite")
        _nested_set(self.data, "keywords.refill_threshold_per_device", 100)
        _nested_set(self.data, "keywords.avoid_reuse_days", 30)
        _nested_set(self.data, "internal_links.enabled", True)
        _nested_set(self.data, "internal_links.body_link_count", 1)
        _nested_set(self.data, "internal_links.related_link_count", 2)
        _nested_set(self.data, "internal_links.overlap_threshold", 0.4)

        _save_yaml(self.context.settings_path, self.data)
        self.pref["animation_intensity"] = str(self.animation_intensity.currentText() or "high").strip().lower()
        _save_ui_preferences(self.context.root, self.pref)

        if self.on_saved:
            self.on_saved()
        QMessageBox.information(self, "저장 완료", "설정이 저장되었습니다.")
        self.accept()

    def test_models(self) -> None:
        api_key = self.gemini_key.text().strip()
        if not _is_valid_gemini_key(api_key):
            QMessageBox.warning(self, "형식 오류", "Gemini API Key 형식이 올바르지 않습니다.\n먼저 API 키를 정확히 입력해 주세요.")
            return

        endpoint = "https://generativelanguage.googleapis.com/v1beta/models"
        try:
            resp = requests.get(endpoint, params={"key": api_key}, timeout=20)
            resp.raise_for_status()
            models = resp.json().get("models", []) or []
        except requests.RequestException as exc:
            QMessageBox.critical(self, "모델 조회 실패", f"모델 목록 조회에 실패했습니다.\n{exc}")
            return

        candidates: list[str] = []
        for m in models:
            name = str(m.get("name", ""))
            if not name.startswith("models/"):
                continue
            short = name.split("/", 1)[1]
            methods = m.get("supportedGenerationMethods", []) or []
            if "generateContent" not in methods:
                continue
            if "gemini" not in short.lower():
                continue
            candidates.append(short)

        if not candidates:
            QMessageBox.warning(self, "모델 없음", "사용 가능한 Gemini generateContent 모델을 찾지 못했습니다.")
            return

        candidates = sorted(set(candidates), key=lambda x: x.lower())
        current = self.model_combo.currentText().strip()
        self.model_combo.clear()
        self.model_combo.addItems(candidates)
        if current and current in candidates:
            self.model_combo.setCurrentText(current)
        else:
            self.model_combo.setCurrentIndex(0)
        selected = self.model_combo.currentText().strip()
        QMessageBox.information(self, "모델 목록 완료", f"사용 가능한 모델 {len(candidates)}개를 불러왔습니다.\n현재 선택: {selected}")

    def show_quota_guide(self) -> None:
        model = self.model_combo.currentText().strip() or _nested_get(self.data, "gemini.model").strip()
        daily_calls = _nested_get(self.data, "budget.daily_gemini_call_limit").strip() or "50"
        daily_posts = _nested_get(self.data, "budget.daily_post_limit").strip() or "6"
        QMessageBox.information(
            self,
            "무료 쿼터 안내",
            "Gemini 무료 쿼터 상한은 모델/프로젝트/시간대에 따라 달라집니다.\n\n"
            f"현재 모델: {model}\n"
            f"일일 제한값: API 호출 {daily_calls}, 생성 {daily_posts}\n\n"
            "아래 공식 페이지에서 현재 계정 한도를 확인해 주세요.\n"
            f"- Usage dashboard: {self.context.gemini_usage_dashboard}\n"
            f"- Rate limits: {self.context.gemini_rate_limit_doc}\n"
            f"- Pricing: {self.context.gemini_pricing_doc}\n"
            f"- GCP Quotas: {self.context.gemini_quota_console}",
        )
        try:
            webbrowser.open(self.context.gemini_usage_dashboard)
            webbrowser.open(self.context.gemini_rate_limit_doc)
        except Exception:
            pass

