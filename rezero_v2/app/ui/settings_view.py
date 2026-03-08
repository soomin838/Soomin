from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QCheckBox, QComboBox, QFormLayout, QFrame, QHBoxLayout, QLabel, QPushButton, QSpinBox, QVBoxLayout, QWidget

from rezero_v2.stores.app_settings_store import V2RuntimeConfig


class SettingsView(QWidget):
    save_requested = Signal(object)

    def __init__(self) -> None:
        super().__init__()
        root = QVBoxLayout(self)
        frame = QFrame()
        frame.setProperty("card", True)
        layout = QVBoxLayout(frame)
        form = QFormLayout()
        self.runtime_combo = QComboBox()
        self.runtime_combo.addItems(["v1", "v2"])
        self.v2_enabled = QCheckBox("V2 런타임 사용")
        self.mix_hot = self._spin(0, 5)
        self.mix_search = self._spin(0, 5)
        self.mix_evergreen = self._spin(0, 5)
        self.hot_min = self._spin(300, 3000)
        self.hot_max = self._spin(300, 3000)
        self.search_min = self._spin(300, 3000)
        self.search_max = self._spin(300, 3000)
        self.evergreen_min = self._spin(300, 4000)
        self.evergreen_max = self._spin(300, 4000)
        self.model_combo = QComboBox()
        self.model_combo.addItems(["flux", "turbo", "anime"])
        self.inline_optional = QCheckBox("인라인 이미지 허용")
        form.addRow("기본 런타임", self.runtime_combo)
        form.addRow("", self.v2_enabled)
        form.addRow("hot 비율", self.mix_hot)
        form.addRow("search 비율", self.mix_search)
        form.addRow("evergreen 비율", self.mix_evergreen)
        form.addRow("hot 길이 범위", self._pair(self.hot_min, self.hot_max))
        form.addRow("search 길이 범위", self._pair(self.search_min, self.search_max))
        form.addRow("evergreen 길이 범위", self._pair(self.evergreen_min, self.evergreen_max))
        form.addRow("Pollinations 모델", self.model_combo)
        form.addRow("", self.inline_optional)
        layout.addLayout(form)
        self.summary = QLabel("V2 뉴스 이미지는 Pollinations 전용이며, 재사용과 라이브러리 fallback을 허용하지 않습니다.")
        self.summary.setWordWrap(True)
        layout.addWidget(self.summary)
        self.save_button = QPushButton("저장")
        self.save_button.clicked.connect(self._emit_save)
        layout.addWidget(self.save_button)
        root.addWidget(frame)

    def load_config(self, config: V2RuntimeConfig) -> None:
        self.runtime_combo.setCurrentText(config.default_version)
        self.v2_enabled.setChecked(bool(config.v2_enabled))
        self.mix_hot.setValue(int(config.mix_hot))
        self.mix_search.setValue(int(config.mix_search_derived))
        self.mix_evergreen.setValue(int(config.mix_evergreen))
        self.hot_min.setValue(int(config.hot_min))
        self.hot_max.setValue(int(config.hot_max))
        self.search_min.setValue(int(config.search_derived_min))
        self.search_max.setValue(int(config.search_derived_max))
        self.evergreen_min.setValue(int(config.evergreen_min))
        self.evergreen_max.setValue(int(config.evergreen_max))
        self.model_combo.setCurrentText(config.pollinations_model)
        self.inline_optional.setChecked(bool(config.allow_inline_optional))

    def _emit_save(self) -> None:
        self.save_requested.emit(
            V2RuntimeConfig(
                default_version=self.runtime_combo.currentText(),
                v2_enabled=self.v2_enabled.isChecked(),
                mix_hot=self.mix_hot.value(),
                mix_search_derived=self.mix_search.value(),
                mix_evergreen=self.mix_evergreen.value(),
                hot_min=self.hot_min.value(),
                hot_max=self.hot_max.value(),
                search_derived_min=self.search_min.value(),
                search_derived_max=self.search_max.value(),
                evergreen_min=self.evergreen_min.value(),
                evergreen_max=self.evergreen_max.value(),
                pollinations_model=self.model_combo.currentText(),
                allow_inline_optional=self.inline_optional.isChecked(),
                allow_reuse=False,
                allow_library_fallback=False,
                allow_news_pack_fallback=False,
            )
        )

    def _spin(self, low: int, high: int) -> QSpinBox:
        spin = QSpinBox()
        spin.setRange(low, high)
        return spin

    def _pair(self, left, right) -> QWidget:
        box = QWidget()
        layout = QHBoxLayout(box)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(left)
        layout.addWidget(right)
        return box
