from __future__ import annotations

SUPPORT_PATTERNS = ['fix ', 'troubleshoot', 'not working', 'error code', 'step-by-step repair', 'faq', 'reset your', 'try these steps']


class SupportDriftGuard:
    def evaluate_text(self, text: str, *, allow_support: bool = False) -> tuple[bool, str]:
        blob = str(text or '').lower()
        if allow_support:
            return True, 'support_allowed'
        for pattern in SUPPORT_PATTERNS:
            if pattern in blob:
                return False, 'support_drift_rejected'
        return True, 'support_drift_clear'
