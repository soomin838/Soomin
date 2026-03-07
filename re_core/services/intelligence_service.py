import re
from typing import Any

class IntelligenceService:
    """
    Centralized service for heuristics, categorization, and local LLM-assisted classification.
    """
    def __init__(self, ollama_client=None):
        self.ollama_client = ollama_client

    def infer_device_type(self, text: str) -> str:
        """Categorize text into device families."""
        low = str(text or "").lower()
        if any(w in low for w in ("iphone", "ipad", "ios", "apple watch", "airpods")):
            return "ios"
        if any(w in low for w in ("mac", "macbook", "macos", "imac")):
            return "mac"
        if any(w in low for w in ("android", "galaxy", "pixel", "oneplus")):
            return "android"
        if any(w in low for w in ("ps5", "xbox", "switch", "playstation", "console")):
            return "console"
        return "windows"

    def infer_feature_token(self, text: str) -> str:
        """Extract primary feature keywords."""
        low = str(text or "").lower()
        tokens = [
            "wifi", "bluetooth", "audio", "display", "update", 
            "battery", "camera", "microphone", "printer", "network", 
            "driver", "security", "performance", "storage"
        ]
        for t in tokens:
            if t in low:
                return t
        return "system"

    def infer_cluster_id_from_keyword(self, text: str) -> str:
        """Infer a broader cluster category from specific keywords."""
        low = str(text or "").lower()
        mapping = {
            "update": ("update", "patch", "version"),
            "network": ("network", "wifi", "internet", "ethernet", "router"),
            "audio": ("audio", "sound", "mic", "speaker", "volume"),
            "display": ("display", "screen", "monitor", "resolution", "gpu"),
            "bluetooth": ("bluetooth", "pairing", "connect"),
            "power": ("battery", "power", "charging", "ac adapter"),
            "performance": ("performance", "slow", "lag", "crash", "freeze")
        }
        for cluster, words in mapping.items():
            if any(w in low for w in words):
                return cluster
        return "software"

    def infer_device_hint(self, text: str, entity: str = "") -> str:
        """Extract a display-friendly device name."""
        low = str(text or "").lower()
        if "windows 11" in low: return "Windows 11"
        if "windows 10" in low: return "Windows 10"
        
        mapping = [
            ("windows", "Windows"), ("macos", "macOS"), ("mac", "Mac"),
            ("iphone", "iPhone"), ("ios", "iPhone"), ("galaxy", "Galaxy"),
            ("samsung", "Galaxy"), ("android", "Android"), ("console", "Console"),
            ("ps5", "PlayStation 5"), ("xbox", "Xbox")
        ]
        for token, label in mapping:
            if token in low: return label
            
        ent = str(entity or "").lower()
        for token, label in mapping:
            if token in ent: return label
        return ""

    def infer_feature_hint(self, text: str) -> str:
        """Extract a display-friendly feature name."""
        low = str(text or "").lower()
        mapping = [
            ("wifi", "Wi-Fi"), ("bluetooth", "Bluetooth"), ("usb", "USB"),
            ("printer", "Printer"), ("microphone", "Microphone"), ("mic", "Microphone"),
            ("camera", "Camera"), ("keyboard", "Keyboard"), ("mouse", "Mouse"),
            ("driver", "Driver"), ("ethernet", "Ethernet"), ("vpn", "VPN"),
            ("audio", "Audio"), ("sound", "Audio"), ("battery", "Battery"),
            ("charging", "Charging"), ("update", "Update")
        ]
        for token, label in mapping:
            if token in low: return label
        return ""

    def detect_ai_markers_locally(self, text: str) -> list[str]:
        """Use local LLM or regex to find common AI-generated markers/leaks."""
        found = []
        low = text.lower()
        
        # Regex-based leaks
        regex_patterns = [
            r"workflow checkpoint stage",
            r"av reference context",
            r"as an ai language model",
            r"here is the article",
            r"successfully generated",
            r"selected topic",
            r"source trending_entities"
        ]
        for pattern in regex_patterns:
            if re.search(pattern, low):
                found.append(f"leak:{pattern}")
        
        return found
