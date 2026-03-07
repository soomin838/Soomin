from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.actionability_gate import ActionabilityGate


def main() -> int:
    gate = ActionabilityGate()

    good_html = """
    <h2>Quick Take</h2><p>Short answer for users.</p>
    <h2>Fix 1</h2><ul><li>Open Settings and check updates.</li><li>Restart the app.</li></ul>
    <p>Expected result: app launches normally. If not: continue to Fix 2.</p>
    <h2>Fix 2</h2><ul><li>Reset the app cache.</li><li>Enable required permissions.</li></ul>
    <p>Expected result: response delay drops below 2 seconds. You should see normal behavior. If that doesn't work, next try network reset.</p>
    <h2>Fix 3</h2><ul><li>Run network reset.</li><li>Reboot device.</li></ul>
    <p>Expected result: connectivity improves. Result: stable connection in two tests. Otherwise move to Fix 4.</p>
    <h2>Fix 4</h2><ul><li>Reinstall the app.</li><li>Sign in again.</li></ul>
    <h2>Fix 5</h2><ul><li>Run diagnostics.</li><li>Contact official support.</li></ul>
    <h2>Prevention Checklist</h2><ul>
      <li>Check weekly updates.</li><li>Turn on auto backup.</li><li>Clear stale cache monthly.</li>
      <li>Review permission changes after updates.</li><li>Keep one restart routine.</li><li>Record what changed.</li>
    </ul>
    """
    bad_html = "<h2>Overview</h2><p>This is generally comprehensive and maybe useful overall.</p>"

    good_result = gate.evaluate(
        title="Windows not working after update? 5 fixes that actually work",
        html=good_html,
        min_steps=8,
        min_word_count=70,
        max_generic_ratio=0.02,
    )
    bad_result = gate.evaluate(
        title="General discussion",
        html=bad_html,
        min_steps=8,
        min_word_count=120,
        max_generic_ratio=0.02,
    )

    print("GOOD:", {"ok": good_result.ok, "score": good_result.score, "reasons": good_result.reasons})
    print("BAD:", {"ok": bad_result.ok, "score": bad_result.score, "reasons": bad_result.reasons})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
