"""Offline rows for scripts/console_steps.py, over the live browser harnesses in scripts/.

    python3 scripts/test_console_steps.py      (or: python3 -m pytest scripts/test_console_steps.py)

No cluster and no browser: each row reads a harness's text or the console's page sources.
"""
from __future__ import annotations

import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import console_steps  # noqa: E402

ROOT = HERE.parent


def test_no_live_ui_harness_drives_a_hidden_control_off_its_step():
    """Every browser harness under scripts/. console-steps.mjs is the walk itself, not a flow."""
    harnesses = [p for p in sorted((ROOT / "scripts").glob("*.mjs")) if p.name != "console-steps.mjs"]
    # NOT VACUOUS: the PROD-16.1 harness is read, and it drives the wizard.
    assert "prod16-1-ui-e2e.mjs" in [p.name for p in harnesses], [p.name for p in harnesses]
    driving = 0
    for p in harnesses:
        text = p.read_text()
        assert console_steps.unreached_controls(text) == [], (p.name, console_steps.unreached_controls(text))
        driving += console_steps.controls_driven(text)
    assert driving > 0


def test_a_link_click_into_the_wizard_is_not_a_walk_to_its_steps():
    """NEGATIVE CONTROL: a link into the one-step wizard followed by fills on steps 2 and 4 is
    flagged on every control; the same flow walked with wizardAt/wizardStep is not."""
    click = ('await page.click("#step-catalog-points a[href*=\\"point=" + p.pointId + "\\"]");\n')
    old = click + ('await waitForSelector(page, "#catalog-topics", "the wizard on the minted point");\n'
                   'await page.fill("#catalog-topics", SOURCE_TOPIC);\n'
                   'await page.fill("#topic-prefix", RESTORE_PREFIX);\n')
    flagged = console_steps.unreached_controls(old)
    assert len(flagged) == 3 and all("no step" in f for f in flagged), flagged
    walked = click + ('await waitForSelector(page, "#wizard-position", "the wizard");\n'
                      'await wizardAt(page, 1, 60);\nawait wizardStep(page, 2);\n'
                      'await waitForSelector(page, "#catalog-topics", "the catalog step");\n'
                      'await page.fill("#catalog-topics", SOURCE_TOPIC);\n'
                      'await wizardStep(page, 4);\nawait page.fill("#topic-prefix", RESTORE_PREFIX);\n')
    assert console_steps.unreached_controls(walked) == []


def test_the_harness_step_map_is_the_pages_own():
    assert console_steps.ui_disagreements() == []


if __name__ == "__main__":
    rows = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for row in rows:
        row()
        print("ok:", row.__name__)
    print(f"test_console_steps: {len(rows)} rows passed")
