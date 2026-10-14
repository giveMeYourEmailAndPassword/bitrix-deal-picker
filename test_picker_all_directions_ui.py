"""The actual Bitrix UI allows active employees without any skill selection."""

import json
import shutil
import subprocess
import unittest

from test_app import app


class TestAllDirectionsPickerUi(unittest.TestCase):
    def test_skill_profiles_do_not_disable_the_button_but_identity_still_does(self):
        node = shutil.which("node")
        if not node:
            self.skipTest("Node.js is unavailable for the browser behavior check")
        rendered = app.render_index_html(initial_auth={}, nonce="all-directions")
        script = rendered.rsplit('<script nonce="all-directions">', 1)[1].split("</script>", 1)[0]
        start = script.index("function renderManagerInfo(manager)")
        end = script.index("function updatePrimaryButton()", start)
        cases = [
            ({"active": True, "intranet": True}, True, True),
            ({"active": True, "intranet": True, "competencies": []}, True, True),
            ({"active": True, "intranet": True, "competencies": ["Турция"]}, True, True),
            ({"active": True, "intranet": True, "competencies": ["Египет"]}, True, True),
            ({"active": False, "intranet": True, "competencies": ["Турция"]}, True, False),
            ({"active": True, "intranet": False, "competencies": ["Турция"]}, True, False),
            ({"active": True, "intranet": True, "competencies": []}, False, False),
        ]
        scenario = """
let userVerified = false;
let canRequestDeal = false;
let elements;
global.document = { getElementById: id => elements[id] };
const results = CASES.map(([manager, verified]) => {
  userVerified = verified;
  elements = { getDealButton: {}, managerInfo: {}, status: {} };
  renderManagerInfo({name: 'Test employee', ...manager});
  return {allowed: canRequestDeal, disabled: elements.getDealButton.disabled};
});
process.stdout.write(JSON.stringify(results));
""".replace("CASES", json.dumps(cases, ensure_ascii=False))
        completed = subprocess.run(
            [node], input=script[start:end] + scenario,
            text=True, capture_output=True, check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr or completed.stdout)
        results = json.loads(completed.stdout)
        for (profile, verified, expected), result in zip(cases, results):
            with self.subTest(profile=profile, verified=verified):
                self.assertEqual(result, {"allowed": expected, "disabled": not expected})


if __name__ == "__main__":
    unittest.main()
