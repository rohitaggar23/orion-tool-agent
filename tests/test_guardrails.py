import unittest
from orion_agent.guardrails import enforce_select_only, GuardrailError

class TestGuardrails(unittest.TestCase):
    def test_select_allowed(self):
        enforce_select_only("SELECT * FROM tickets")

    def test_drop_blocked(self):
        with self.assertRaises(GuardrailError):
            enforce_select_only("DROP TABLE tickets")

if __name__ == "__main__":
    unittest.main()
