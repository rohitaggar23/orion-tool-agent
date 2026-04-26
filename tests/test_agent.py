import unittest
from orion_agent.factory import build_planner

class TestAgent(unittest.TestCase):
    def test_sql_question(self):
        result = build_planner().run("How many P0 tickets are open?")
        self.assertIn("open_p0", result.answer)
        self.assertEqual(result.trace[0].tool, "sql")

    def test_policy_question(self):
        result = build_planner().run("What is the escalation rule for P0 incidents?")
        self.assertIn("incident commander", result.answer.lower())

if __name__ == "__main__":
    unittest.main()
