"""Keep the TLC runner from accepting a run that did not end as expected."""

import pathlib
import unittest

from tlc import Expectation, outcome_matches, parse_expectation

SPEC = pathlib.Path(__file__).resolve().parent.parent / "spec"
PASSED = "Model checking completed. No error has been found.\n10 states generated, 5 distinct states found"
VIOLATED = "Error: Invariant ReferencedObjectsVerified is violated.\nError: The behavior up to this point is:"


class ParseExpectationTest(unittest.TestCase):
    def test_pass(self):
        self.assertIsNone(parse_expectation("\\* expect: pass\nSPECIFICATION Spec\n").violation)

    def test_violation(self):
        got = parse_expectation("\\* expect: violation MonotoneFencing\n")
        self.assertEqual(got.violation, "MonotoneFencing")

    def test_missing_line_is_an_error(self):
        for text in ("", "SPECIFICATION Spec\n\\* expect: pass\n", "\\* expect: maybe\n"):
            with self.assertRaises(ValueError):
                parse_expectation(text)

    def test_every_configuration_states_an_expectation(self):
        configs = sorted(SPEC.glob("*.cfg"))
        self.assertTrue(configs)
        for cfg in configs:
            with self.subTest(cfg=cfg.name):
                parse_expectation(cfg.read_text())


class OutcomeTest(unittest.TestCase):
    def test_pass_needs_success_message_and_status(self):
        self.assertTrue(outcome_matches(Expectation(None), 0, PASSED))
        self.assertFalse(outcome_matches(Expectation(None), 12, VIOLATED))
        self.assertFalse(outcome_matches(Expectation(None), 0, "Error: parse error"))

    def test_violation_needs_the_named_invariant(self):
        self.assertTrue(outcome_matches(Expectation("ReferencedObjectsVerified"), 12, VIOLATED))
        self.assertFalse(outcome_matches(Expectation("MonotoneFencing"), 12, VIOLATED))
        self.assertFalse(outcome_matches(Expectation("ReferencedObjectsVerified"), 0, PASSED))

    def test_other_failures_are_not_the_expected_violation(self):
        deadlock = "Error: Deadlock reached.\n"
        self.assertFalse(outcome_matches(Expectation("ReferencedObjectsVerified"), 11, deadlock))


if __name__ == "__main__":
    unittest.main()
