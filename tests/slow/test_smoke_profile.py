import unittest

from tests._env import require_slow_or_skip


class SlowProfileSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        require_slow_or_skip()

    def test_slow_profile_gate(self) -> None:
        self.assertTrue(True)

