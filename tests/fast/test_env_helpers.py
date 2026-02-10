import os
import unittest
from unittest.mock import patch

from tests import _env


class EnvHelpersTests(unittest.TestCase):
    def test_is_db_enabled(self) -> None:
        with patch.dict(os.environ, {"RUN_DB_TESTS": "1"}, clear=False):
            self.assertTrue(_env.is_db_enabled())
        with patch.dict(os.environ, {"RUN_DB_TESTS": "0"}, clear=False):
            self.assertFalse(_env.is_db_enabled())

    def test_is_slow_enabled(self) -> None:
        with patch.dict(os.environ, {"RUN_SLOW_TESTS": "1"}, clear=False):
            self.assertTrue(_env.is_slow_enabled())
        with patch.dict(os.environ, {"RUN_SLOW_TESTS": ""}, clear=False):
            self.assertFalse(_env.is_slow_enabled())

    def test_require_db_or_skip(self) -> None:
        with patch.dict(os.environ, {"RUN_DB_TESTS": "0"}, clear=False):
            with self.assertRaises(unittest.SkipTest):
                _env.require_db_or_skip()
        with patch.dict(
            os.environ,
            {"RUN_DB_TESTS": "1", "TEST_DATABASE_URL": ""},
            clear=False,
        ):
            with self.assertRaises(unittest.SkipTest):
                _env.require_db_or_skip()

