import os
import unittest
from unittest.mock import patch

import config


class ConfigCoreTests(unittest.TestCase):
    def test_get_env_var_required_missing_returns_none(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            self.assertIsNone(config.get_env_var("NON_EXISTENT_REQUIRED", required=True))

    def test_get_env_var_optional_with_default(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            self.assertEqual(
                "fallback",
                config.get_env_var(
                    "NON_EXISTENT_OPTIONAL",
                    default="fallback",
                    required=False,
                ),
            )

    def test_get_env_bool_truthy_values(self) -> None:
        for value in ("1", "true", "yes", "y", "on", "TRUE"):
            with patch.dict(os.environ, {"BOOL_TEST": value}, clear=False):
                self.assertTrue(config.get_env_bool("BOOL_TEST"))

    def test_get_env_bool_falsey_values(self) -> None:
        for value in ("0", "false", "off", "", "nope"):
            with patch.dict(os.environ, {"BOOL_TEST": value}, clear=False):
                self.assertFalse(config.get_env_bool("BOOL_TEST"))

    def test_require_env_value(self) -> None:
        self.assertEqual("x", config.require_env_value("NAME", "x"))
        with self.assertRaises(ValueError):
            config.require_env_value("NAME", None)

