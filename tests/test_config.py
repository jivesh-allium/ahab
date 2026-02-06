import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pequod.config import load_settings


class ConfigTests(unittest.TestCase):
    def test_dotenv_api_key_wins_but_runtime_knobs_can_be_env_overridden(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "ALLIUM_API_KEY=dotenv-key",
                        "PEQUOD_POLL_INTERVAL_SECONDS=30",
                        "PEQUOD_RUN_ONCE=false",
                    ]
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "ALLIUM_API_KEY": "env-key",
                    "PEQUOD_POLL_INTERVAL_SECONDS": "9",
                    "PEQUOD_RUN_ONCE": "true",
                },
                clear=False,
            ):
                settings = load_settings(str(env_path))

        self.assertEqual("dotenv-key", settings.allium_api_key)
        self.assertEqual(9, settings.poll_interval_seconds)
        self.assertTrue(settings.run_once)
        self.assertTrue(settings.dashboard_base_url.startswith("http://"))

    def test_hosting_platform_port_fallbacks_are_applied(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text("ALLIUM_API_KEY=dotenv-key\n", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "PORT": "4321",
                },
                clear=False,
            ):
                settings = load_settings(str(env_path))

        self.assertEqual("0.0.0.0", settings.dashboard_host)
        self.assertEqual(4321, settings.dashboard_port)

    def test_explicit_dashboard_bindings_override_platform_port(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "ALLIUM_API_KEY=dotenv-key",
                        "PEQUOD_DASHBOARD_HOST=127.0.0.1",
                        "PEQUOD_DASHBOARD_PORT=8080",
                    ]
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "PORT": "9999",
                },
                clear=False,
            ):
                settings = load_settings(str(env_path))

        self.assertEqual("127.0.0.1", settings.dashboard_host)
        self.assertEqual(8080, settings.dashboard_port)


if __name__ == "__main__":
    unittest.main()
