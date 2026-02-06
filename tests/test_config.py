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


if __name__ == "__main__":
    unittest.main()
