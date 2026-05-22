import os
import tempfile
import unittest
from unittest import mock

from scripts import deepseek_agent_loop


class DeepSeekConfigTests(unittest.TestCase):
    def write_env(self, contents):
        handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False)
        self.addCleanup(lambda: os.path.exists(handle.name) and os.unlink(handle.name))
        with handle:
            handle.write(contents)
        return handle.name

    def test_env_file_values_feed_argument_defaults(self):
        env_file = self.write_env(
            "\n".join(
                [
                    "AGENTD_SOCKET_PATH=/tmp/from-env.sock",
                    "DEEPSEEK_BASE_URL=https://example.invalid",
                    "DEEPSEEK_MODEL=env-model",
                    "DEEPSEEK_MAX_TOKENS=37",
                    "DEEPSEEK_TEMPERATURE=0.7",
                    "AGENTD_AGENT_WORKERS=3",
                ]
            )
        )

        with mock.patch.dict(os.environ, {}, clear=True):
            args = deepseek_agent_loop.parse_args(["--env-file", env_file])

        self.assertEqual(args.socket_path, "/tmp/from-env.sock")
        self.assertEqual(args.base_url, "https://example.invalid")
        self.assertEqual(args.model, "env-model")
        self.assertEqual(args.max_tokens, 37)
        self.assertEqual(args.temperature, 0.7)
        self.assertEqual(args.workers, 3)

    def test_process_environment_overrides_env_file(self):
        env_file = self.write_env("DEEPSEEK_MODEL=file-model\n")

        with mock.patch.dict(
            os.environ,
            {"DEEPSEEK_MODEL": "process-model"},
            clear=True,
        ):
            args = deepseek_agent_loop.parse_args(["--env-file", env_file])

        self.assertEqual(args.model, "process-model")

    def test_cli_arguments_override_env_file(self):
        env_file = self.write_env("DEEPSEEK_MAX_TOKENS=37\nAGENTD_AGENT_WORKERS=3\n")

        with mock.patch.dict(os.environ, {}, clear=True):
            args = deepseek_agent_loop.parse_args(
                ["--env-file", env_file, "--max-tokens", "9", "--workers", "1"]
            )

        self.assertEqual(args.max_tokens, 9)
        self.assertEqual(args.workers, 1)


if __name__ == "__main__":
    unittest.main()
