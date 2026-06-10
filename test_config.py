import os
from pathlib import Path
import unittest
from unittest.mock import patch

from config import OrchestratorConfig


ROOT = Path(__file__).resolve().parent
REQUIRED_RUNTIME_ENV_KEYS = {
    "ORCH_STT_WS_URL",
    "ORCH_LLM_HTTP_URL",
    "ORCH_TTS_HTTP_URL",
    "ORCH_AGENT_CONTROL_WSS_URL",
    "ORCH_AGENT_TOKEN",
}


class OrchestratorConfigTest(unittest.TestCase):
    def test_from_env_requires_runtime_service_endpoints(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "ORCH_STT_WS_URL"):
                OrchestratorConfig.from_env()

    def test_from_env_reads_runtime_service_endpoints_from_environment(self) -> None:
        env = {
            "ORCH_STT_WS_URL": "ws://stt.local:8000/stt/ws",
            "ORCH_LLM_HTTP_URL": "http://llm.local:8001",
            "ORCH_TTS_HTTP_URL": "http://tts.local:8002",
            "ORCH_AGENT_CONTROL_WSS_URL": "wss://api.example.com/api/v1/agents/control",
            "ORCH_AGENT_TOKEN": "agent-token",
        }

        with patch.dict(os.environ, env, clear=True):
            config = OrchestratorConfig.from_env()

        self.assertEqual("ws://stt.local:8000/stt/ws", config.stt_ws_url)
        self.assertEqual("http://llm.local:8001", config.llm_http_url)
        self.assertEqual("http://tts.local:8002", config.tts_http_url)
        self.assertEqual("wss://api.example.com/api/v1/agents/control", config.agent_control_wss_url)
        self.assertEqual("agent-token", config.agent_token)


class ConfigurationOwnershipTest(unittest.TestCase):
    def test_local_env_contains_only_home_server_required_values(self) -> None:
        env_keys = {
            line.split("=", 1)[0]
            for line in (ROOT / ".env").read_text().splitlines()
            if line and not line.startswith("#")
        }

        self.assertEqual(
            REQUIRED_RUNTIME_ENV_KEYS,
            env_keys,
        )

    def test_local_compose_passes_all_required_runtime_values(self) -> None:
        compose = (ROOT / "docker-compose.yml").read_text()

        for key in REQUIRED_RUNTIME_ENV_KEYS:
            self.assertIn(f"{key}:", compose)
            self.assertIn(f"${{{key}:?", compose)

    def test_dockerfile_does_not_own_runtime_configuration(self) -> None:
        dockerfile = (ROOT / "Dockerfile").read_text()

        self.assertNotIn("ORCH_", dockerfile)
        self.assertNotIn("AWS_", dockerfile)

    def test_local_compose_does_not_reference_github_secret_values(self) -> None:
        compose = (ROOT / "docker-compose.yml").read_text()

        self.assertNotIn("AWS_S3_ACCESS_KEY", compose)
        self.assertNotIn("AWS_S3_SECRET_KEY", compose)
        self.assertNotIn("ORCH_S3_AUDIO_BUCKET", compose)

    def test_github_deploy_passes_and_validates_required_runtime_values(self) -> None:
        deploy = (ROOT / ".github/workflows/deploy.yml").read_text()

        for key in REQUIRED_RUNTIME_ENV_KEYS:
            self.assertIn(f"require_value", deploy)
            self.assertIn(key, deploy)
            self.assertIn(f"-e {key}=", deploy)


if __name__ == "__main__":
    unittest.main()
