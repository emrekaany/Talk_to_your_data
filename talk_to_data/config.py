"""Application configuration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency in some environments
    def load_dotenv(*args: object, **kwargs: object) -> bool:
        return False


DEFAULT_LLM_URL = (
    "http://query-insight-llm-router-ai.apps.ocpai.anadolusigorta.com.tr"
    "/v1/chat/completions"
)
_ROOT_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


@dataclass(frozen=True)
class AppConfig:
    """Runtime configuration loaded from environment variables."""

    metadata_path: Path
    agent_registry_path: Path
    runs_dir: Path
    llm_url: str
    llm_model: str
    llm_api_key: str
    llm_timeout_sec: int
    llm_summarizer_enabled: bool
    oracle_user: str
    oracle_password: str
    oracle_dsn: str

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Create config from environment variables."""
        _load_local_env()
        return cls(
            metadata_path=Path(
                os.getenv("METADATA_VECTORED_PATH", "metadata_vectored.json")
            ),
            agent_registry_path=Path(
                os.getenv("AGENT_REGISTRY_PATH", "metadata/agents/agents.json")
            ),
            runs_dir=Path(os.getenv("RUNS_DIR", "runs")),
            llm_url=os.getenv("LLM_URL", DEFAULT_LLM_URL),
            llm_model=os.getenv("LLM_MODEL", "florence_v2"),
            llm_api_key=os.getenv("LLM_API_KEY", "") or os.getenv("OPENAI_API_KEY", ""),
            llm_timeout_sec=_safe_int(os.getenv("LLM_TIMEOUT_SEC"), fallback=60),
            llm_summarizer_enabled=_safe_bool(os.getenv("LLM_SUMMARIZER_ENABLED"), False),
            oracle_user=os.getenv("ORACLE_USER", ""),
            oracle_password=os.getenv("ORACLE_PASSWORD", ""),
            oracle_dsn=os.getenv("ORACLE_DSN", ""),
        )

    @property
    def llm_enabled(self) -> bool:
        return bool(self.llm_api_key and self.llm_url and self.llm_model)

    def missing_oracle_env(self) -> list[str]:
        missing: list[str] = []
        if not self.oracle_user:
            missing.append("ORACLE_USER")
        if not self.oracle_password:
            missing.append("ORACLE_PASSWORD")
        if not self.oracle_dsn:
            missing.append("ORACLE_DSN")
        return missing


def _safe_int(value: str | None, fallback: int) -> int:
    if value is None:
        return fallback
    try:
        return int(value)
    except ValueError:
        return fallback


def _safe_bool(value: str | None, fallback: bool) -> bool:
    if value is None:
        return fallback
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return fallback


def _load_local_env() -> None:
    loaded = bool(load_dotenv(override=False))
    if loaded:
        return
    _load_env_file(_ROOT_ENV_PATH)


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_key = key.strip()
        if not env_key or env_key in os.environ:
            continue
        env_value = value.strip()
        if len(env_value) >= 2 and env_value[0] == env_value[-1] and env_value[0] in {"'", '"'}:
            env_value = env_value[1:-1]
        os.environ[env_key] = env_value
