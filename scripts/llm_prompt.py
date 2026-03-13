import argparse
import json
import os
from pathlib import Path
import sys
import urllib.request
import urllib.error
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency in some environments
    def load_dotenv(*args: object, **kwargs: object) -> bool:
        return False

try:
    from talk_to_data.llm_logging import log_prompt
except Exception:  # pragma: no cover - script can run outside repository context
    log_prompt = None  # type: ignore[assignment]


DEFAULT_URL = (
    "http://query-insight-llm-router-ai.apps.ocpai.anadolusigorta.com.tr"
    "/v1/chat/completions"
)
DEFAULT_MODEL = "florence_v2"
DEFAULT_TIMEOUT_SEC = 60

_ROOT_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


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


def _resolve_api_key(api_key: str | None) -> str:
    key = (api_key or os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
    if not key:
        raise RuntimeError(
            "Missing API key. Set LLM_API_KEY or OPENAI_API_KEY in environment or .env."
        )
    if key.lower() == "sk-xxxx":
        raise RuntimeError(
            "Placeholder key detected (sk-xxxx). Replace it with your real token."
        )
    return key


def _resolve_url(url: str | None) -> str:
    resolved = (url or os.getenv("LLM_URL") or DEFAULT_URL).strip()
    return resolved or DEFAULT_URL


def _resolve_model(model: str | None) -> str:
    resolved = (model or os.getenv("LLM_MODEL") or DEFAULT_MODEL).strip()
    return resolved or DEFAULT_MODEL


def _resolve_timeout(timeout_sec: int | None) -> int:
    if timeout_sec is not None:
        return max(1, int(timeout_sec))
    raw = os.getenv("LLM_TIMEOUT_SEC")
    if raw is None:
        return DEFAULT_TIMEOUT_SEC
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_TIMEOUT_SEC


_load_local_env()


def _read_prompt(prompt_arg: str | None) -> str:
    if prompt_arg:
        return prompt_arg
    if not sys.stdin.isatty():
        return sys.stdin.read().strip()
    return input("Prompt: ").strip()


def _extract_content(raw: str) -> str:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return raw

    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, str) and content.strip():
            return content.strip()
    return raw


def request_completion(
    *,
    prompt: str,
    system: str = "You are a helpful assistant.",
    model: str | None = None,
    url: str | None = None,
    temperature: float = 0.2,
    max_tokens: int = 512,
    api_key: str | None = None,
    timeout_sec: int | None = None,
    raw: bool = False,
) -> str:
    """Send a chat completion request and return extracted content text."""
    key = _resolve_api_key(api_key)
    resolved_url = _resolve_url(url)
    resolved_model = _resolve_model(model)
    resolved_timeout = _resolve_timeout(timeout_sec)

    if log_prompt is not None:
        log_prompt(
            source="scripts.llm_prompt",
            model=resolved_model,
            url=resolved_url,
            system_prompt=system,
            user_prompt=prompt,
            temperature=temperature,
            max_tokens=max_tokens,
        )

    payload: dict[str, Any] = {
        "model": resolved_model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    req = urllib.request.Request(
        resolved_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {key}",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=resolved_timeout) as resp:
            raw_text = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"LLM HTTP error {exc.code}: {body[:300] or exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"LLM connection error: {exc.reason}") from exc

    return raw_text if raw else _extract_content(raw_text)


def main() -> int:
    parser = argparse.ArgumentParser(description="Simple chat-completions client.")
    parser.add_argument("prompt", nargs="?", help="User prompt text.")
    parser.add_argument(
        "--system",
        default="You are a helpful assistant.",
        help="System prompt.",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Model name (defaults to LLM_MODEL env or florence_v2).",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Chat completions endpoint (defaults to LLM_URL env or repo default).",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.2,
        help="Sampling temperature.",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=512,
        help="Max tokens to generate.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=int,
        default=None,
        help="HTTP timeout seconds (defaults to LLM_TIMEOUT_SEC env or 60).",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print raw JSON response.",
    )
    args = parser.parse_args()

    prompt = _read_prompt(args.prompt)
    if not prompt:
        print("Prompt is empty.", file=sys.stderr)
        return 2

    try:
        text = request_completion(
            prompt=prompt,
            system=args.system,
            model=args.model,
            url=args.url,
            temperature=args.temperature,
            max_tokens=args.max_tokens,
            timeout_sec=args.timeout_sec,
            raw=args.raw,
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
