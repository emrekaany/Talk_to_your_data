"""Agent registry loading and lookup helpers."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any


class AgentRegistryError(RuntimeError):
    """Raised when agent registry is missing or malformed."""


@dataclass(frozen=True)
class AgentConfig:
    """One query agent configuration."""

    id: str
    label: str
    metadata_path: Path
    rules_path: Path
    description: str


@dataclass(frozen=True)
class AgentRegistry:
    """Agent registry model."""

    default_agent_id: str
    agents: tuple[AgentConfig, ...]

    def list_agents(self) -> list[dict[str, str]]:
        """Return ordered agent records for UI and service layers."""
        return [
            {
                "id": agent.id,
                "label": agent.label,
                "description": agent.description,
                "metadata_path": str(agent.metadata_path),
                "rules_path": str(agent.rules_path),
            }
            for agent in self.agents
        ]

    def resolve(self, agent_id: str | None) -> AgentConfig:
        """Resolve selected agent id or fallback to default."""
        selected = (agent_id or "").strip()
        if selected:
            for agent in self.agents:
                if agent.id == selected:
                    return agent
            available = ", ".join(agent.id for agent in self.agents)
            raise AgentRegistryError(
                f"Unknown agent '{selected}'. Available agents: {available}"
            )

        for agent in self.agents:
            if agent.id == self.default_agent_id:
                return agent
        raise AgentRegistryError(
            f"Default agent '{self.default_agent_id}' is not defined in registry."
        )


def load_agent_registry(registry_path: Path) -> AgentRegistry:
    """Load and validate agent registry file."""
    if not registry_path.exists():
        raise AgentRegistryError(f"Missing agent registry at '{registry_path}'.")

    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise AgentRegistryError(
            f"Agent registry is not valid JSON: '{registry_path}'."
        ) from exc

    if not isinstance(payload, dict):
        raise AgentRegistryError("Agent registry root must be a JSON object.")

    raw_agents = payload.get("agents")
    if not isinstance(raw_agents, list) or not raw_agents:
        raise AgentRegistryError("Agent registry must contain non-empty 'agents' list.")

    base_dir = registry_path.parent
    agents: list[AgentConfig] = []
    seen_ids: set[str] = set()

    for index, item in enumerate(raw_agents, start=1):
        if not isinstance(item, dict):
            raise AgentRegistryError(f"Agent entry #{index} must be an object.")
        agent = _parse_agent(item, base_dir, index=index)
        if agent.id in seen_ids:
            raise AgentRegistryError(f"Duplicate agent id '{agent.id}'.")
        seen_ids.add(agent.id)
        agents.append(agent)

    default_agent_id = str(payload.get("default_agent_id", "")).strip() or agents[0].id
    if default_agent_id not in seen_ids:
        raise AgentRegistryError(
            f"default_agent_id '{default_agent_id}' does not exist in agents list."
        )

    ordered = _order_agents(agents, default_agent_id=default_agent_id)
    return AgentRegistry(default_agent_id=default_agent_id, agents=tuple(ordered))


def _parse_agent(raw: dict[str, Any], base_dir: Path, *, index: int) -> AgentConfig:
    agent_id = str(raw.get("id", "")).strip()
    if not re.fullmatch(r"[a-z0-9_]+", agent_id):
        raise AgentRegistryError(
            f"Agent entry #{index} has invalid id '{agent_id}'. "
            "Use lowercase ASCII letters, numbers, and underscore."
        )

    label = str(raw.get("label", "")).strip() or agent_id
    description = str(raw.get("description", "")).strip()
    raw_metadata_path = str(raw.get("metadata_path", "")).strip()
    if not raw_metadata_path:
        raise AgentRegistryError(
            f"Agent '{agent_id}' is missing required field 'metadata_path'."
        )
    raw_rules_path = str(raw.get("rules_path", "")).strip()
    if not raw_rules_path:
        raise AgentRegistryError(
            f"Agent '{agent_id}' is missing required field 'rules_path'."
        )

    metadata_path = Path(raw_metadata_path)
    if not metadata_path.is_absolute():
        metadata_path = (base_dir / metadata_path).resolve()
    rules_path = Path(raw_rules_path)
    if not rules_path.is_absolute():
        rules_path = (base_dir / rules_path).resolve()
    if not rules_path.exists():
        raise AgentRegistryError(
            f"Agent '{agent_id}' rules_path does not exist: '{rules_path}'."
        )

    return AgentConfig(
        id=agent_id,
        label=label,
        metadata_path=metadata_path,
        rules_path=rules_path,
        description=description,
    )


def _order_agents(agents: list[AgentConfig], *, default_agent_id: str) -> list[AgentConfig]:
    default = [agent for agent in agents if agent.id == default_agent_id]
    non_default = [agent for agent in agents if agent.id != default_agent_id]
    return default + non_default
