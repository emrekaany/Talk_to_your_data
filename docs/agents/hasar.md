# Hasar Agent

## Identity

- `id`: `hasar`
- `label`: `Hasar`
- `registry metadata_path`: `metadata_vectored_hasar.json`
- `resolved file`: `metadata/agents/metadata_vectored_hasar.json`

## Business Scope

- Claims and damage analytics use cases.

## Time Filter Policy

- No agent-specific override is documented yet.
- If a request explicitly asks for a period, apply available metadata-safe period filters.
- Do not invent non-metadata columns.

## Mandatory Constraints

- Use only tables/columns/joins from metadata.
- Keep Oracle-safe SQL guardrails and row-limit policy.
