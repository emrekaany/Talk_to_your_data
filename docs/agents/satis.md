# Satis Agent

## Identity

- `id`: `satis`
- `label`: `Satis`
- `registry metadata_path`: `metadata_vectored_satis.json`
- `resolved file`: `metadata/agents/metadata_vectored_satis.json`

## Business Scope

- Sales and distribution analytics use cases.

## Time Filter Policy

- No agent-specific override is documented yet.
- If a request explicitly asks for a period, apply available metadata-safe period filters.
- Do not invent non-metadata columns.

## Mandatory Constraints

- Use only tables/columns/joins from metadata.
- Keep Oracle-safe SQL guardrails and row-limit policy.
