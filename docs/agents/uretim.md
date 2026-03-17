# Uretim Agent

## Identity

- `id`: `uretim`
- `label`: `Uretim`
- `registry metadata_path`: `metadata_vectored_uretim.json`
- `resolved file`: `metadata/agents/metadata_vectored_uretim.json`

## Business Scope

- Policy production and premium analytics use cases.

## Time Filter Policy (Source of Truth)

- Time filter is NOT globally mandatory for this agent.
- Do not fail a query only because there is no explicit time filter.
- If user request explicitly contains a time scope, apply a metadata-valid time predicate.
- For tanzim-period requests, prefer ek tanzim-compatible date context and avoid forcing `REPORT_PERIOD` column predicates.

## Mandatory Constraints

- Use only tables/columns/joins from metadata.
- Keep Oracle-safe SQL guardrails and row-limit policy.
