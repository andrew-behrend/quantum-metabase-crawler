# Backlog

## Pre-Prod Required

- [x] Implement formalized error-handling framework after retrieval scope is largely complete (to avoid rework during active endpoint expansion).
- [x] Enhance duplicate-name analysis beyond simple normalization (for example fuzzy matching / similarity thresholds) after baseline review workflows are stable.
- [x] Rename Phase 6 output/report/query filenames to remove the `phase6_` prefix and align with the standard output naming convention.
- [ ] Move from system Python to a managed runtime (pyenv/Homebrew Python) linked to OpenSSL 1.1.1+.
- [ ] Create and use a project virtual environment for all runs.
- [ ] Remove real credentials from tracked files and add `.env` to `.gitignore`.
- [ ] Define production credential strategy (secret manager or CI/CD injected env vars).
- [x] Add request retry/backoff behavior for transient 429/5xx responses.
- [x] Add crawl run summary metadata (run id, start/end time, endpoint status table).
- [x] Add configurable request timeout and max retries via env vars.
- [x] Add clear non-zero exit codes by failure type (config/auth/network/api/write).
- [x] Add basic tests for config loading, auth failure handling, and file outputs.
- [x] Add README setup for local and production modes.

## Temporary Dev Workaround

- [x] Pin `urllib3<2` to suppress LibreSSL warning in current local Python runtime.
