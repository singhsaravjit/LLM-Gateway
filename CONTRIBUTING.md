# Contributing

## Workflow
1. Branch from `main`: `<name>/<short-topic>` (e.g. `saravjit/redis-limiter`).
2. Keep PRs small; one component per PR where possible.
3. CI (gofmt, vet, tests, Docker build) must pass. At least **one** teammate approves.
4. Changes to `internal/api` or Redis key formats need review from **all** owners and an update to `docs/api.md`.
5. Squash-merge.

## Component ownership
| Path                                       | Owner    |
|--------------------------------------------|----------|
| `cmd/gateway`, `internal/router`, `internal/provider` | Preshit  |
| `internal/cache`                           | Dinesh   |
| `internal/ratelimit`, `internal/budget`, `internal/queue` | Saravjit |
| `cmd/mockllm`, `deploy/`, `loadtest/`, `internal/metrics` | Kanad    |

## Rules
- Never commit API keys or `.tfstate`. Use `.env` (git-ignored).
- Every new component ships with an in-memory implementation *and* tests, so the stack runs with zero setup.
- Run `terraform destroy` at the end of every AWS session.
