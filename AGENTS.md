# AGENTS.md

## Purpose and ownership

`helianthus-eebus-go` owns Helianthus integration glue for eeBUS. It composes
upstream SHIP/SPINE capabilities into Helianthus-facing runtime integration
without redefining the upstream protocols or absorbing cross-protocol semantic
ownership.

`helianthus-ship-go` and `helianthus-spine-go` are temporary upstream
dependencies, not Helianthus-owned product layers. Do not add Helianthus policy
to them, modify them from this repository, or expose their internal types as
protocol-neutral contracts. Prefer released upstream capabilities when they
meet the required behavior.

## Workflow

1. Reconcile `helianthus-v0.7`, local changes, linked issue, and open pull
   requests before editing.
2. Create `issue/<number>-<slug>` from `origin/helianthus-v0.7` and keep the
   change within the issue acceptance criteria.
3. Use RED-first tests for behavioral, protocol, lifecycle, retry, persistence,
   or concurrency changes. Preserve last-known-good fields on partial failures
   where the owning contract permits it.
4. Run `gofmt -w` on changed Go files, `go test ./...`, and `git diff --check`
   before pushing; run any additional repository CI command required by the
   changed area.
5. Open a linked pull request that records commands, results, scope, and
   residual risk.
6. Obtain a fresh exact-HEAD blocker review and resolve P0-P2 findings before
   merge. Squash merge only with green applicable checks and review, then verify
   `helianthus-v0.7` remotely. Never merge without requested authorization.

## Safety, evidence, and privacy

- Keep SHIP transport/session management separate from connected-only SPINE
  topology and service data; a trusted offline peer is not browseable topology.
- Use bounded, fail-closed discovery and retries. Live pairing, device writes,
  credentials, private keys, and trust-store mutation require an explicit
  contract and action-time operator authorization.
- Do not commit credentials, private captures, private network details, serials,
  account data, trust-store bytes, or device fingerprints.
- Base durable claims on publishable evidence, mark inference and unknowns, and
  do not reproduce restricted-source material.

## Public references

- [Helianthus eeBUS integration](https://github.com/Project-Helianthus/helianthus-eebus-go)
- [Canonical eeBUS documentation](https://github.com/Project-Helianthus/helianthus-docs-eebus)
- [Upstream enbility/eebus-go](https://github.com/enbility/eebus-go)
- [EEBUS Initiative](https://www.eebus.org/)
