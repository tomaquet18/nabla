# Contributing to nabla

## Contributor License Agreement

Every contribution requires a signed [Contributor License Agreement](CLA.md).
Signing takes one comment on your first pull request:

    I have read the CLA Document and I hereby sign the CLA

A check on the pull request records the signature and applies it to all your
future contributions. Pull requests from unsigned contributors are not merged,
no matter how small the change. Read the agreement before signing; it grants
the project the right to distribute your contribution under other licenses,
which is what allows the components below to carry different ones.

## Licenses by component

| Component | Path | License |
|---|---|---|
| nabla PostgreSQL extension | `src/`, `sql/`, `nabla.control` | AGPL-3.0-or-later |
| reference client and subscription protocol | `clients/` | MIT OR Apache-2.0 |
| delta engine crate (once split out of the extension) | — | Apache-2.0 |

Each source file carries an `SPDX-License-Identifier` header naming its
license. New files must carry the header of the component they belong to.

## Development

Everything builds and runs inside a Linux container; see `scripts/dev.sh`:

```
scripts/dev.sh build        # compile the extension
scripts/dev.sh test         # run tests/integration.sh against a throwaway cluster
scripts/dev.sh bench        # worker throughput benchmark
scripts/dev.sh playground   # a cluster with nabla installed, for hands-on use
```

A change is complete when the integration suite passes and, for anything that
touches the worker's apply path, the benchmark does not regress.

## Commits and pull requests

- Conventional commit messages (`feat:`, `fix:`, `perf:`, `docs:`, `chore:`),
  imperative mood, body explaining why.
- One reviewable change per pull request; keep tests and documentation in the
  same commit as the code they cover.
- Do not add license headers other than the SPDX line, and do not change the
  license of any component.
