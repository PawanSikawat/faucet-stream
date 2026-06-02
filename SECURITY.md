# Security Policy

## Supported versions

faucet-stream has reached **1.0**. Security fixes are applied to the latest
published `1.x` release on crates.io and to `main`. Older releases — including
all pre-1.0 (`0.x`) versions — are no longer maintained; please upgrade to the
latest release before reporting.

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues,
discussions, or pull requests.**

Report privately through either channel:

1. **GitHub private vulnerability reporting** (preferred) — open the repository's
   **Security** tab and click **"Report a vulnerability"**. This creates a private
   advisory visible only to maintainers.
2. **Email** — **pawanksikawat@gmail.com**.

Please include:

- a description of the vulnerability and its impact,
- the affected crate(s) and version(s),
- steps to reproduce or a proof of concept,
- any suggested remediation.

## What to expect

- We aim to acknowledge a report within a few days.
- We'll confirm the issue, determine affected versions, and work on a fix.
- We'll keep you informed of progress and coordinate a disclosure timeline.
- With your consent, we'll credit you in the advisory and release notes.

## Scope notes

faucet-stream moves data between external systems, so keep in mind:

- **Credentials** belong in environment variables / secret files referenced via
  `${env:VAR}` / `${file:PATH}` / `${secret:VAR}` — never commit them to config
  files. A leaked credential in your own config is not a vulnerability in
  faucet-stream.
- SQL identifier handling (`quote_ident`), bind-parameter substitution, and
  JSON-safe interpolation are security-relevant — bug reports about injection
  vectors in these paths are especially welcome.
