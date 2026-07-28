# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.0.0] - 2026-07-28

### Features

- Initial release: DuckDB sink — writes JSON records to a DuckDB table via a
  JSON column or auto-mapped columns, each batch a transaction-wrapped
  multi-row INSERT. Conformance battery wired ([#413](https://github.com/PawanSikawat/faucet-stream/issues/413)).
