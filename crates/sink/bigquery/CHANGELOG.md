# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.2.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-bigquery-v1.2.0...faucet-sink-bigquery-v1.2.1) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-bigquery

## [1.2.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-bigquery-v1.1.0...faucet-sink-bigquery-v1.2.0) - 2026-06-22

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/PawanSikawat/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/PawanSikawat/faucet-stream/pull/240))

### Features

- Schema-drift handling policy (warn/evolve/ignore/quarantine/fail) ([#194](https://github.com/PawanSikawat/faucet-stream/pull/194))
- *(sink-bigquery)* Write_mode upsert/delete via in-place MERGE ([#245](https://github.com/PawanSikawat/faucet-stream/pull/245))
