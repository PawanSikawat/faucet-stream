# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.2.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-mysql-v1.2.0...faucet-sink-mysql-v1.2.1) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-mysql-v1.1.0...faucet-sink-mysql-v1.2.0) - 2026-06-22

### Bug Fixes

- Resolve all 20 Medium reliability/data-integrity findings (F20–F39, #264) ([#266](https://github.com/PawanSikawat/faucet-stream/pull/266))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/PawanSikawat/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/PawanSikawat/faucet-stream/pull/240))

### Features

- Schema-drift handling policy (warn/evolve/ignore/quarantine/fail) ([#194](https://github.com/PawanSikawat/faucet-stream/pull/194))
