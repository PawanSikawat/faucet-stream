# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.2.2](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-iceberg-v1.2.1...faucet-sink-iceberg-v1.2.2) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-iceberg-v1.2.0...faucet-sink-iceberg-v1.2.1) - 2026-07-17

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-iceberg-v1.1.1...faucet-sink-iceberg-v1.2.0) - 2026-07-10

### Features

- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/PawanSikawat/faucet-stream/pull/289))

## [1.1.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-iceberg-v1.1.0...faucet-sink-iceberg-v1.1.1) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-iceberg-v1.0.0...faucet-sink-iceberg-v1.1.0) - 2026-06-22

### Bug Fixes

- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/PawanSikawat/faucet-stream/pull/264)) ([#265](https://github.com/PawanSikawat/faucet-stream/pull/265))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/PawanSikawat/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/PawanSikawat/faucet-stream/pull/240))

### Features

- *(sink-iceberg)* Opt-in orphan cleanup on definitive commit failure ([#193](https://github.com/PawanSikawat/faucet-stream/pull/193)) ([#260](https://github.com/PawanSikawat/faucet-stream/pull/260))
- Schema-drift handling policy (warn/evolve/ignore/quarantine/fail) ([#194](https://github.com/PawanSikawat/faucet-stream/pull/194))
