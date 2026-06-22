# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.2.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-source-rest-v1.1.0...faucet-source-rest-v1.2.0) - 2026-06-22

### Bug Fixes

- Resolve all 18 Low reliability/data-integrity findings (F40–F57, #264) ([#267](https://github.com/PawanSikawat/faucet-stream/pull/267))
- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/PawanSikawat/faucet-stream/pull/264)) ([#265](https://github.com/PawanSikawat/faucet-stream/pull/265))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/PawanSikawat/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/PawanSikawat/faucet-stream/pull/240))

### Features

- Unified resilience policy (retry / circuit-breaker / poison-pill) ([#252](https://github.com/PawanSikawat/faucet-stream/pull/252))
