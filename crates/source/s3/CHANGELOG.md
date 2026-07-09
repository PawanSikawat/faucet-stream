# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.3.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-source-s3-v1.2.0...faucet-source-s3-v1.3.0) - 2026-07-08

### Features

- Extend cluster Mode B sharding to mysql, mssql, sqlite, gcs, and parquet sources ([#271](https://github.com/PawanSikawat/faucet-stream/pull/271))

## [1.2.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-source-s3-v1.1.0...faucet-source-s3-v1.2.0) - 2026-06-22

### Bug Fixes

- *(s3,gcs)* Verify object read integrity — length + opt-in checksum ([#161](https://github.com/PawanSikawat/faucet-stream/pull/161)) ([#257](https://github.com/PawanSikawat/faucet-stream/pull/257))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/PawanSikawat/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/PawanSikawat/faucet-stream/pull/240))

### Features

- Serve cluster Mode B — source-shard distribution across workers ([#230](https://github.com/PawanSikawat/faucet-stream/pull/230)) ([#263](https://github.com/PawanSikawat/faucet-stream/pull/263))
