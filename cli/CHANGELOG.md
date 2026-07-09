# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.3.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-cli-v1.2.0...faucet-cli-v1.3.0) - 2026-07-08

### Features

- Persistent Data Movement Catalog — datasets, schema timelines & lineage graph ([#286](https://github.com/PawanSikawat/faucet-stream/pull/286))
- *(cli)* DLQ replay & management — faucet dlq inspect / replay / discard ([#281](https://github.com/PawanSikawat/faucet-stream/pull/281)) ([#285](https://github.com/PawanSikawat/faucet-stream/pull/285))
- *(cli)* Notification & incident routing — Slack / PagerDuty / webhook ([#280](https://github.com/PawanSikawat/faucet-stream/pull/280)) ([#284](https://github.com/PawanSikawat/faucet-stream/pull/284))
- *(masking)* PII detection + column-level masking policies ([#206](https://github.com/PawanSikawat/faucet-stream/pull/206))
- *(serve)* RBAC + audit log for the control plane ([#205](https://github.com/PawanSikawat/faucet-stream/pull/205)) ([#277](https://github.com/PawanSikawat/faucet-stream/pull/277))
- *(cli)* Depends_on — completion ordering between matrix rows ([#276](https://github.com/PawanSikawat/faucet-stream/pull/276))
- *(cli)* Data-freshness & volume SLA monitoring with anomaly alerts ([#275](https://github.com/PawanSikawat/faucet-stream/pull/275))
- *(cli)* Faucet test — fixture-based offline pipeline testing ([#273](https://github.com/PawanSikawat/faucet-stream/pull/273))
- *(core)* Data contracts — versioned output schema/constraints enforced per page ([#272](https://github.com/PawanSikawat/faucet-stream/pull/272))

## [1.2.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-cli-v1.1.0...faucet-cli-v1.2.0) - 2026-06-22

### Bug Fixes

- Resolve all 18 Low reliability/data-integrity findings (F40–F57, #264) ([#267](https://github.com/PawanSikawat/faucet-stream/pull/267))
- Resolve all 20 Medium reliability/data-integrity findings (F20–F39, #264) ([#266](https://github.com/PawanSikawat/faucet-stream/pull/266))
- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/PawanSikawat/faucet-stream/pull/264)) ([#265](https://github.com/PawanSikawat/faucet-stream/pull/265))
- *(triggers)* Reject unknown fields in --triggers config ([#232](https://github.com/PawanSikawat/faucet-stream/pull/232)) ([#246](https://github.com/PawanSikawat/faucet-stream/pull/246))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/PawanSikawat/faucet-stream/pull/250))

### Features

- Serve cluster Mode B — source-shard distribution across workers ([#230](https://github.com/PawanSikawat/faucet-stream/pull/230)) ([#263](https://github.com/PawanSikawat/faucet-stream/pull/263))
- *(observability)* OpenTelemetry (OTLP) trace + metric export ([#201](https://github.com/PawanSikawat/faucet-stream/pull/201)) ([#259](https://github.com/PawanSikawat/faucet-stream/pull/259))
- Schema-drift handling policy (warn/evolve/ignore/quarantine/fail) ([#194](https://github.com/PawanSikawat/faucet-stream/pull/194))
- *(sink-kafka)* Exactly-once delivery via transactional producer ([#216](https://github.com/PawanSikawat/faucet-stream/pull/216)) ([#253](https://github.com/PawanSikawat/faucet-stream/pull/253))
- Unified resilience policy (retry / circuit-breaker / poison-pill) ([#252](https://github.com/PawanSikawat/faucet-stream/pull/252))
- *(cli)* Add execution schema output ([#244](https://github.com/PawanSikawat/faucet-stream/pull/244))
- *(sink-bigquery)* Write_mode upsert/delete via in-place MERGE ([#245](https://github.com/PawanSikawat/faucet-stream/pull/245))
- Consistent snapshot → CDC replication handoff — faucet replicate ([#189](https://github.com/PawanSikawat/faucet-stream/pull/189))
