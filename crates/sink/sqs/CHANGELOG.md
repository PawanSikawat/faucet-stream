# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.0.0] - 2026-07-28

### Features

- Initial release: AWS SQS sink — batched `SendMessageBatch` writes (≤10 entries
  / ≤256 KiB per request), bounded request concurrency, per-entry
  partial-failure retry, optional FIFO `message_group_id` /
  `message_deduplication_id`, and DLQ-routable per-record outcomes. Conformance
  battery wired ([#412](https://github.com/PawanSikawat/faucet-stream/issues/412)).
