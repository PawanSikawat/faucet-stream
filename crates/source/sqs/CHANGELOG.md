# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.0.0] - 2026-07-28

### Features

- Initial release: AWS SQS source — long-polls `ReceiveMessage`, buffers to
  `batch_size` and streams pages with bounded memory, deletes each page's
  receipt handles before yielding (at-least-once), and terminates on
  `idle_timeout_secs` / `max_messages`. Conformance battery wired
  ([#412](https://github.com/PawanSikawat/faucet-stream/issues/412)).
