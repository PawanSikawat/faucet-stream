# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.0.0] - 2026-07-28

### Features

- Initial release: shared `SqsCredentials` auth enum + `build_client` helper for
  the AWS SQS source and sink connectors ([#412](https://github.com/faucet-hq/faucet-stream/issues/412)).
