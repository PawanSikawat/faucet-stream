# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.0.0] - 2026-07-28

### Features

- Initial release: SFTP sink connector — writes records to an SFTP server as
  JSON Lines objects under a remote directory. Atomic writes (upload to a
  temporary name, then rename into place), append-only, lazy connect with a
  reused session, conformance battery wired ([#410](https://github.com/PawanSikawat/faucet-stream/issues/410)).
