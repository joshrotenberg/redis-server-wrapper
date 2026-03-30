# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/joshrotenberg/redis-server-wrapper/releases/tag/v0.1.0) - 2026-03-30

### Added

- *(blocking)* add blocking module and feature flags closes #11 ([#17](https://github.com/joshrotenberg/redis-server-wrapper/pull/17))
- add CI and release-plz GitHub Actions workflows
- *(server)* convert RedisServer to async closes #9
- *(cli)* add tokio dependency and convert RedisCli to async closes #8
- add redis-run example CLI for standalone, cluster, and sentinel topologies closes #1
- initial release -- redis-server and redis-cli wrapper

### Fixed

- *(ci)* install redis-server in CI and drop Windows from test matrix

### Other

- *(readme,cargo)* add badges and verify crates.io readiness closes #19 ([#20](https://github.com/joshrotenberg/redis-server-wrapper/pull/20))
- *(readme,tests)* update README to async API and add blocking tests closes #12 ([#18](https://github.com/joshrotenberg/redis-server-wrapper/pull/18))
- first
