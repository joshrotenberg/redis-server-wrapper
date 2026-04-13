# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.1](https://github.com/joshrotenberg/redis-server-wrapper/compare/v0.4.0...v0.4.1) - 2026-04-13

### Fixed

- prevent kill_by_port from killing the calling process ([#77](https://github.com/joshrotenberg/redis-server-wrapper/pull/77))

## [0.4.0](https://github.com/joshrotenberg/redis-server-wrapper/compare/v0.3.0...v0.4.0) - 2026-04-13

### Added

- add chaos module with fault injection primitives ([#74](https://github.com/joshrotenberg/redis-server-wrapper/pull/74))
- auto-detect redis-stack-server binary and load modules ([#70](https://github.com/joshrotenberg/redis-server-wrapper/pull/70))
- add stale pidfile cleanup before server start ([#71](https://github.com/joshrotenberg/redis-server-wrapper/pull/71))
- add robust multi-layer shutdown with escalating cleanup ([#72](https://github.com/joshrotenberg/redis-server-wrapper/pull/72))
- add TLS cert generation and wire TLS through internal CLIs ([#64](https://github.com/joshrotenberg/redis-server-wrapper/pull/64))

### Other

- bump MSRV to 1.88 to fix time crate vulnerability ([#73](https://github.com/joshrotenberg/redis-server-wrapper/pull/73))

## [0.3.0](https://github.com/joshrotenberg/redis-server-wrapper/compare/v0.2.0...v0.3.0) - 2026-04-06

### Added

- *(cluster)* add per-node configuration callback ([#61](https://github.com/joshrotenberg/redis-server-wrapper/pull/61))
- *(cluster)* add per-node access and CONFIG SET helpers on handle ([#60](https://github.com/joshrotenberg/redis-server-wrapper/pull/60))
- *(cluster)* forward all cluster and replication directives to builder ([#59](https://github.com/joshrotenberg/redis-server-wrapper/pull/59))
- add remaining server directives and CLI arguments ([#58](https://github.com/joshrotenberg/redis-server-wrapper/pull/58))
- *(redis-server)* add data structure tuning directives closes #42 ([#57](https://github.com/joshrotenberg/redis-server-wrapper/pull/57))
- *(redis-server)* add memory, eviction, and lazyfree directives closes #41 ([#56](https://github.com/joshrotenberg/redis-server-wrapper/pull/56))
- *(redis-server)* add advanced TLS directives closes #40 ([#55](https://github.com/joshrotenberg/redis-server-wrapper/pull/55))
- *(redis-server)* add advanced cluster directives closes #39 ([#54](https://github.com/joshrotenberg/redis-server-wrapper/pull/54))
- *(redis-server)* add replication tuning directives closes #38 ([#53](https://github.com/joshrotenberg/redis-server-wrapper/pull/53))
- *(cli)* add advanced TLS and connection arguments ([#51](https://github.com/joshrotenberg/redis-server-wrapper/pull/51))
- redis-server: add aof tuning directives ([#50](https://github.com/joshrotenberg/redis-server-wrapper/pull/50))

## [0.2.0](https://github.com/joshrotenberg/redis-server-wrapper/compare/v0.1.0...v0.2.0) - 2026-03-30

### Added

- *(sentinel)* support multiple monitored masters closes #27 ([#33](https://github.com/joshrotenberg/redis-server-wrapper/pull/33))
- *(builders)* add logfile and extra to cluster and sentinel closes #26 ([#32](https://github.com/joshrotenberg/redis-server-wrapper/pull/32))
- *(cluster)* add password support to RedisClusterBuilder closes #25 ([#31](https://github.com/joshrotenberg/redis-server-wrapper/pull/31))
- add pid() method to RedisServerHandle and pids() to cluster/sentinel handles ([#28](https://github.com/joshrotenberg/redis-server-wrapper/pull/28))

### Fixed

- *(cluster)* clear stale node dirs closes #24 ([#30](https://github.com/joshrotenberg/redis-server-wrapper/pull/30))

### Other

- pre-release documentation audit closes #34 ([#36](https://github.com/joshrotenberg/redis-server-wrapper/pull/36))
- add forza.toml configuration ([#29](https://github.com/joshrotenberg/redis-server-wrapper/pull/29))
- release v0.1.0 ([#21](https://github.com/joshrotenberg/redis-server-wrapper/pull/21))

### Added

- *(server)* add `detach()` to async and blocking server handles so they can be consumed without shutting down the Redis process closes #24

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
