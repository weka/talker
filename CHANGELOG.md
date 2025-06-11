# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Add retry mechanism for Redis operations in client and agent to improve resilience against transient Redis issues.

### Fixed
- Agent: Ignore pending exception from a previous run if it's a Redis error, preventing unnecessary error states.

## [1.9.4] - 2022-11-09
### Fixed
- parse command OSError.strerror to bytes
- fix travis build links
- start talker agent logging after configure logging
- fix travis python3.7 package installation issue
- fix is_alive function

### Added
- allow stop waiting on command when the host isn't responsive
- add agent talker daemon systemd support

### Changed
- log only once when reactor gets a command
- add talker details to NoResponseForRedisCommand exception

## [1.9.3] - 2021-09-23
### Fixed
- pass ClientCommandTimeoutError client timer elapsed time as timeout parameter

## [1.9.2] - 2021-07-13
### Fixed
- improve reactor pipeline execution error handling
- add missing 'bash -ce' to sync and reboot in 'reboot_host'
- make client is_alive resilient

### Added
- log talker reboot stdout and stderr in case reboot didn't complete

## [1.9.1] - 2020-07-19
### Fixed
- support ack_timeout None value
- when killing a talker job wait until process is terminated (remove hanging job mechanism)
- handle properly corrupted seen jobs file

## [1.9.0] - 2020-03-05
### Fixed
- sync all files before reboot

### Added
- add support for client to pass ack_timeout
- add is_alive to client api
- raise special exception on client redis connection error

## [1.8.9] - 2020-02-05
### Fixed
- agent syncs only reboot sentinel and log file before reboot
- make all agent threads thread-safe
- check recover from reboot only when there is no previous exception
  <br/>or orphaned job

## [1.8.8] - 2020-01-28
### Added
- Running diagnostics on startup and when uncaught exception occurs

### Fixed
- fix dict items iteration bug happening in python3

## [1.8.6] - 2019-11-05
### Added
- Redis connection parameters in agent config file

### Changed
- raise client's Redis socket timeout
- using .ini file for agent configuration

## [1.8.4] - 2019-09-22
### Fixed
- fix reactor ASYNC_COMMANDS memory leak
- fix missing *wait* in polling completed commands
- fix redis.exceptions.ConnectionError:<br/>
  Error while reading from socket: (104, 'Connection reset by peer')

## [1.8.3] - 2019-09-05
### Changed
- upgrade Redis client version to 3.3.7
- change agent installation script to allow remote installation

### Fixed
- fix get process ID race
- fix talker termination when trying to kill an existed process

## [1.8.2] - 2019-08-13
### Added
- fix installation via pypi

## [1.8.1] - 2019-08-13
