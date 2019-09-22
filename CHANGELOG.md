# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
