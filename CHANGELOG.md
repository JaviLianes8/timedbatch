# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0]

- First release.
- `batched_by_time`: batch an iterable by a max count and a max time window,
  flushing when either limit is reached and at the end, with an injectable clock.
