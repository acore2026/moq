# Examples Overview

This directory is organized around the main MOQ protocol flows and a few practical usage patterns.

## Core Examples

- `relay_example.py`: starts a caching relay with disk-backed storage.
- `publisher_example.py`: publishes objects to a relay using both stream and datagram transport.
- `subscriber_example.py`: subscribes to a track and receives live objects.
- `fetch_example.py`: fetches historical objects by range.
- `reconnection_example.py`: demonstrates reconnect and cache-assisted recovery.
- `integration_example.py`: shows how to embed MOQ Transport in an external application.
- `basic_example.py`: one-process quick start for relay, publisher, and subscriber roles.

## Recommended Run Order

1. Start `relay_example.py`.
2. Run `publisher_example.py`.
3. Run `subscriber_example.py`.
4. Try `fetch_example.py` after objects have been published.
5. Use `reconnection_example.py` to exercise cache and reconnect behavior.
6. Read `integration_example.py` for application integration patterns.

## Notes

- Example scripts now resolve the repository root automatically, so they can be run from the repo root without editing `sys.path`.
- `integration_example.py` supersedes the older `external_usage_example.py` script.
