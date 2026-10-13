# Contributing

Use Python 3.10 or newer. Install locally with `python -m pip install -e .`.
Run `python -m unittest discover -s tests -v`, then both scripts in `examples/`.
Tests use temporary SQLite databases and fake reducers; no credentials or network are required.

For concurrency changes, cover expiry/reclaim, stale completion, simultaneous workers, revision conflicts, and process death. Keep external side effects outside reducers used in tests. Never commit private document data or database files.

Build with `python -m pip install build` and `python -m build`. Validate both the wheel and sdist in clean environments before releasing. Release credentials and publishing are owner-controlled; CI does not publish packages.
