# Contributing

This is a small teaching project.  Its value is in being correct and
auditable rather than in having many features, so the rules below are
about evidence, not ceremony.

## Before you send a change

```
python -m pip install pytest pytest-cov hypothesis mypy ruff
ruff check .
mypy                         # strict; configured in pyproject.toml
pytest --cov=minikv          # the coverage floor (85%) is in pyproject.toml
```

CI runs the same three commands on Linux, macOS and Windows for Python
3.9, 3.12 and 3.13.  A change that only passes on one of them is not done.

**The library is strictly typed; tests and benchmarks are not checked.**
The package must pass `mypy --strict`, which is why a stored entry is
modelled as `Value = bytes | Tombstone` rather than `object`, and why a
reader narrows with `isinstance` instead of comparing against the
sentinel: only the former tells the checker anything.

`mypy` resolves modules for the platform it runs on, so if you touch
platform-specific code, check the other platforms too -- this bit CI
once, passing on Windows and failing on Linux and macOS:

```
mypy --platform linux
mypy --platform darwin
```

## What a change needs to include

- **A test that fails without it.**  Every guarantee in README.md has at
  least one test; new behaviour needs the same.  Bugs get a regression
  test that reproduces the bug.
- **Real files, not mocks.**  The suite uses temporary directories and
  child processes on purpose: recovery, checksums and crash behaviour are
  the point of this project, and a mocked filesystem cannot fail the way
  a real one does.
- **Numbers, not adjectives**, if the change is about performance.  Run
  `benchmarks/bench.py` before and after and put both numbers in the pull
  request.  If a change is not measured, it is not an optimization.
- **An honest note**, if a change gives something up.  The manifest fsync
  that cost 14% on writes and the cached handle that made compaction
  defer deletion are both documented rather than hidden; follow that.

## On-disk format changes

Bumping the format means bumping `SST_MAGIC` in `minikv/encoding.py`.
There is no migration path: the project is a teaching engine and the
tests build every store from scratch.  Say so in the changelog if you do
it.

## Style

`ruff` decides.  Docstrings explain *why* a thing is the way it is -- the
`why` is what a reader came for -- and inline comments are for the parts
that look wrong until you know the reason.

## Scope

Out of scope, and pull requests for them will be closed: network or
client/server operation, replication, multi-process writers, secondary
indexes, TTL, SQL, and any runtime dependency outside the standard
library.  Leveled compaction and background compaction are explicitly
listed as future work in DESIGN.md, so those are in scope if they come
with measurements.
