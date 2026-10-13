# contribfold

**Apply a batch of knowledge-base document edits and deletions, rebuild each affected topic once, and resume unfinished work after failure.**

`contribfold` is a small Python library for the **contribution → derived artifact** layer of a RAG application. Your extractor supplies which text each document contributes to which entity/topic. Your reducer produces the summary, embedding, or other JSON artifact. This library coalesces changes, persists work, and publishes the resulting document state and artifacts together in a dedicated SQLite database.

It does not extract entities, call a model, operate your vector database, or automatically integrate with LightRAG. It is not a new agent framework. No runtime dependencies, accounts, telemetry, or network calls. Python 3.10+; MIT; initial alpha release 0.1.0.

## The useful difference

If twelve changed documents share a topic, rebuilding that topic after each document wastes work. Stage those twelve changes together; the reducer sees only the final surviving contributions and runs once for that affected topic in a normal successful run. Unaffected artifacts are preserved. A target with no surviving contributions is removed without calling the reducer.

Completed reducer results survive restarts. Interrupted in-flight calls can run again: **exactly-once external API execution is not guaranteed**. See [failure semantics](docs/semantics.md).

## Install

This source tree includes buildable packaging. Until a PyPI release is actually published, install from a local checkout or the supplied wheel:

```sh
python -m pip install .
# Install the published source directly from GitHub:
# python -m pip install "git+https://github.com/jum-apzn/contribfold.git"
# or: python -m pip install dist/contribfold-0.1.0-py3-none-any.whl
```

## Five-minute example

```python
from contribfold import Store

# A deterministic example. Replace the body with your own model/embedder.
def rebuild(job):
    return {"summary": " / ".join(c.text for c in job.contributions)}

with Store("knowledge.sqlite") as kb:
    kb.stage("import-001", upserts={
        "guide": {"pricing": "$10/month", "support": "Email only"},
        "faq": {"pricing": "$10/month", "export": "CSV export"},
    }, reducer_version="summary-prompt-1/model-1")
    kb.run("import-001", rebuild)

    kb.stage("update-002", deletes=["guide"], upserts={
        "faq": {"pricing": "$12/month", "export": "CSV export"},
    }, reducer_version="summary-prompt-1/model-1")
    kb.run("update-002", rebuild)
    print(kb.outputs())
    # {'export': {'summary': 'CSV export'},
    #  'pricing': {'summary': '$12/month'}}
```

`pricing` rebuilds once, `support` disappears, `export` keeps its existing result. The original source files are never opened or modified. An upsert is a **full replacement** of one document's contribution map, not a partial merge.

## Recover from a failed run

Reopen the same SQLite file and call `kb.run("update-002", rebuild)` again. Ready jobs are skipped; a failed callback is returned to pending. Hard process death leaves a running job leased until expiry (default 300 seconds), after which a worker can reclaim it.

`run()` returns a `Batch`. Check `batch.state == "committed"`; `"staged"` can mean another worker still owns unfinished work. It does not wait for other workers or silently start a background loop.

## Concurrency and versioning

- Use a separate `Store` connection in each thread/process. Workers may claim different jobs in the same batch.
- Give every logical batch a unique durable ID. Reusing it with identical intent is a no-op; different intent raises `BatchMismatchError`.
- Independent batches are optimistic snapshots. If another batch commits first, the stale batch raises `ConflictError`; abort it and stage your still-wanted changes under a new ID. No automatic destructive rebasing.
- `expected_revision=kb.revision` can protect a user-reviewed change against intervening commits.
- Set `reducer_version` to identify the model, prompt, output schema, and any relevant configuration. Changing it rebuilds **all live targets**. Use it consistently across every call to `stage`.
- Set a lease longer than callback latency, or explicitly renew it through `heartbeat()` from a separate connection. Late results cannot overwrite reclaimed jobs.

## API

- `Store(path)`: open a new/dedicated contribfold SQLite database.
- `stage(id, upserts={}, deletes=(), reducer_version="v1", expected_revision=None)` → `Batch`.
- `run(id, reducer, lease_seconds=300)` → `Batch`; synchronous convenience worker.
- `claim(id)` → `WorkItem | None`; `complete(item, json_value)`; `release(item)`; `heartbeat(item)`: explicit worker control.
- `commit(id)` → revision; atomically publishes a ready batch, idempotently.
- `status(id)` → batch state and pending/running/ready counts.
- `documents()` / `outputs()`: committed snapshots as copied dictionaries.
- `abort(id)`: invalidate a staged batch and remove its jobs/results, keeping intent for deduplication.
- `forget(id)`: logically remove a terminal batch journal, **including its deduplication record**.

A `WorkItem` provides target ID, sorted `(document, text)` contributions, reducer version, and a stable `idempotency_key`. Forward the key only to providers that document idempotent request handling. A reducer may return JSON-compatible summaries and vectors, e.g. `{"summary": "...", "embedding": [0.1, 0.2]}`. Serialization is strict (no NaN/Infinity).

## Run real, offline examples

```sh
python examples/batch_update.py
python examples/call_count.py
python -m unittest discover -s tests -v
```

The second example uses 14 synthetic documents, deletes 10 and updates 2. Sequential updates perform **12 fake reducer calls**; one batch performs **1**, with identical final output. No paid LLM is called. This demonstrates call coalescing, not real-world cost or speed improvement.

## Limits and data retention

This is a single-machine, local-filesystem SQLite core, not a distributed service. Staging currently loads all committed contributions in memory and scans them for affected targets. It is intended for modest knowledge bases and prototypes; benchmark your workload before adopting it at scale.

Atomicity covers only this database. Your external vector index is not updated by `commit`. Export a committed snapshot into a versioned index and atomically switch your application pointer if you need cross-system publication. Reducers should generate values, not send emails or mutate remote stores.

Until commit, readers see the **old committed snapshot**, including documents staged for deletion. This is deliberate all-or-nothing publication, not immediate revocation. Do not use it alone for urgent privacy/authorization removal.

Historical batch intent and job inputs retain contribution text after document deletion. `abort` also retains intent. Explicit `forget` removes the terminal journal logically but loses duplicate-batch protection; SQLite pages, WAL files, backups, and exported artifacts can retain data. This package is **not a secure erasure system**. Protect the database with OS permissions; it is not encrypted. Never open an untrusted database or feed private content to an external reducer without authorization.

## Background and scope

The practical problem is visible in [LightRAG's open batch-rebuild RFC](https://github.com/HKUDS/LightRAG/issues/3659) and [RAGFlow's incremental graph request](https://github.com/infiniflow/ragflow/issues/15791). LightRAG already supports individual document deletion and rebuilding. `contribfold` independently implements a narrow batch coalescing/persistence core; it does not copy or implement that RFC wholesale and is not an official adapter. Existing incremental systems overlap with this problem; no worldwide novelty claim is made. See [design and comparisons](docs/design.md).
