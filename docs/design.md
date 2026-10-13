# Design and prior art

Research checked 2026-10-13.

- [LightRAG RFC #3659](https://github.com/HKUDS/LightRAG/issues/3659) is open and describes repeated rebuilding during batch document deletion. Its proposed solution reuses LightRAG's existing purge journals, performs incremental per-document deletion, and explicitly excludes all-or-nothing batches. contribfold instead owns a dedicated SQLite store, stages mixed upserts/deletions, and publishes an entire batch atomically. It is not a LightRAG patch and cannot be pointed at a LightRAG database.
- [RAGFlow #15791](https://github.com/infiniflow/ragflow/issues/15791) requests incremental graph updates. This package does not provide graph extraction, community detection, or a RAGFlow connector.
- [CocoIndex](https://github.com/cocoindex-io/cocoindex) provides much broader incremental data processing. contribfold is only a small source-contribution folding engine with an explicit publish boundary; it does not claim incremental processing is new.

No upstream implementation code is copied. The motivation and basic optimization are established ideas. Novelty claims should be limited to this package's particular API/semantics, not a claim that no one has solved related problems.

## Why contribution maps?

A document can affect several targets, and a target can be supported by several documents. Full per-document replacement makes removing obsolete associations unambiguous. Sorting contribution inputs ensures deterministic keys. The reducer, not the library, owns domain semantics such as conflicting facts, date priority, source reliability, summary wording, and vector generation.

## Deliberate first-release limits

- String document and target IDs; one nonempty text contribution per document/target.
- JSON-compatible output artifacts; no arbitrary Python pickle payloads.
- In-memory staging: roughly O(documents + affected targets × documents) work.
- Coarse database revision conflict detection.
- Synchronous callback convenience API; async callers can coordinate explicit claims themselves.
- No integrated network adapters, paid model calls, vector-store writes, migrations, garbage-collection daemon, or secure erase.

These limits keep the actual atomicity boundary understandable. Adapters should be added only with end-to-end integration tests, rather than advertised from a generic callback alone.
