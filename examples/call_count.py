"""Synthetic call-count comparison, NOT an LLM latency or cost benchmark."""
from pathlib import Path
from tempfile import TemporaryDirectory
from contribfold import Store


def scenario(path, batched):
    calls = []
    def reducer(job):
        calls.append(job.target)
        return [item.text for item in job.contributions]
    with Store(path) as kb:
        kb.stage("seed", upserts={f"doc-{i}": {"shared-topic": f"fact-{i}"} for i in range(14)})
        kb.run("seed", reducer)
        calls.clear()
        deletes = [f"doc-{i}" for i in range(10)]
        upserts = {"doc-10": {"shared-topic": "revised-10"}, "doc-11": {"shared-topic": "revised-11"}}
        if batched:
            kb.stage("one-batch", deletes=deletes, upserts=upserts)
            kb.run("one-batch", reducer)
        else:
            for i, doc in enumerate(deletes):
                kb.stage(f"delete-{i}", deletes=[doc])
                kb.run(f"delete-{i}", reducer)
            for i, (doc, values) in enumerate(upserts.items()):
                kb.stage(f"update-{i}", upserts={doc: values})
                kb.run(f"update-{i}", reducer)
        return len(calls), kb.outputs()


def main():
    with TemporaryDirectory() as folder:
        sequential, a = scenario(str(Path(folder) / "sequential.sqlite"), False)
        batched, b = scenario(str(Path(folder) / "batched.sqlite"), True)
        assert a == b
        assert (sequential, batched) == (12, 1)
        print(f"Synthetic reducer calls: sequential={sequential}, batched={batched}; identical final outputs")
        print("No model was called. This does not measure real API cost, tokens, or wall-clock speed.")


if __name__ == "__main__":
    main()
