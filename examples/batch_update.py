"""Offline demo: edits + deletes, simulated provider failure, durable resume."""
from pathlib import Path
from tempfile import TemporaryDirectory
from contribfold import Store


def build_summary(job):
    # Replace this deterministic fake with YOUR model/embedding call.
    # job.idempotency_key can be forwarded if the provider supports it.
    return {
        "summary": " / ".join(c.text for c in job.contributions),
        "source_documents": [c.document for c in job.contributions],
    }


def main():
    with TemporaryDirectory() as folder:
        path = str(Path(folder) / "knowledge.sqlite")
        with Store(path) as kb:
            kb.stage("initial-import", upserts={
                "manual-v1": {"pricing": "$10/month", "support": "Email only"},
                "release-notes": {"pricing": "$10/month", "support": "Email and chat"},
                "faq": {"pricing": "$10/month", "export": "CSV export"},
            })
            kb.run("initial-import", build_summary)
            print("BEFORE:", kb.outputs())
            kb.stage("october-update", deletes=["manual-v1"], upserts={
                "release-notes": {"pricing": "$12/month", "support": "Email and chat"},
                "faq": {"pricing": "$12/month", "export": "CSV and JSON export"},
            }, expected_revision=kb.revision)
            completed = []
            def flaky(job):
                if job.target == "pricing":
                    raise RuntimeError("simulated provider outage")
                completed.append(job.target)
                return build_summary(job)
            try:
                kb.run("october-update", flaky)
            except RuntimeError:
                print("PAUSED; durably finished:", completed)
            print("No half-update visible:", kb.outputs()["pricing"]["summary"])
        # New process/connection can resume from the same file and ID.
        with Store(path) as kb:
            resumed = []
            kb.run("october-update", lambda job: resumed.append(job.target) or build_summary(job))
            print("RESUMED targets:", resumed)
            print("AFTER:", kb.outputs())


if __name__ == "__main__":
    main()
