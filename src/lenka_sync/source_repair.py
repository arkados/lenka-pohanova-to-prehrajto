"""Find replacement sources in a producer separate from the upload workers."""

from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import time

from . import policy
from .discovery import CzechDiscovery, EnglishDiscovery
from .language import WhisperLanguageDetector
from .metadata import Metadata
from .series.episodes import EpisodeSourceProvider
from .series.resilience import error_evidence
from .storage import Publisher, atomic_json, read_json, rows

FEED_PATH = "state/source-repairs.json"
SEARCH_PATH = "state/source-repair-search.json"
REPAIR_POLICY = "verified-episode-source-repair-v1"


def source_key(selected):
    return f"{selected.get('source_id')}:{selected.get('url') or selected.get('source_url')}"


def repair_key(variant, identity):
    return f"{variant}:{identity}"


def repair_token(entry):
    payload = {key: value for key, value in entry.items() if key != "token"}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def select_repair(row, record, feed):
    """Read a verified replacement without changing an allocated or completed upload."""
    if record.get("upload") or record.get("prepared_target") or record.get("claim") or record.get("review_target_id"):
        return row
    entry = feed.get("repairs", {}).get(repair_key(row["variant"], row["identity"]))
    if not entry:
        return row
    if (
        entry.get("policy") != REPAIR_POLICY
        or entry.get("identity") != row["identity"]
        or entry.get("variant") != row["variant"]
        or entry.get("token") != repair_token(entry)
    ):
        raise ValueError("Source repair identity or verification token is invalid")
    current = source_key(record.get("source") or row["selected"])
    consumed = record.get("source_repair_token") == entry["token"] and current == source_key(entry["selected"])
    if current != entry["base"] and not consumed:
        return row
    selected = copy.deepcopy(row)
    selected["selected"] = entry["selected"]
    for field in ("subtitles", "original_language"):
        if field in entry:
            selected[field] = entry[field]
    selected["source_repair"] = {"token": entry["token"], "base": entry["base"]}
    if not policy.upload_eligible(selected):
        raise ValueError("Replacement source did not pass the variant policy")
    return selected


def has_new_repair(row, record):
    repair = row.get("source_repair", {})
    return bool(repair.get("token") and (repair["token"] != record.get("source_repair_token")
                or source_key(record.get("source", {})) != source_key(row["selected"])))


def repair_requests(queue, states, feed, progress, *, now=None):
    """The uploader's failed-source records form a durable producer queue."""
    now = now or datetime.now(UTC)
    requested = []
    for variant, selected in queue.items():
        records = states[variant].get("episodes", {})
        for source in selected:
            record = records.get(source["identity"], {})
            if record.get("upload") or record.get("prepared_target") or record.get("claim") or record.get("review_target_id"):
                continue
            attempts = record.get("attempts", [])
            if not record.get("source_repair_requested") and (
                not attempts or attempts[-1].get("error") != "SourceUnavailable"
            ):
                continue
            row = select_repair(source, record, feed)
            if has_new_repair(row, record):
                continue
            current = source_key(record.get("source") or row["selected"])
            key = repair_key(variant, row["identity"])
            previous = progress.get(key, {})
            if previous.get("base") == current and previous.get("retry_after", "") > now.isoformat():
                continue
            requested.append((row, current))
    requested.sort(key=lambda item: (item[0]["catalog"].get("priority_rank") or 10**9, item[0]["identity"]))
    return requested


def find_replacements(root: Path, *, limit=100, runtime_minutes=210, persist=False, discoveries=None, continuous=False):
    """Publish a repair feed; this producer never writes the uploader's histories."""
    root = Path(root)
    feed = read_json(root / FEED_PATH, {"schema_version": 1, "repairs": {}})
    progress = read_json(root / SEARCH_PATH)
    publisher = Publisher(root, (root / FEED_PATH, root / SEARCH_PATH)) if persist else None
    manifests = {v: f"variants/{v}/manifests/selected-episodes.jsonl" for v in policy.VARIANTS}
    histories = {v: f"variants/{v}/state/uploads.json" for v in policy.VARIANTS}
    paths = [*manifests.values(), *histories.values(), "state/titles.json", "config/titles.json", "config/runtime.json"]
    provider = None
    checked = repaired = 0
    deadline = time.monotonic() + runtime_minutes * 60
    try:
        while checked < limit and time.monotonic() < deadline:
            if publisher:
                snapshot = publisher.read_snapshot(paths)
                queue = {v: [json.loads(line) for line in snapshot[path].splitlines() if line.strip()]
                         for v, path in manifests.items()}
                states = {v: json.loads(snapshot[path] or "{}") for v, path in histories.items()}
                titles = json.loads(snapshot["state/titles.json"] or '{"series":{},"episodes":{}}')
                overrides = json.loads(snapshot["config/titles.json"] or '{"series":{},"episodes":{}}')
                titles = {kind: {**titles.get(kind, {}), **overrides.get(kind, {})} for kind in ("series", "episodes")}
            else:
                queue = {v: rows(root / path) for v, path in manifests.items()}
                states = {v: read_json(root / path) for v, path in histories.items()}
                titles = Metadata(root).snapshot()
            requests = repair_requests(queue, states, feed, progress)
            if not requests:
                if not continuous:
                    break
                time.sleep(min(30, max(0, deadline - time.monotonic())))
                continue
            if discoveries is None:
                provider = EpisodeSourceProvider.authenticated(os.environ["SDILEJ_EMAIL"], os.environ["SDILEJ_PASSWORD"])
                detector = WhisperLanguageDetector()
                discoveries = {"cz": CzechDiscovery(provider, detector), "original": EnglishDiscovery(provider, detector)}
            row, base = requests[0]
            key = repair_key(row["variant"], row["identity"])
            now = datetime.now(UTC)
            status = "not_found"
            checked += 1
            try:
                series = titles["series"].get(str(row["episode"]["series_id"]), {})
                discovery = discoveries["cz" if row["variant"] == "cz" else "original"]
                found = discovery.discover(row, series, runtime_seconds=min(600, max(1, deadline - time.monotonic())))
                replacement = found.get(row["variant"])
                if replacement:
                    candidate, proof = replacement
                    entry = dict(policy=REPAIR_POLICY, identity=row["identity"], variant=row["variant"], base=base,
                                 selected=candidate.to_dict(), verified_at=now.isoformat())
                    if row["variant"] != "cz":
                        entry.update(subtitles=proof, original_language=series.get("original_language", "en"))
                    # Candidate serialization excludes signed media URLs. Validate before publishing.
                    entry["token"] = repair_token(entry)
                    trial = {"repairs": {key: entry}}
                    validated = select_repair(row, {"source": {"source_id": base.split(":", 1)[0],
                                                               "source_url": base.split(":", 1)[1]}}, trial)
                    if source_key(validated["selected"]) == base:
                        raise ValueError("Source discovery returned the unavailable source again")
                    feed["repairs"][key] = entry
                    repaired += 1
                    status = "verified"
            except Exception as error:
                status = type(error).__name__
                print(f"source_repair_deferred identity={row['identity']} error={error_evidence(error)}", flush=True)
            progress[key] = dict(base=base, status=status, checked_at=now.isoformat(),
                                 retry_after=(now + timedelta(hours=6 if status == "not_found" else 1)).isoformat())
            atomic_json(root / FEED_PATH, feed)
            atomic_json(root / SEARCH_PATH, progress)
            if publisher:
                publisher(root / FEED_PATH)
            print(f"source_repair variant={row['variant']} identity={row['identity']} status={status}", flush=True)
    finally:
        if provider is not None:
            provider.session.close()
    return {"checked": checked, "repaired": repaired}
