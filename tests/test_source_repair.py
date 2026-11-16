from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import yaml

from lenka_sync.discovery import CzechDiscovery
from lenka_sync.media.models import Candidate
from lenka_sync.media.sdilej import SdilejError
from lenka_sync.pipeline import planned
from lenka_sync.policy import VARIANTS
from lenka_sync.series.models import Episode
from lenka_sync.series.source_transfer import SourceUnavailable
from lenka_sync.source_repair import (
    FEED_PATH, REPAIR_POLICY, find_replacements, has_new_repair, repair_key,
    repair_requests, repair_token, select_repair, source_key,
)
from lenka_sync.state import VariantState
from lenka_sync.storage import atomic_json, read_json, write_rows


def replacement(row, source_id="456", width=3840, size=300_000_000):
    candidate = Candidate.from_dict(row["selected"])
    candidate.source_id = source_id
    candidate.url = f"https://sdilej.cz/{source_id}/replacement.mkv"
    candidate.width, candidate.height, candidate.size_bytes = width, 2160 if width == 3840 else 1080, size
    return candidate


def feed_for(row, candidate):
    entry = dict(policy=REPAIR_POLICY, identity=row["identity"], variant=row["variant"],
                 base=source_key(row["selected"]), selected=candidate.to_dict(), verified_at=datetime.now(UTC).isoformat())
    entry["token"] = repair_token(entry)
    return {"schema_version": 1, "repairs": {repair_key(row["variant"], row["identity"]): entry}}


def failed_state(root, row):
    state = VariantState(root / f"variants/{row['variant']}/state/uploads.json", row["variant"])
    episode = Episode.from_dict(row["episode"])
    state.prepared(episode, Candidate.from_dict(row["selected"]), "Previous source")
    state.failure(episode, SourceUnavailable("private diagnostic must not be stored"))
    return state


def test_failure_enqueues_only_unallocated_sources(tmp_path, czech_row):
    state = failed_state(tmp_path, czech_row)
    record = state.row(Episode.from_dict(czech_row["episode"]))
    assert record["source_repair_requested"]["base"] == source_key(czech_row["selected"])
    assert "private diagnostic" not in state.path.read_text()
    record.pop("source_repair_requested")
    record["prepared_target"] = {"target_video_id": "900"}
    state.failure(Episode.from_dict(czech_row["episode"]), SourceUnavailable("test"))
    assert "source_repair_requested" not in record


def test_repair_producer_writes_feed_without_modifying_upload_history_or_manifest(tmp_path, czech_row, titles):
    state = failed_state(tmp_path, czech_row)
    manifest = tmp_path / "variants/cz/manifests/selected-episodes.jsonl"
    write_rows(manifest, [czech_row])
    atomic_json(tmp_path / "state/titles.json", titles)
    original_history, original_manifest = state.path.read_bytes(), manifest.read_bytes()
    candidate = replacement(czech_row)
    candidate.download_url = "https://private.invalid/?token=do-not-publish"
    discovery = SimpleNamespace(discover=lambda *_, **__: {"cz": (candidate, {"method": "none"})})
    assert find_replacements(tmp_path, limit=1, discoveries={"cz": discovery}) == {"checked": 1, "repaired": 1}
    assert state.path.read_bytes() == original_history
    assert manifest.read_bytes() == original_manifest
    assert "do-not-publish" not in (tmp_path / FEED_PATH).read_text()
    repaired = select_repair(czech_row, state.row(Episode.from_dict(czech_row["episode"])), read_json(tmp_path / FEED_PATH))
    assert repaired["selected"]["source_id"] == "456"


@pytest.mark.parametrize("field", ["upload", "prepared_target", "claim", "review_target_id"])
def test_repair_cannot_replace_a_locked_or_completed_source(czech_row, field):
    feed = feed_for(czech_row, replacement(czech_row))
    record = {field: {"target_video_id": "900"}}
    assert select_repair(czech_row, record, feed) == czech_row
    states = {"cz": {"episodes": {czech_row["identity"]: {**record, "attempts": [{"error": "SourceUnavailable"}]}}}}
    assert repair_requests({"cz": [czech_row]}, states, feed, {}) == []


def test_new_repair_can_bypass_old_source_backoff_but_not_its_own_failure(tmp_path, czech_row, titles):
    state = failed_state(tmp_path, czech_row)
    episode = Episode.from_dict(czech_row["episode"])
    states = {"cz": state}
    queue = {"cz": [czech_row]}
    assert planned(queue, titles, states) == []
    state.data["source_outages"] = {str(episode.series_id): {"retry_after": (datetime.now(UTC) + timedelta(hours=1)).isoformat()}}
    feed = feed_for(czech_row, replacement(czech_row))
    row = planned(queue, titles, states, repairs=feed)[0]
    assert row["selected"]["source_id"] == "456"
    assert state.activate_source_repair(row)
    assert state.claim(episode, "worker")
    state.prepared(episode, Candidate.from_dict(row["selected"]), row["display_name"])
    state.failure(episode, SourceUnavailable("replacement failed"))
    assert not has_new_repair(row, state.row(episode))
    assert planned(queue, titles, states, repairs=feed) == []
    assert len(state.row(episode)["attempts"]) == 2


def test_successive_repairs_are_bound_to_the_latest_failed_source(tmp_path, czech_row):
    state = failed_state(tmp_path, czech_row)
    episode = Episode.from_dict(czech_row["episode"])
    first = select_repair(czech_row, state.row(episode), feed_for(czech_row, replacement(czech_row)))
    state.activate_source_repair(first)
    state.prepared(episode, Candidate.from_dict(first["selected"]), "Replacement")
    second_feed = feed_for(first, replacement(czech_row, source_id="789"))
    second = select_repair(czech_row, state.row(episode), second_feed)
    assert second["selected"]["source_id"] == "789"
    unrelated = deepcopy(state.row(episode))
    unrelated["source"]["source_id"] = "999"
    assert select_repair(czech_row, unrelated, second_feed) == czech_row


def test_tampered_or_unverified_replacement_is_rejected(czech_row):
    feed = feed_for(czech_row, replacement(czech_row))
    entry = next(iter(feed["repairs"].values()))
    entry["selected"]["audio_language"] = "en"
    with pytest.raises(ValueError, match="token"):
        select_repair(czech_row, {}, feed)
    entry["token"] = repair_token(entry)
    with pytest.raises(ValueError, match="variant policy"):
        select_repair(czech_row, {}, feed)


def test_czech_replacement_selects_best_resolution_then_smallest_file(czech_row):
    candidates = [replacement(czech_row, "1", 1920, 1), replacement(czech_row, "2", 3840, 900_000_000),
                  replacement(czech_row, "3", 3840, 600_000_000)]
    discovery = CzechDiscovery(SimpleNamespace(search=lambda _: candidates), None)
    discovery.inspect_media = lambda _, candidate: candidate
    inspected_audio = []

    def speech(candidate):
        inspected_audio.append(candidate.source_id)
        return candidate

    discovery.verify_speech = speech
    assert discovery.discover(czech_row, {})["cz"][0].source_id == "3"
    assert inspected_audio == ["3"]


def test_czech_quality_selection_rejects_foreign_speech_before_trying_next_same_quality(czech_row):
    candidates = [replacement(czech_row, "1", 1920, 1), replacement(czech_row, "2", 3840, 900_000_000),
                  replacement(czech_row, "3", 3840, 600_000_000)]
    discovery = CzechDiscovery(SimpleNamespace(search=lambda _: candidates), None)
    discovery.inspect_media = lambda _, candidate: candidate
    discovery.verify_speech = lambda candidate: None if candidate.source_id == "3" else candidate
    assert discovery.discover(czech_row, {})["cz"][0].source_id == "2"


def test_upload_failure_only_queues_search_while_other_uploads_continue(tmp_path, czech_row, titles, monkeypatch):
    from lenka_sync import pipeline

    second = deepcopy(czech_row)
    second["identity"] = "42:1:2"
    second["episode"].update(episode=2, episode_id=101)
    second["selected"].update(source_id="456", url="https://sdilej.cz/456/second.mkv")
    titles["episodes"]["42:1:2"] = {"status": "official_original_episode_title_unavailable"}
    write_rows(tmp_path / "variants/cz/manifests/selected-episodes.jsonl", [czech_row, second])
    atomic_json(tmp_path / "state/titles.json", titles)
    monkeypatch.setattr(pipeline, "credentials", lambda: {})
    monkeypatch.setattr(pipeline, "followup", lambda *_, **__: {})

    def unexpected_search(*_, **__):
        pytest.fail("The upload pool must not run source discovery")

    monkeypatch.setattr("lenka_sync.source_repair.find_replacements", unexpected_search)

    def transfer(selected, state, **kwargs):
        row = selected[0]
        episode = Episode.from_dict(row["episode"])
        state.prepared(episode, Candidate.from_dict(row["selected"]), row["display_name"])
        if row["identity"] == czech_row["identity"]:
            state.failure(episode, SourceUnavailable("source missing"))
        else:
            state.success(episode, "900", row["display_name"])
        return {}

    monkeypatch.setattr(pipeline, "upload_continuously", transfer)
    result = pipeline.upload(tmp_path, workers=2, limit=2, runtime_minutes=1, variant="cz")
    assert result["confirmed_uploads"] == 1
    state = read_json(tmp_path / "variants/cz/state/uploads.json")
    assert state["episodes"][czech_row["identity"]]["source_repair_requested"]
    assert state["episodes"][second["identity"]]["upload"]["target_video_id"] == "900"


def test_czech_repair_skips_missing_details_but_defers_uncertain_better_media(czech_row):
    low = replacement(czech_row, "1", 1920)
    high = replacement(czech_row, "2", 3840)
    discovery = CzechDiscovery(SimpleNamespace(search=lambda _: [low, high]), None)

    def verify(_, candidate):
        if candidate.source_id == "2":
            raise SdilejError("Source detail is temporarily unavailable; no original download link")
        return candidate

    discovery.inspect_media = verify
    discovery.verify_speech = lambda candidate: candidate
    assert discovery.discover(czech_row, {})["cz"][0].source_id == "1"

    def uncertain(_, candidate):
        raise SdilejError("Original media metadata is incomplete")

    discovery.inspect_media = uncertain
    with pytest.raises(SdilejError):
        discovery.discover(czech_row, {})


def test_source_repair_workflow_runs_separately_without_target_credentials():
    from pathlib import Path

    directory = Path(__file__).parents[1] / ".github/workflows"
    repair = yaml.safe_load((directory / "repair.yml").read_text())
    upload = yaml.safe_load((directory / "upload.yml").read_text())
    assert repair["concurrency"]["group"] != upload["concurrency"]["group"]
    assert "PREHRAJTO_" not in (directory / "repair.yml").read_text()
    assert VARIANTS == ("cz", "en", "en_plain")
