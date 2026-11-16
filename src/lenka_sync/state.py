"""Variant-isolated durable upload histories bound to the Lenka account."""

from lenka_sync.series.pipeline import EpisodeState
from lenka_sync.series.source_transfer import SourceUnavailable
from lenka_sync.series.models import Episode
from datetime import UTC, datetime, timedelta
from . import policy


def audit_history(root):
    """Refuse missing histories or reused target IDs before any remote operation."""
    states = {}
    targets = set()
    summary = {}
    for variant in policy.VARIANTS:
        path = root / f"variants/{variant}/state/uploads.json"
        if not path.is_file():
            raise ValueError(f"Required upload history is missing for {variant}")
        state = states[variant] = VariantState(path, variant)
        completed = pending = 0
        for record in state.data["episodes"].values():
            completed += bool(record.get("upload"))
            pending += bool(record.get("prepared_target"))
            for field in ("upload", "prepared_target"):
                video_id = record.get(field, {}).get("target_video_id")
                if video_id is not None:
                    if str(video_id) in targets:
                        raise ValueError("Upload histories reuse a target video ID")
                    targets.add(str(video_id))
        summary[variant] = {"completed": completed, "pending": pending}
    validate_target_ownership(states)
    return summary


def validate_target_ownership(states):
    owners = {}
    for variant, state in states.items():
        for record in state.data["episodes"].values():
            for field in ("upload", "prepared_target"):
                video_id = record.get(field, {}).get("target_video_id")
                if video_id is None:
                    continue
                video_id = str(video_id)
                if video_id in owners and owners[video_id] != variant:
                    raise ValueError("Persisted target ID is shared across language variants")
                owners[video_id] = variant


class VariantState(EpisodeState):
    def __init__(self, path, variant, *, on_save=None, lock=None):
        if variant not in policy.VARIANTS:
            raise ValueError("Unknown variant")
        super().__init__(path, on_save=on_save)
        expected = {"account_hash": policy.TARGET_ACCOUNT_HASH, "variant": variant, "policy": policy.POLICY}
        if self.data.get("episodes") and self.data.get("binding") != expected:
            raise ValueError("Upload history belongs to a different account or variant")
        if self.data.get("binding", expected) != expected:
            raise ValueError("Upload history binding mismatch")
        self.data["binding"] = expected
        self.variant = variant
        self.peers = {}
        if lock is not None:
            self._lock = lock

    def retry_deferred_identities(self):
        deferred = super().retry_deferred_identities()
        with self._lock:
            return {identity for identity in deferred
                    if self.data["episodes"][identity]["attempts"][-1]["at"]
                    > self.data["episodes"][identity].get("source_retry_reset_at", "")}

    def activate_source_repair(self, row):
        from .source_repair import has_new_repair, source_key

        episode = Episode.from_dict(row["episode"])
        with self._lock:
            record = self.row(episode)
            if not has_new_repair(row, record):
                return True
            if any(record.get(field) for field in ("upload", "prepared_target", "claim", "review_target_id")):
                return False
            repair = row["source_repair"]
            if record.get("source") and source_key(record["source"]) != repair["base"]:
                return False
            record["source_retry_reset_at"] = datetime.now(UTC).isoformat()
            record["source_repair_token"] = repair["token"]
            record.pop("source_repair_requested", None)
            history = record.setdefault("source_repair_history", [])
            if not history or history[-1]["token"] != repair["token"]:
                history.append(dict(token=repair["token"], previous=repair["base"],
                                    replacement=source_key(row["selected"]), at=record["source_retry_reset_at"]))
            self.save()
            return True

    def source_deferred_series(self, *, at=None):
        now = at or datetime.now(UTC)
        with self._lock:
            return {
                series_id for series_id, record in self.data.get("source_outages", {}).items()
                if record.get("retry_after", "") > now.isoformat()
            }

    def failure(self, episode, error):
        with self._lock:
            record = self.row(episode)
            if isinstance(error, SourceUnavailable) and record.get("source", {}).get("source_id") and not record.get("prepared_target"):
                from .source_repair import source_key

                now = datetime.now(UTC)
                record["source_repair_requested"] = {"base": source_key(record["source"]), "at": now.isoformat()}
                series_id = str(episode.series_id)
                outage = self.data.setdefault("source_outages", {}).setdefault(series_id, {})
                recent = {
                    item["identity"]: item for item in outage.get("recent_failures", [])
                    if item["at"] > (now - timedelta(minutes=5)).isoformat()
                }
                recent[episode.identity] = dict(identity=episode.identity, at=now.isoformat())
                outage["recent_failures"] = list(recent.values())[-3:]
                if len(recent) >= 3:
                    outage["retry_after"] = (now + timedelta(minutes=10)).isoformat()
                    print(f"source_series_deferred variant={self.variant} series_id={series_id}", flush=True)
            super().failure(episode, error)

    def success(self, episode, video_id, name):
        with self._lock:
            for variant, peer in self.peers.items():
                if variant == self.variant:
                    continue
                for other in peer.data["episodes"].values():
                    if str(video_id) in {
                        str(other.get("upload", {}).get("target_video_id")),
                        str(other.get("prepared_target", {}).get("target_video_id")),
                    }:
                        raise RuntimeError("Target ID already belongs to another language variant")
            row = self.row(episode)
            row.pop("transfer_failure", None)
            receipt = dict(row.get("transfer_receipt") or {})
            source = dict(row.get("source") or {})
            # Persist completion provenance before the base class removes transient fields.
            if receipt:
                row["completion_evidence"] = {
                    "receipt": receipt,
                    "variant": self.variant,
                    "source_id": source.get("source_id"),
                    "source_url": source.get("source_url"),
                    "audio_language": source.get("audio_language"),
                }
            super().success(episode, video_id, name)
