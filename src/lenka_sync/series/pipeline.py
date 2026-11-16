"""Durable episode state and account-scoped target sessions."""
from __future__ import annotations

import json
import os
import tempfile
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from lenka_sync.media.models import Candidate
from .models import Episode
from .target import episode_key
from .auth import login_with_retry
from .resilience import retry_target_reads
from lenka_sync.media import prehrajto

def now_iso() -> str:
    return datetime.now(UTC).isoformat()

def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(value, output, ensure_ascii=False, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)

class EpisodeState:
    def __init__(self, path: Path, *, on_save=None):
        self.path = path
        self.on_save = on_save
        self._lock = threading.RLock()
        if path.exists():
            self.data = json.loads(path.read_text(encoding="utf-8"))
        else:
            self.data = {"schema_version": 1, "episodes": {}}
        if self.data.get("schema_version") != 1:
            raise RuntimeError("Unsupported episode state schema")
        self.data.setdefault("episodes", {})

    def row(self, episode: Episode) -> dict:
        with self._lock:
            return self.data["episodes"].setdefault(episode.identity, {})

    def uploaded(self, episode: Episode) -> bool:
        with self._lock:
            return bool(self.data['episodes'].get(episode.identity, {}).get("upload", {}).get("target_video_id"))

    def inspected(self, episode: Episode) -> None:
        with self._lock:
            self.row(episode)['last_inspected_at'] = now_iso()
            self.save()

    def tracked_identities(self) -> set[str]:
        with self._lock:
            return set(self.data["episodes"])

    def retry_deferred_identities(self) -> set[str]:
        """Persisted backoff: 15 minutes, up to 60 for repeated unavailable sources."""
        now = datetime.now(UTC)
        def retry_minutes(row):
            consecutive = 0
            for attempt in reversed(row.get('attempts', [])):
                if attempt['error'] != 'SourceUnavailable':
                    break
                consecutive += 1
            return 15 * 2 ** min(max(consecutive - 1, 0), 2)
        with self._lock:
            return {
                identity for identity, row in self.data['episodes'].items()
                if row.get('attempts')
                and datetime.fromisoformat(row['attempts'][-1]['at']) > now - timedelta(minutes=retry_minutes(row))
                and not row.get('upload')
            }

    def save(self) -> None:
        with self._lock:
            self.data["updated_at"] = now_iso()
            atomic_json(self.path, self.data)
            if self.on_save:
                self.on_save(self.path)

    def claim(self, episode: Episode, worker_id: str, *, lease_hours: int = 6) -> bool:
        with self._lock:
            if episode.identity in self.retry_deferred_identities():
                return False
            row = self.row(episode)
            key = episode_key(f"{episode.series_title} {episode.code}")
            row["episode_key"] = key
            for identity, other in self.data["episodes"].items():
                if identity == episode.identity:
                    continue
                other_key = other.get("episode_key") or episode_key(other.get("upload", {}).get("display_name", ""))
                if other_key != key:
                    continue
                if other.get("upload", {}).get("target_video_id"):
                    row["upload"] = dict(other["upload"])
                    self.save()
                    return False
                if other.get("claim") or other.get("prepared_target"):
                    return False
            if row.get("upload", {}).get("target_video_id"):
                return False
            existing = row.get("claim") or {}
            expiry = existing.get("lease_expires_at")
            if expiry and datetime.fromisoformat(expiry) > datetime.now(UTC):
                return False
            row["claim"] = {
                "worker_id": worker_id,
                "run_id": os.environ.get("GITHUB_RUN_ID"),
                "claimed_at": now_iso(),
                "lease_expires_at": (datetime.now(UTC) + timedelta(hours=lease_hours)).isoformat(),
            }
            self.save()
            return True

    def release_orphaned_claims(self) -> int:
        """A new serialized Actions run may safely release prior-run leases."""
        current_run = os.environ.get("GITHUB_RUN_ID")
        if not current_run:
            return 0
        with self._lock:
            released = 0
            for row in self.data["episodes"].values():
                claim = row.get("claim") or {}
                if claim and claim.get("run_id") != current_run:
                    row.pop("claim", None)
                    released += 1
            if released:
                self.save()
            return released

    def prepared(self, episode: Episode, candidate: Candidate, name: str) -> None:
        with self._lock:
            self.row(episode)["source"] = {
                "source_id": candidate.source_id,
                "source_url": candidate.url,
                "source_title": candidate.title,
                "source_filename": candidate.filename,
                "size_bytes": candidate.size_bytes,
                "duration_sec": candidate.duration_sec,
                "width": candidate.width,
                "height": candidate.height,
                "audio_language": candidate.audio_language,
                "language_probability": candidate.language_probability,
                "language_tier": candidate.language_tier.name.lower(),
                "match_evidence": candidate.match_evidence,
                "display_name": name,
                "prepared_at": now_iso(),
            }
            self.save()

    def success(self, episode: Episode, video_id: str, name: str) -> None:
        with self._lock:
            row = self.row(episode)
            row["upload"] = {"target_video_id": str(video_id), "display_name": name, "uploaded_at": now_iso()}
            row.pop("prepared_target", None)
            row.pop("transfer_receipt", None)
            row.pop("claim", None)
            self.save()

    def failure(self, episode: Episode, error: Exception) -> None:
        with self._lock:
            row = self.row(episode)
            row.setdefault("attempts", []).append({"at": now_iso(), "error": type(error).__name__})
            row["attempts"] = row["attempts"][-3:]
            # An uncertain target must never be replaced by another upload.
            row.pop("claim", None)
            self.save()


def target_session(email: str, password: str, *, expected_email: str | None = None):
    if expected_email is not None and email.strip().casefold() != expected_email.strip().casefold():
        raise RuntimeError("Target account does not match the requested account")
    return retry_target_reads(login_with_retry(prehrajto.login, email, password))
