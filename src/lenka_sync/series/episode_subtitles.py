"""Attach verified exact-source subtitles with one durable upload intent."""
from __future__ import annotations

import hashlib
import re

from lenka_sync.media.models import Candidate
from .models import Episode
from .pipeline import now_iso
from .subtitle_media import extract_original, normalize_srt, SubtitleUnavailable
from .subtitle_target import TargetUnavailable

def read_target_view(api, video_id, listing, detail_url):
    """Require the owned upload form to be ready before reading its player."""
    if listing is None:
        raise TargetUnavailable('target_listing_video_missing')
    if not listing.ready:
        return listing
    view = api.refresh(video_id, listing.title, detail_url)
    view.ready = bool(view.ready and listing.ready)
    view.action = listing.action
    by_id = {track['id']: track for track in listing.tracks}
    for track in view.tracks:
        track['language_action'] = by_id.get(track['id'], {}).get('language_action', '')
    return view

def exact_source(row, record):
    source = record.get('source', {})
    selected = row['selected']
    return (source.get('source_id') == selected['source_id']
            and source.get('source_url') == selected['url']
            and source.get('size_bytes') == selected['size_bytes'])

def finish(entry, view, save, rename, dry_run):
    entry.update(status='attached_verified', verified_at=now_iso())
    ready_title = re.sub(r' \(titulky čekají\)(?=(?:\.[A-Za-z0-9]{1,8})?$)', '', view.title)
    if not dry_run:
        save()
        if ready_title != view.title:
            rename(ready_title)
        entry['title_updated'] = True

def attach(row, record, entry, view, api, provider, refresh, save, rename, *, deadline, dry_run=False):
    """One durable POST intent; an uncertain result is only ever read again."""
    if not exact_source(row, record):
        entry['status'] = 'source_binding_missing'
        return
    expected = row['subtitles']['sha256']
    if not view.ready:
        entry['status'] = 'target_processing'
        return
    intent = entry.get('intent')
    if not intent:
        if view.has_czech:
            for track in view.tracks:
                if track['czech'] and not track['processing'] and api.verify_content(track, expected):
                    finish(entry, view, save, rename, dry_run)
                    return
            entry['status'] = 'existing_czech_requires_review'
            return
        if view.unknown_tracks:
            entry['status'] = 'existing_tracks_uncertain'
            return
        episode, candidate = Episode.from_dict(row['episode']), Candidate.from_dict(row['selected'])
        current = provider._inspect(episode, candidate)
        if current is None or current.size_bytes != candidate.size_bytes:
            raise SubtitleUnavailable('source_original_changed')
        payload, _ = extract_original(provider.session,
            dict(source_id=candidate.source_id, source_url=candidate.url, source_filename=candidate.filename), deadline=deadline)
        payload = normalize_srt(payload)
        if hashlib.sha256(payload).hexdigest() != expected:
            raise SubtitleUnavailable('source_subtitle_changed')
        if dry_run:
            entry['status'] = 'ready_to_attach'
            return
        filename = f"episode-{view.video_id}-cs-{expected[:16]}.srt"
        entry['intent'] = dict(filename=filename, sha256=expected,
                               before_ids=[t['id'] for t in view.tracks], created_at=now_iso())
        entry['status'] = 'upload_intent'
        save()  # Must reach Git before the only subtitle POST.
        api.upload(view, filename, payload)
        entry['status'] = 'upload_unconfirmed'
        save()
        view = refresh()
        intent = entry['intent']
    own = [track for track in view.tracks
           if track['id'] not in intent['before_ids']
           and (track['id'] == intent.get('subtitle_id') or intent['filename'].rsplit('.', 1)[0] in track['name'])]
    if len(own) != 1:
        entry['status'] = 'upload_unconfirmed'
        return
    track = own[0]
    if not track['czech']:
        if intent.get('language_attempted') or not track.get('language_action'):
            entry['status'] = 'language_unconfirmed'
            return
        if dry_run:
            entry['status'] = 'language_pending'
            return
        intent.update(subtitle_id=track['id'], language_attempted=True)
        save()
        api.set_own_czech_language(track, view.video_id)
        entry['status'] = 'verifying_language'
        return
    if track['processing'] or not api.verify_content(track, expected):
        entry['status'] = 'verifying_content'
        return
    intent['subtitle_id'] = track['id']
    finish(entry, view, save, rename, dry_run)
