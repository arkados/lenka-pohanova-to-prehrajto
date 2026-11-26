import pytest
from lenka_sync.metadata import Metadata, MetadataUnavailable


def test_series_identity_is_exact_and_unique(tmp_path, monkeypatch):
    api = Metadata(tmp_path, api_key="test")
    responses = {
        "search/tv": {"results": [{"id": 10, "original_name": "Dark"}], "total_pages": 1},
        "tv/10": dict(id=10, name="Dark", original_name="Dark", original_language="de", first_air_date="2017-12-01"),
        "tv/10/translations": {"translations": [{"iso_639_1": "en", "data": {"name": "Dark"}}]},
    }
    monkeypatch.setattr(api, "get", lambda endpoint, **_: responses[endpoint])
    assert api.resolve_series({"series_original_title": "Dark"})["tmdb_id"] == 10
    responses["search/tv"]["results"].append({"id": 20, "original_name": "Dark"})
    with pytest.raises(MetadataUnavailable):
        api.resolve_series({"series_original_title": "Dark"})


def test_episode_uses_original_language_without_english_translation(tmp_path, monkeypatch):
    api = Metadata(tmp_path, api_key="test")

    def get(endpoint, **_):
        if endpoint.endswith("translations"):
            return {"translations": [{"iso_639_1": "de", "data": {"name": "Geheimnisse"}}]}
        return {
            "season_number": 1,
            "episodes": [dict(id=100, season_number=1, episode_number=1, show_id=10, name="Geheimnisse")],
        }

    monkeypatch.setattr(api, "get", get)
    assert (
        api.resolve_episode({"season": 1, "episode": 1}, {"tmdb_id": 10, "original_language": "de"})["original_title"]
        == "Geheimnisse"
    )


def test_wrong_episode_numbering_cannot_supply_name(tmp_path, monkeypatch):
    api = Metadata(tmp_path, api_key="test")
    monkeypatch.setattr(api, "get", lambda *_, **__: {"season_number": 2, "episodes": []})
    with pytest.raises(MetadataUnavailable):
        api.resolve_episode({"season": 1, "episode": 1}, {"tmdb_id": 10, "original_language": "en"})


def test_title_budget_prioritizes_unchecked_episodes_and_skips_unchanged_checkpoints(tmp_path, monkeypatch):
    api = Metadata(tmp_path, api_key="test")
    api.data["series"] = {
        "1": {"status": "unresolved", "retry_after": "2000-01-01"},
        "2": {"original_title": "Known Series", "original_language": "en", "tmdb_id": 10},
    }
    api.data["episodes"]["2:1:1"] = {"original_title": "Pilot"}
    catalog = [dict(series_id=series, season=1, episode=number) for series, number in ((1, 1), (2, 1), (2, 2))]
    saves = []
    monkeypatch.setattr("lenka_sync.metadata.rows", lambda _: catalog)
    monkeypatch.setattr(api, "save", lambda: saves.append(True))
    monkeypatch.setattr(api, "resolve_series", lambda _: pytest.fail("Series retry used the title budget"))

    def resolve_episode(row, _):
        assert row["episode"] == 2
        return {"original_title": "Second Episode"}

    monkeypatch.setattr(api, "resolve_episode", resolve_episode)
    assert api.resolve(limit=1)["checked"] == 1
    assert api.data["episodes"]["2:1:2"]["original_title"] == "Second Episode"
    assert len(saves) == 1
