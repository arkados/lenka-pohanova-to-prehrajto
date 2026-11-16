"""Verify that the installed project works without the former private packages."""

import subprocess
import sys
from types import SimpleNamespace

import pytest

from lenka_sync.media import prehrajto
from lenka_sync.series.pipeline import target_session


def test_all_runtime_modules_load_without_legacy_packages(tmp_path):
    script = """
import importlib
import importlib.abc
import pkgutil
import sys
import lenka_sync

class RejectLegacyPackages(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'sdilej_serialy', 'sdilej_to_prehrajto'}:
            raise AssertionError('Private project dependency: ' + fullname)

sys.meta_path.insert(0, RejectLegacyPackages())
for module in pkgutil.walk_packages(lenka_sync.__path__, lenka_sync.__name__ + '.'):
    importlib.import_module(module.name)
"""
    subprocess.run([sys.executable, "-c", script], cwd=tmp_path, capture_output=True, text=True, check=True)


def test_target_login_rejects_another_account_before_network(monkeypatch):
    def unexpected_session():
        pytest.fail("An invalid target account must be rejected before a network request")

    monkeypatch.setattr(prehrajto.requests, "Session", unexpected_session)
    with pytest.raises(prehrajto.PrehrajtoError, match="Lenka account"):
        prehrajto.login("another@example.invalid", "test")


def test_target_login_accepts_the_configured_account():
    requests = []

    def request(method, url, **kwargs):
        requests.append((method, url, kwargs))
        return SimpleNamespace(status_code=200, raise_for_status=lambda: None)

    session = SimpleNamespace(
        headers={},
        get=lambda url, **kwargs: request("GET", url, **kwargs),
        post=lambda url, **kwargs: request("POST", url, **kwargs),
    )
    assert prehrajto.login("target@example.invalid", "test", session=session) is session
    assert [method for method, _, _ in requests] == ["GET", "POST", "GET"]
    assert requests[1][2]["files"]["email"] == (None, "target@example.invalid")


def test_target_session_rejects_an_unexpected_account():
    with pytest.raises(RuntimeError, match="requested account"):
        target_session("target@example.invalid", "test", expected_email="another@example.invalid")
