# SPDX-License-Identifier: Apache-2.0
"""
Tests for the password-protected report flow in src/controller.py.

Regression coverage for a bypass where /build-wbr/publish/protected only
checked that a `password` query param was present, without verifying it
against the report's real password. See PR description for details.
"""
import json

import pytest
from cryptography.fernet import Fernet

import src.controller as controller

FILE_NAME = "test_report.json"
REAL_PASSWORD = "correct-horse-battery-staple"
PROTECTED_DATA = {"password": REAL_PASSWORD, "data": {"secret": "only for authorised viewers"}}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(controller.publisher, "download", lambda path: PROTECTED_DATA)
    controller.app.config["TESTING"] = True
    with controller.app.test_client() as test_client:
        yield test_client


def test_login_with_correct_password_redirects_with_valid_token(client):
    resp = client.post(f"/login?file={FILE_NAME}", data={"password": REAL_PASSWORD})

    assert resp.status_code == 302
    location = resp.headers["Location"]
    assert location.startswith(f"/build-wbr/publish/protected?file={FILE_NAME}&password=")

    token = location.split("password=", 1)[1]
    # The token must actually decrypt to the real password, not just be present.
    decrypted = Fernet(controller.key).decrypt(token.encode("utf-8")).decode("utf-8")
    assert decrypted == REAL_PASSWORD


def test_login_with_wrong_password_is_rejected(client):
    resp = client.post(f"/login?file={FILE_NAME}", data={"password": "not-the-password"})

    assert resp.status_code == 403
    assert json.loads(resp.get_data(as_text=True)) == {"message": "Unauthorised"}


def test_protected_report_rejects_request_without_going_through_login(client):
    """An arbitrary, never-validated password value must not grant access."""
    resp = client.get(f"/build-wbr/publish/protected?file={FILE_NAME}&password=anything-goes")

    assert resp.status_code == 403
    assert "only for authorised viewers" not in resp.get_data(as_text=True)


def test_protected_report_rejects_a_validly_encrypted_but_wrong_password(client):
    """A well-formed Fernet token that decrypts to the wrong password must still be rejected."""
    forged_token = Fernet(controller.key).encrypt(b"not-the-real-password").decode("utf-8")

    resp = client.get(f"/build-wbr/publish/protected?file={FILE_NAME}&password={forged_token}")

    assert resp.status_code == 403
    assert "only for authorised viewers" not in resp.get_data(as_text=True)


def test_protected_report_accepts_the_token_issued_by_login(client):
    login_resp = client.post(f"/login?file={FILE_NAME}", data={"password": REAL_PASSWORD})
    location = login_resp.headers["Location"]

    resp = client.get(location)

    assert resp.status_code == 200
    assert "only for authorised viewers" in resp.get_data(as_text=True)


def test_protected_report_without_password_redirects_to_login(client):
    resp = client.get(f"/build-wbr/publish/protected?file={FILE_NAME}")

    assert resp.status_code == 302
    assert resp.headers["Location"] == f"/login?file={FILE_NAME}"
