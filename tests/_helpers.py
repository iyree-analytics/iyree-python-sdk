"""Shared test helpers (importable from test modules)."""

from __future__ import annotations

import base64
import json
import time


def make_jwt(exp: float | None = None, extra_claims: dict | None = None) -> str:
    """Create a minimal JWT for testing (no cryptographic signature)."""
    header = base64.urlsafe_b64encode(
        json.dumps({"alg": "HS256", "typ": "JWT"}).encode()
    ).rstrip(b"=").decode()

    claims = {"sub": "test", "iat": int(time.time())}
    if exp is not None:
        claims["exp"] = int(exp)
    else:
        claims["exp"] = int(time.time()) + 3600
    if extra_claims:
        claims.update(extra_claims)

    payload = base64.urlsafe_b64encode(
        json.dumps(claims).encode()
    ).rstrip(b"=").decode()

    signature = base64.urlsafe_b64encode(b"fakesig").rstrip(b"=").decode()
    return f"{header}.{payload}.{signature}"
