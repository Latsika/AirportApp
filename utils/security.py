# utils/security.py
from __future__ import annotations

import hmac
from typing import Optional, Tuple

import bcrypt
from werkzeug.security import check_password_hash


BCRYPT_PREFIXES = ("$2a$", "$2b$", "$2y$")


def hash_password(password: str) -> str:
    """Hash password using bcrypt (recommended)."""
    if password is None:
        raise ValueError("Password is required")
    pw_bytes = password.encode("utf-8")
    return bcrypt.hashpw(pw_bytes, bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, stored_hash: str) -> bool:
    """Backward compatible verify.

    Returns True/False only. (keeps existing call sites working)
    """
    ok, _new_hash = verify_password_and_upgrade(password, stored_hash)
    return ok


def verify_password_and_upgrade(password: str, stored_hash: Optional[str]) -> Tuple[bool, Optional[str]]:
    """Verify password against stored hash and optionally return upgraded hash.

    Supports:
    - bcrypt hashes ($2a$ / $2b$ / $2y$)
    - Werkzeug hashes (pbkdf2:/scrypt:/argon2:)  -> upgrade to bcrypt on success
    - plaintext legacy values                    -> upgrade to bcrypt on success
    """
    if stored_hash is None:
        return False, None

    stored = stored_hash.strip()
    if stored == "":
        return False, None

    # 1) bcrypt (only if it looks like bcrypt)
    if stored.startswith(BCRYPT_PREFIXES):
        try:
            ok = bcrypt.checkpw(password.encode("utf-8"), stored.encode("utf-8"))
            return ok, None
        except ValueError:
            # malformed bcrypt hash in DB -> fallback below (plaintext) to avoid lockout
            pass

    # 2) Werkzeug hashes (from older versions)
    if ":" in stored and (
        stored.startswith("pbkdf2:")
        or stored.startswith("scrypt:")
        or stored.startswith("argon2:")
    ):
        ok = check_password_hash(stored, password)
        if ok:
            # upgrade to bcrypt
            return True, hash_password(password)
        return False, None

    # 3) Plaintext legacy (or unknown format) -> constant-time compare
    ok = hmac.compare_digest(password, stored)
    if ok:
        return True, hash_password(password)
    return False, None
