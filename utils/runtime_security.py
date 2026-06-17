from __future__ import annotations

import os
import subprocess


def restrict_path_to_current_user(path: str, *, is_dir: bool = False) -> None:
    """Best-effort Windows ACL hardening for local portable app data."""
    if os.name != "nt" or not path or not os.path.exists(path):
        return

    user = os.environ.get("USERNAME")
    domain = os.environ.get("USERDOMAIN")
    account = f"{domain}\\{user}" if domain and user else user
    if not account:
        return

    grant = f"{account}:(OI)(CI)F" if is_dir else f"{account}:F"
    args = ["icacls", path, "/inheritance:r", "/grant:r", grant, "*S-1-5-18:F", "*S-1-5-32-544:F"]
    try:
        subprocess.run(args, capture_output=True, text=True, check=False, timeout=10)
    except (OSError, subprocess.SubprocessError):
        pass


def ensure_private_text_file(path: str, value_factory) -> str:
    """Create/read a sensitive local text file and restrict its ACL where possible."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "r", encoding="utf-8") as f:
            value = f.read().strip()
        if value:
            restrict_path_to_current_user(path)
            return value
    except OSError:
        pass

    value = value_factory()
    with open(path, "w", encoding="utf-8") as f:
        f.write(value)
    restrict_path_to_current_user(path)
    return value
