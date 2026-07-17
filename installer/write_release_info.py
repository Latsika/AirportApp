from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT_DIR / "dist"
RELEASE_MD = DIST_DIR / "RELEASE_INFO.md"
RELEASE_JSON = DIST_DIR / "app_release.json"
IGNORED_DIRS = (
    "__pycache__/",
    "backups/",
    "build/",
    "dist/",
    "logs/",
    "release_",
)
IGNORED_PARTS = ("/__pycache__/",)
IGNORED_SUFFIXES = (".db", ".log", ".pyc", ".secret")


def _git_value(*args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return "unknown"
    if result.returncode != 0:
        return "unknown"
    return result.stdout.strip() or "unknown"


def _status_path(line: str) -> str:
    path = line[3:].strip()
    if " -> " in path:
        path = path.split(" -> ", 1)[1].strip()
    return path.strip('"').replace("\\", "/")


def _is_ignored_status_path(path: str) -> bool:
    return (
        path.startswith(IGNORED_DIRS)
        or any(part in path for part in IGNORED_PARTS)
        or path.endswith(IGNORED_SUFFIXES)
    )


def _source_status_lines() -> list[str]:
    status = _git_value("status", "--porcelain")
    if status == "unknown":
        return []
    return [
        line
        for line in status.splitlines()
        if line.strip() and not _is_ignored_status_path(_status_path(line))
    ]


def _working_tree_state(status_lines: list[str]) -> str:
    if _git_value("status", "--porcelain") == "unknown":
        return "unknown"
    return "dirty" if status_lines else "clean"


def _build_info() -> dict[str, str]:
    built_at_dt = datetime.now(timezone.utc).replace(microsecond=0)
    built_at = built_at_dt.isoformat()
    commit_short = _git_value("rev-parse", "--short", "HEAD")
    commit_full = _git_value("rev-parse", "HEAD")
    source_status = _source_status_lines()
    state = _working_tree_state(source_status)
    release_id = built_at_dt.strftime("%Y%m%dT%H%M%SZ")
    if commit_short != "unknown":
        release_id = f"{release_id}-{commit_short}"

    return {
        "app": "AirportApp",
        "release_id": release_id,
        "built_at_utc": built_at,
        "git_commit": commit_full,
        "git_commit_short": commit_short,
        "working_tree": state,
        "source_changes": "\n".join(source_status),
    }


def _write_markdown(info: dict[str, str]) -> None:
    dirty_note = ""
    if info["working_tree"] == "dirty":
        dirty_note = (
            "\n> WARNING: This release was built while relevant source files had "
            "uncommitted changes.\n"
        )
        if info["source_changes"]:
            dirty_note += "\nSource changes included in this build:\n\n"
            dirty_note += "\n".join(
                f"- `{line}`" for line in info["source_changes"].splitlines()
            )
            dirty_note += "\n"

    RELEASE_MD.write_text(
        "\n".join(
            [
                "# AirportApp Release Info",
                "",
                f"- Release ID: `{info['release_id']}`",
                f"- Built at UTC: `{info['built_at_utc']}`",
                f"- Git commit: `{info['git_commit']}`",
                f"- Working tree: `{info['working_tree']}`",
                "",
                "This file is copied next to `AirportApp.exe` during install/update.",
                "To identify the installed customer release, open this file in the app folder.",
                dirty_note,
            ]
        ).rstrip()
        + "\n",
        encoding="utf-8",
    )


def main() -> int:
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    info = _build_info()
    _write_markdown(info)
    RELEASE_JSON.write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {RELEASE_MD}")
    print(f"Wrote {RELEASE_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
