from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import sqlite3
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

from database.db import get_db_path
from utils.runtime_security import restrict_path_to_current_user


SETTINGS_FILE_NAME = "backup_settings.json"
DEFAULT_RETENTION = {
    "daily": 10,
    "weekly": 10,
    "monthly": 10,
    "manual": 100,
}


def app_base_dir() -> Path:
    import sys

    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def settings_path() -> Path:
    return app_base_dir() / SETTINGS_FILE_NAME


def default_settings() -> dict[str, Any]:
    return {
        "enabled": False,
        "backup_dir": "",
        "last_auto_attempt_date": "",
        "last_daily_date": "",
        "last_weekly_period": "",
        "last_monthly_period": "",
        "last_success_at": "",
        "last_error": "",
        "last_backup_path": "",
        "retention": DEFAULT_RETENTION.copy(),
    }


def load_backup_settings() -> dict[str, Any]:
    settings = default_settings()
    path = settings_path()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return settings
    if isinstance(raw, dict):
        settings.update(raw)
    retention = settings.get("retention")
    if not isinstance(retention, dict):
        retention = {}
    merged_retention = DEFAULT_RETENTION.copy()
    for key, value in retention.items():
        try:
            merged_retention[str(key)] = max(1, int(value))
        except (TypeError, ValueError):
            pass
    settings["retention"] = merged_retention
    return settings


def save_backup_settings(settings: dict[str, Any]) -> None:
    merged = default_settings()
    merged.update(settings)
    path = settings_path()
    path.write_text(json.dumps(merged, indent=2) + "\n", encoding="utf-8")
    try:
        restrict_path_to_current_user(str(path))
    except Exception:
        pass


def _resolve_backup_dir(raw_path: str) -> Path:
    raw_path = (raw_path or "").strip().strip('"')
    if not raw_path:
        raise ValueError("Backup folder is required.")
    target = Path(os.path.expandvars(os.path.expanduser(raw_path))).resolve()
    base = app_base_dir().resolve()
    try:
        common = os.path.commonpath([str(base), str(target)])
    except ValueError:
        common = ""
    if common == str(base):
        raise ValueError("Backup folder must be outside the AirportApp folder.")
    target.mkdir(parents=True, exist_ok=True)
    test_file = target / ".airportapp_write_test"
    test_file.write_text("ok", encoding="utf-8")
    test_file.unlink(missing_ok=True)
    return target


def configure_backup_folder(raw_path: str, enabled: bool = True) -> dict[str, Any]:
    target = _resolve_backup_dir(raw_path)
    settings = load_backup_settings()
    settings["backup_dir"] = str(target)
    settings["enabled"] = bool(enabled)
    settings["last_error"] = ""
    save_backup_settings(settings)
    return settings


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_sqlite_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute("PRAGMA integrity_check").fetchone()
    finally:
        conn.close()
    if not row or str(row[0]).lower() != "ok":
        raise ValueError(f"SQLite integrity check failed for {path.name}.")


def _database_paths() -> list[Path]:
    paths: dict[str, Path] = {}
    base = app_base_dir()
    try:
        for path in base.glob("*.db"):
            if path.is_file():
                paths[str(path.resolve()).lower()] = path.resolve()
    except OSError:
        pass
    db_path = Path(get_db_path()).resolve()
    if db_path.exists():
        paths[str(db_path).lower()] = db_path
    return sorted(paths.values(), key=lambda p: str(p).lower())


def _safe_sqlite_backup(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(str(source))
    dst = sqlite3.connect(str(destination))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def _backup_existing_database(source: Path, destination: Path) -> None:
    try:
        _safe_sqlite_backup(source, destination)
    except sqlite3.Error:
        shutil.copy2(source, destination)


def _archive_name_for_db(path: Path, used_names: set[str]) -> str:
    name = path.name
    if name.lower() not in used_names:
        used_names.add(name.lower())
        return f"databases/{name}"
    digest = hashlib.sha1(str(path).encode("utf-8")).hexdigest()[:8]
    archive_name = f"{path.stem}_{digest}{path.suffix}"
    used_names.add(archive_name.lower())
    return f"databases/{archive_name}"


def _read_optional_text(path: Path) -> str | None:
    try:
        if path.exists() and path.is_file():
            return path.read_text(encoding="utf-8")
    except OSError:
        return None
    return None


def _release_info() -> dict[str, Any]:
    base = app_base_dir()
    release_json = base / "app_release.json"
    try:
        if release_json.exists():
            raw = json.loads(release_json.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
    except (OSError, json.JSONDecodeError):
        pass
    text = _read_optional_text(base / "RELEASE_INFO.md")
    return {"release_info_text": text or ""}


def _apply_retention(root: Path, category: str, keep_count: int) -> None:
    folder = root / category
    try:
        files = sorted(
            [path for path in folder.glob("AirportApp_backup_*.zip") if path.is_file()],
            key=lambda path: path.name,
        )
    except OSError:
        return
    excess = len(files) - keep_count
    if excess <= 0:
        return
    for path in files[:excess]:
        try:
            path.unlink()
        except OSError:
            pass


def create_backup(category: str = "manual", reason: str = "manual") -> dict[str, Any]:
    settings = load_backup_settings()
    target_root = _resolve_backup_dir(str(settings.get("backup_dir") or ""))
    category = category if category in DEFAULT_RETENTION else "manual"
    target_dir = target_root / category
    target_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    stamp = now.strftime("%Y-%m-%d_%H%M%S")
    host = socket.gethostname() or "unknown-pc"
    zip_path = target_dir / f"AirportApp_backup_{category}_{stamp}.zip"
    db_paths = _database_paths()
    if not db_paths:
        raise FileNotFoundError("No database files were found to back up.")

    manifest: dict[str, Any] = {
        "app": "AirportApp",
        "reason": reason,
        "category": category,
        "created_at_local": now.isoformat(timespec="seconds"),
        "computer_name": host,
        "app_folder": str(app_base_dir()),
        "release": _release_info(),
        "databases": [],
    }

    with tempfile.TemporaryDirectory(prefix="airportapp_backup_") as tmp:
        tmp_dir = Path(tmp)
        used_names: set[str] = set()
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for source in db_paths:
                archive_name = _archive_name_for_db(source, used_names)
                temp_db = tmp_dir / archive_name.replace("/", "_")
                _safe_sqlite_backup(source, temp_db)
                zf.write(temp_db, archive_name)
                manifest["databases"].append(
                    {
                        "source_path": str(source),
                        "archive_name": archive_name,
                        "size_bytes": temp_db.stat().st_size,
                        "sha256": _sha256(temp_db),
                    }
                )

            for file_name in ("RELEASE_INFO.md", "app_release.json"):
                source = app_base_dir() / file_name
                if source.exists() and source.is_file():
                    zf.write(source, file_name)

            zf.writestr("backup_manifest.json", json.dumps(manifest, indent=2) + "\n")

    retention = settings.get("retention") or DEFAULT_RETENTION
    try:
        keep_count = int(retention.get(category, DEFAULT_RETENTION[category]))
    except (AttributeError, TypeError, ValueError):
        keep_count = DEFAULT_RETENTION[category]
    _apply_retention(target_root, category, keep_count)

    settings["last_success_at"] = now.isoformat(timespec="seconds")
    settings["last_backup_path"] = str(zip_path)
    settings["last_error"] = ""
    save_backup_settings(settings)

    manifest["backup_path"] = str(zip_path)
    manifest["backup_size_bytes"] = zip_path.stat().st_size
    return manifest


def run_scheduled_backup_if_due() -> dict[str, Any] | None:
    settings = load_backup_settings()
    if not settings.get("enabled") or not settings.get("backup_dir"):
        return None

    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    if settings.get("last_auto_attempt_date") == today:
        return None

    settings["last_auto_attempt_date"] = today
    save_backup_settings(settings)

    categories: list[tuple[str, str, str]] = []
    if settings.get("last_daily_date") != today:
        categories.append(("daily", "last_daily_date", today))

    week_period = f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"
    if settings.get("last_weekly_period") != week_period:
        categories.append(("weekly", "last_weekly_period", week_period))

    month_period = now.strftime("%Y-%m")
    if settings.get("last_monthly_period") != month_period:
        categories.append(("monthly", "last_monthly_period", month_period))

    if not categories:
        return None

    results = []
    try:
        for category, setting_key, period_value in categories:
            result = create_backup(category=category, reason="scheduled")
            results.append(result)
            settings = load_backup_settings()
            settings[setting_key] = period_value
            settings["last_error"] = ""
            save_backup_settings(settings)
    except Exception as exc:
        settings = load_backup_settings()
        settings["last_error"] = f"{type(exc).__name__}: {exc}"
        save_backup_settings(settings)
        raise

    return {"created": results}


def backup_status() -> dict[str, Any]:
    settings = load_backup_settings()
    backup_dir = str(settings.get("backup_dir") or "")
    exists = False
    counts: dict[str, int] = {}
    if backup_dir:
        root = Path(backup_dir)
        exists = root.exists() and root.is_dir()
        for category in DEFAULT_RETENTION:
            try:
                counts[category] = len(list((root / category).glob("AirportApp_backup_*.zip")))
            except OSError:
                counts[category] = 0
    return {
        "settings": settings,
        "settings_path": str(settings_path()),
        "app_folder": str(app_base_dir()),
        "backup_dir_exists": exists,
        "counts": counts,
    }


def extract_backup_zip(backup_zip: Path, destination_dir: Path) -> None:
    """Support helper for manual recovery tooling; not used by the web UI."""
    destination_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(backup_zip, "r") as zf:
        zf.extractall(destination_dir)
    try:
        restrict_path_to_current_user(str(destination_dir), is_dir=True)
    except Exception:
        pass


def _database_members(zf: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    members: list[zipfile.ZipInfo] = []
    for info in zf.infolist():
        if info.is_dir():
            continue
        normalized = info.filename.replace("\\", "/")
        if not normalized.startswith("databases/") or not normalized.lower().endswith(".db"):
            continue
        name = PurePosixPath(normalized).name
        if not name or name in {".", ".."}:
            continue
        members.append(info)
    return members


def restore_backup_zip(backup_zip: str | Path) -> dict[str, Any]:
    source_zip = Path(backup_zip).resolve()
    if not source_zip.exists() or not source_zip.is_file():
        raise FileNotFoundError(f"Backup ZIP was not found: {source_zip}")
    if source_zip.suffix.lower() != ".zip":
        raise ValueError("Selected backup must be a .zip file.")

    base = app_base_dir()
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    pre_restore_dir = base / "backups" / f"pre_restore_{stamp}"
    restored: list[dict[str, Any]] = []
    previous: list[dict[str, Any]] = []

    with tempfile.TemporaryDirectory(prefix="airportapp_restore_") as tmp:
        tmp_dir = Path(tmp)
        extracted_dir = tmp_dir / "databases"
        extracted_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(source_zip, "r") as zf:
            members = _database_members(zf)
            if not members:
                raise ValueError("Backup ZIP does not contain any databases.")
            if not any(PurePosixPath(m.filename.replace("\\", "/")).name.lower() == "airport_app.db" for m in members):
                raise ValueError("Backup ZIP does not contain databases/airport_app.db.")

            seen_names: set[str] = set()
            extracted: list[tuple[Path, Path]] = []
            for member in members:
                db_name = PurePosixPath(member.filename.replace("\\", "/")).name
                if db_name.lower() in seen_names:
                    raise ValueError(f"Backup ZIP contains duplicate database name: {db_name}")
                seen_names.add(db_name.lower())
                extracted_path = extracted_dir / db_name
                with zf.open(member, "r") as src, extracted_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                _validate_sqlite_db(extracted_path)
                extracted.append((extracted_path, base / db_name))

        pre_restore_dir.mkdir(parents=True, exist_ok=True)
        for current in _database_paths():
            backup_path = pre_restore_dir / current.name
            _backup_existing_database(current, backup_path)
            previous.append(
                {
                    "source_path": str(current),
                    "backup_path": str(backup_path),
                    "size_bytes": backup_path.stat().st_size,
                    "sha256": _sha256(backup_path),
                }
            )

        for extracted_path, target_path in extracted:
            tmp_target = target_path.with_name(f"{target_path.name}.restore_new")
            shutil.copy2(extracted_path, tmp_target)
            os.replace(tmp_target, target_path)
            restored.append(
                {
                    "target_path": str(target_path),
                    "size_bytes": target_path.stat().st_size,
                    "sha256": _sha256(target_path),
                }
            )

    return {
        "backup_zip": str(source_zip),
        "pre_restore_backup_dir": str(pre_restore_dir),
        "restored": restored,
        "previous": previous,
    }
