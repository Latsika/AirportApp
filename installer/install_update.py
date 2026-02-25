from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox


APP_EXE_NAME = "AirportApp.exe"
DB_FILE_NAME = "airport_app.db"
PAYLOAD_RELATIVE_PATH = os.path.join("payload", APP_EXE_NAME)


def _payload_base() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(getattr(sys, "_MEIPASS"))
    return Path(__file__).resolve().parents[1]


def _payload_exe_path() -> Path:
    return _payload_base() / PAYLOAD_RELATIVE_PATH


def _choose_target_dir() -> Path | None:
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    selected = filedialog.askdirectory(
        title="Select folder that contains AirportApp.exe",
    )
    root.destroy()
    if not selected:
        return None
    return Path(selected)


def _stop_running_target_exe(target_exe: Path) -> None:
    path = str(target_exe.resolve()).replace("'", "''")
    cmd = (
        "Get-Process | "
        "Where-Object { $_.Path -eq '" + path + "' } | "
        "Stop-Process -Force"
    )
    subprocess.run(
        ["powershell", "-NoProfile", "-Command", cmd],
        capture_output=True,
        text=True,
        check=False,
    )


def _backup_db(target_dir: Path) -> Path | None:
    db_path = target_dir / DB_FILE_NAME
    if not db_path.exists():
        return None
    backups_dir = target_dir / "backups"
    backups_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    backup_path = backups_dir / f"airport_app_update_{ts}.db"
    shutil.copy2(db_path, backup_path)
    return backup_path


def _update_exe(target_dir: Path, source_exe: Path) -> None:
    target_exe = target_dir / APP_EXE_NAME
    tmp_exe = target_dir / f"{APP_EXE_NAME}.new"
    shutil.copy2(source_exe, tmp_exe)
    os.replace(tmp_exe, target_exe)


def _run() -> int:
    source_exe = _payload_exe_path()
    if not source_exe.exists():
        messagebox.showerror(
            "Update failed",
            "Updater payload is missing AirportApp.exe.",
        )
        return 1

    if len(sys.argv) > 1:
        target_dir = Path(sys.argv[1]).resolve()
    else:
        target_dir = _choose_target_dir()
        if target_dir is None:
            return 0

    if not target_dir.exists() or not target_dir.is_dir():
        messagebox.showerror("Update failed", "Selected path is not a folder.")
        return 1

    target_exe = target_dir / APP_EXE_NAME
    if not target_exe.exists():
        messagebox.showerror(
            "Update failed",
            "Selected folder does not contain AirportApp.exe",
        )
        return 1

    try:
        _stop_running_target_exe(target_exe)
        backup_path = _backup_db(target_dir)
        _update_exe(target_dir, source_exe)
    except PermissionError:
        messagebox.showerror(
            "Update failed",
            "AirportApp.exe is in use. Close AirportApp and run update again.",
        )
        return 1
    except Exception as exc:
        messagebox.showerror("Update failed", f"{type(exc).__name__}: {exc}")
        return 1

    backup_info = (
        f"Database backup created:\n{backup_path}\n\n"
        if backup_path is not None
        else "No database file was found in selected folder.\n\n"
    )
    messagebox.showinfo(
        "Update complete",
        "AirportApp was updated successfully.\n\n"
        f"{backup_info}"
        "User data and sales data were preserved.",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_run())
