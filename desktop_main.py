from __future__ import annotations

import ctypes
import hashlib
import json
import os
import threading
import time
import webbrowser
from datetime import datetime

from werkzeug.serving import make_server

from web.app import app


def _runtime_dir() -> str:
    if getattr(__import__("sys"), "frozen", False):
        return os.path.dirname(__import__("sys").executable)
    return os.path.abspath(os.path.dirname(__file__))


def _write_crash_log(message: str) -> None:
    path = os.path.join(_runtime_dir(), "crash.log")
    stamp = datetime.utcnow().isoformat() + "Z"
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"[{stamp}] {message}\n")


def _show_error(message: str) -> None:
    try:
        ctypes.windll.user32.MessageBoxW(0, message, "AirportApp - Error", 0x10)
    except Exception:
        pass


def _mutex_name() -> str:
    digest = hashlib.sha1(_runtime_dir().encode("utf-8")).hexdigest()
    return f"Local\\AirportApp_{digest}"


def _runtime_state_path() -> str:
    return os.path.join(_runtime_dir(), "app_runtime.json")


def _open_existing_instance() -> bool:
    try:
        with open(_runtime_state_path(), "r", encoding="utf-8") as f:
            state = json.load(f)
        port = int(state.get("port") or 0)
    except Exception:
        port = 0
    if port <= 0:
        _show_error("AirportApp is already running. Use the existing browser window.")
        return False
    webbrowser.open_new(f"http://127.0.0.1:{port}")
    return True


def _acquire_single_instance_mutex():
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.CreateMutexW(None, False, _mutex_name())
    if not handle:
        raise OSError("CreateMutexW failed")
    already_exists = kernel32.GetLastError() == 183
    return handle, already_exists


def _release_single_instance_mutex(handle) -> None:
    try:
        ctypes.windll.kernel32.ReleaseMutex(handle)
    except Exception:
        pass
    try:
        ctypes.windll.kernel32.CloseHandle(handle)
    except Exception:
        pass


class _ServerThread(threading.Thread):
    def __init__(self, host: str, port: int):
        super().__init__(daemon=True)
        self._server = make_server(host, port, app, threaded=True)
        self.port = int(self._server.server_port)
        self._ctx = app.app_context()
        self._ctx.push()

    def run(self) -> None:
        self._server.serve_forever()

    def shutdown(self) -> None:
        self._server.shutdown()
        self._ctx.pop()


def main() -> None:
    mutex_handle, already_running = _acquire_single_instance_mutex()
    if already_running:
        _open_existing_instance()
        _release_single_instance_mutex(mutex_handle)
        return

    host = "127.0.0.1"
    raw_port = os.environ.get("AIRPORTAPP_PORT", "").strip()
    try:
        port = int(raw_port) if raw_port else 0
    except ValueError:
        port = 0

    server = None
    try:
        server = _ServerThread(host, port)
        server.start()

        with open(_runtime_state_path(), "w", encoding="utf-8") as f:
            json.dump({"host": host, "port": server.port, "pid": os.getpid()}, f)

        url = f"http://{host}:{server.port}"
        time.sleep(0.35)
        webbrowser.open_new(url)

        try:
            while server.is_alive():
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
    finally:
        if server is not None:
            server.shutdown()
        try:
            os.remove(_runtime_state_path())
        except OSError:
            pass
        _release_single_instance_mutex(mutex_handle)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        text = f"{type(exc).__name__}: {exc}"
        _write_crash_log(text)
        _show_error(
            "AirportApp failed to start.\n\n"
            f"{text}\n\n"
            "Details were written to crash.log in the app folder."
        )
