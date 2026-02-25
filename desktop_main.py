from __future__ import annotations

import ctypes
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
    host = "127.0.0.1"
    raw_port = os.environ.get("AIRPORTAPP_PORT", "").strip()
    try:
        port = int(raw_port) if raw_port else 0
    except ValueError:
        port = 0

    server = _ServerThread(host, port)
    server.start()

    url = f"http://{host}:{server.port}"
    time.sleep(0.35)
    webbrowser.open_new(url)

    try:
        while server.is_alive():
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()


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
