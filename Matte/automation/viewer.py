from __future__ import annotations

import argparse
import json
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
from urllib.parse import parse_qs, unquote, urlparse
import webbrowser

if __package__ in {None, ""}:
    package_dir = Path(__file__).resolve().parent
    package_parent = package_dir.parent
    if str(package_parent) not in sys.path:
        sys.path.insert(0, str(package_parent))
    __package__ = package_dir.name

from .results import resolve_experiment_root
from .schedule_viewer_data import list_schedule_view, load_schedule_view, preview_schedule_view
from .viewer_data import list_run_experiments, load_experiment_view, load_run_view


STATIC_DIR = Path(__file__).resolve().parent / "viewer_static"
DEFAULT_RESULTS_ROOT = Path(__file__).resolve().parent / "runs"
DEFAULT_SCHEDULES_DIR = Path(__file__).resolve().parent / "schedules"
DEFAULT_SCHEDULE_QUEUE_PATH = Path(__file__).resolve().parent / "schedule_queue.yaml"
DEFAULT_TIMES_CSV_PATH = Path(__file__).resolve().parents[2] / "Part2summary_times.csv"


class _RunViewerHandler(SimpleHTTPRequestHandler):
    def __init__(
        self,
        *args,
        results_root: Path,
        schedules_dir: Path,
        schedule_queue_path: Path | None,
        times_csv_path: Path,
        default_experiment_id: str | None,
        **kwargs,
    ) -> None:
        self.results_root = results_root
        self.schedules_dir = schedules_dir
        self.schedule_queue_path = schedule_queue_path
        self.times_csv_path = times_csv_path
        self.default_experiment_id = default_experiment_id
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            self._handle_api(parsed)
            return
        if parsed.path == "/":
            self.path = "/index.html"
        else:
            self.path = parsed.path
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            self._handle_api_post(parsed)
            return
        self._write_json(404, {"error": "Not found"})

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, format: str, *args) -> None:
        return

    def _handle_api(self, parsed) -> None:
        try:
            if parsed.path == "/api/experiments":
                self._write_json(
                    200,
                    {
                        "experiments": list_run_experiments(self.results_root),
                        "default_experiment_id": self._default_experiment_id(),
                    },
                )
                return

            if parsed.path == "/api/runs":
                experiment_id = self._resolve_experiment_id(parse_qs(parsed.query))
                self._write_json(200, load_experiment_view(self.results_root, experiment_id))
                return

            if parsed.path.startswith("/api/runs/"):
                run_id = unquote(parsed.path.removeprefix("/api/runs/"))
                experiment_id = self._resolve_experiment_id(parse_qs(parsed.query))
                self._write_json(200, load_run_view(self.results_root, experiment_id, run_id))
                return

            if parsed.path == "/api/schedules":
                self._write_json(
                    200,
                    list_schedule_view(
                        schedules_dir=self.schedules_dir,
                        schedule_queue_path=self.schedule_queue_path,
                        times_csv_path=self.times_csv_path,
                    ),
                )
                return

            if parsed.path.startswith("/api/schedules/"):
                schedule_id = unquote(parsed.path.removeprefix("/api/schedules/"))
                self._write_json(
                    200,
                    load_schedule_view(
                        schedules_dir=self.schedules_dir,
                        schedule_queue_path=self.schedule_queue_path,
                        times_csv_path=self.times_csv_path,
                        schedule_id=schedule_id,
                    ),
                )
                return

            self._write_json(404, {"error": "Not found"})
        except FileNotFoundError as exc:
            self._write_json(404, {"error": str(exc)})
        except ValueError as exc:
            self._write_json(400, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover - defensive HTTP surface
            self._write_json(500, {"error": f"Unexpected server error: {exc}"})

    def _handle_api_post(self, parsed) -> None:
        try:
            if parsed.path == "/api/schedules/preview":
                self._write_json(
                    200,
                    preview_schedule_view(
                        times_csv_path=self.times_csv_path,
                        payload=self._read_json_body(),
                    ),
                )
                return
            self._write_json(404, {"error": "Not found"})
        except json.JSONDecodeError as exc:
            self._write_json(400, {"error": f"Request body is not valid JSON: {exc}"})
        except ValueError as exc:
            self._write_json(400, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover - defensive HTTP surface
            self._write_json(500, {"error": f"Unexpected server error: {exc}"})

    def _read_json_body(self) -> dict[str, object]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        payload = json.loads(raw.decode("utf-8") if raw else "{}")
        if not isinstance(payload, dict):
            raise ValueError("Request body must contain a JSON object")
        return payload

    def _default_experiment_id(self) -> str | None:
        if self.default_experiment_id is not None:
            return self.default_experiment_id
        experiments = list_run_experiments(self.results_root)
        if not experiments:
            return None
        return str(experiments[0]["experiment_id"])

    def _resolve_experiment_id(self, query_params: dict[str, list[str]]) -> str:
        experiment_values = query_params.get("experiment", [])
        if experiment_values and experiment_values[0]:
            return experiment_values[0]
        default_experiment_id = self._default_experiment_id()
        if default_experiment_id is None:
            raise FileNotFoundError(f"No experiment directories found in {self.results_root}")
        return default_experiment_id

    def _write_json(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def launch_run_viewer(
    *,
    results_root: Path,
    schedules_dir: Path = DEFAULT_SCHEDULES_DIR,
    schedule_queue_path: Path | None = DEFAULT_SCHEDULE_QUEUE_PATH,
    times_csv_path: Path = DEFAULT_TIMES_CSV_PATH,
    experiment_id: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8000,
    open_browser: bool = True,
) -> int:
    if not results_root.exists():
        raise FileNotFoundError(f"Results root not found: {results_root}")
    if not STATIC_DIR.exists():
        raise FileNotFoundError(f"Viewer assets not found: {STATIC_DIR}")
    if experiment_id is not None:
        resolve_experiment_root(results_root, experiment_id)

    server = ThreadingHTTPServer(
        (host, port),
        partial(
            _RunViewerHandler,
            results_root=results_root,
            schedules_dir=schedules_dir,
            schedule_queue_path=schedule_queue_path,
            times_csv_path=times_csv_path,
            default_experiment_id=experiment_id,
        ),
    )

    bound_host, bound_port = server.server_address[:2]
    display_host = "127.0.0.1" if bound_host in {"0.0.0.0", "::"} else bound_host
    url = f"http://{display_host}:{bound_port}/"
    print(f"Run viewer available at {url}")
    if open_browser:
        try:
            webbrowser.open(url, new=2)
        except Exception as exc:  # pragma: no cover - browser integration is platform-specific
            print(f"Could not open browser automatically: {exc}")
    print("Press Ctrl+C to stop the server.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping run viewer.")
    finally:
        server.server_close()
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve the Part 3 run timing viewer")
    parser.add_argument("--experiment")
    parser.add_argument("--results-root", default=str(DEFAULT_RESULTS_ROOT))
    parser.add_argument("--schedules-dir", default=str(DEFAULT_SCHEDULES_DIR))
    parser.add_argument("--schedule-queue", default=str(DEFAULT_SCHEDULE_QUEUE_PATH))
    parser.add_argument("--times-csv", default=str(DEFAULT_TIMES_CSV_PATH))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--no-open", action="store_true", help="Print the URL without opening a browser")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    return launch_run_viewer(
        results_root=Path(args.results_root).resolve(),
        schedules_dir=Path(args.schedules_dir).resolve(),
        schedule_queue_path=Path(args.schedule_queue).resolve() if args.schedule_queue else None,
        times_csv_path=Path(args.times_csv).resolve(),
        experiment_id=args.experiment,
        host=args.host,
        port=args.port,
        open_browser=not args.no_open,
    )


if __name__ == "__main__":
    raise SystemExit(main())
