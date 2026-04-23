from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any

if __package__ in {None, ""}:
    package_dir = Path(__file__).resolve().parent
    package_parent = package_dir.parent
    if str(package_parent) not in sys.path:
        sys.path.insert(0, str(package_parent))
    __package__ = package_dir.name

try:
    import tkinter as tk
    from tkinter import messagebox, ttk

    TKINTER_IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - depends on local Python build
    tk = None  # type: ignore[assignment]
    ttk = None  # type: ignore[assignment]
    messagebox = None  # type: ignore[assignment]
    TKINTER_IMPORT_ERROR = exc

from .audit import (
    AuditJob,
    AuditMemcached,
    AuditReport,
    audit_schedule,
    build_schedule_model,
    dependency_text,
    estimate_runtime,
    load_runtime_table,
    load_schedule_model,
    parse_dependency_text,
    write_policy_document,
)
from .catalog import JOB_CATALOG, NODE_A, NODE_B, suggested_core_sets, validate_node_core_spec


TIMELINE_COLORS = (
    "#d95f02",
    "#1b9e77",
    "#7570b3",
    "#e7298a",
    "#66a61e",
    "#e6ab02",
    "#a6761d",
    "#1f78b4",
)

DEFAULT_POLICY_PATH = Path(__file__).resolve().with_name("schedule.yaml")
DEFAULT_TIMES_CSV_PATH = Path(__file__).resolve().parents[2] / "Part2summary_times.csv"


@dataclass(frozen=True)
class PlannerJobState:
    job_id: str
    order: int
    node: str
    cores: str
    threads: int
    after_text: str
    delay_s: int


@dataclass(frozen=True)
class PlannerState:
    policy_name: str
    memcached_node: str
    memcached_cores: str
    memcached_threads: int
    jobs: tuple[PlannerJobState, ...]


@dataclass
class JobRowWidgets:
    order_var: tk.StringVar
    node_var: tk.StringVar
    cores_var: tk.StringVar
    threads_var: tk.StringVar
    after_var: tk.StringVar
    delay_var: tk.StringVar
    duration_var: tk.StringVar
    cores_box: ttk.Combobox


def planner_state_from_model(model) -> PlannerState:
    ordered_jobs = sorted(model.jobs.values(), key=lambda item: (item.order, item.job_id))
    return PlannerState(
        policy_name=model.policy_name,
        memcached_node=model.memcached.node,
        memcached_cores=model.memcached.cores,
        memcached_threads=model.memcached.threads,
        jobs=tuple(
            PlannerJobState(
                job_id=job.job_id,
                order=index + 1,
                node=job.node,
                cores=job.cores,
                threads=job.threads,
                after_text=dependency_text(job.dependencies),
                delay_s=job.delay_s,
            )
            for index, job in enumerate(ordered_jobs)
        ),
    )


def build_model_from_planner_state(
    state: PlannerState,
    *,
    config_path: Path | None = None,
    parse_errors: tuple[str, ...] = (),
) -> object:
    jobs = {
        job.job_id: AuditJob(
            job_id=job.job_id,
            node=job.node,
            cores=job.cores,
            threads=job.threads,
            dependencies=parse_dependency_text(job.after_text),
            delay_s=job.delay_s,
            order=job.order,
        )
        for job in sorted(state.jobs, key=lambda item: (item.order, item.job_id))
    }
    return build_schedule_model(
        policy_name=state.policy_name,
        memcached=AuditMemcached(
            node=state.memcached_node,
            cores=state.memcached_cores,
            threads=state.memcached_threads,
        ),
        jobs=jobs,
        config_path=config_path,
        parse_errors=parse_errors,
    )


class PlannerApp:
    def __init__(self, root: Any, *, policy_path: Path, times_csv_path: Path) -> None:
        self.root = root
        self.policy_path = policy_path
        self.runtime_table = load_runtime_table(str(times_csv_path))
        self.status_var = tk.StringVar(value="Loading...")
        self.makespan_var = tk.StringVar(value="Estimated makespan: n/a")
        self.policy_name_var = tk.StringVar()
        self.memcached_node_var = tk.StringVar()
        self.memcached_cores_var = tk.StringVar()
        self.memcached_threads_var = tk.StringVar()
        self.validation_text: tk.Text | None = None
        self.node_canvases: dict[str, tk.Canvas] = {}
        self.job_rows: dict[str, JobRowWidgets] = {}
        self.color_by_job = {
            "memcached": "#9e9e9e",
            **{job_id: TIMELINE_COLORS[index % len(TIMELINE_COLORS)] for index, job_id in enumerate(sorted(JOB_CATALOG))},
        }
        self._refresh_after_id: str | None = None
        self._build_ui()
        self.reload_from_disk()

    def _build_ui(self) -> None:
        self.root.title("Part 3 Schedule Planner")
        self.root.geometry("1360x980")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(3, weight=1)

        top_bar = ttk.Frame(self.root, padding=12)
        top_bar.grid(row=0, column=0, sticky="ew")
        top_bar.columnconfigure(1, weight=1)
        ttk.Label(top_bar, text="Policy file:").grid(row=0, column=0, sticky="w")
        ttk.Label(top_bar, text=str(self.policy_path)).grid(row=0, column=1, sticky="w")
        ttk.Button(top_bar, text="Reload", command=self.reload_from_disk).grid(row=0, column=2, padx=(12, 0))
        ttk.Button(top_bar, text="Save", command=self.save_to_disk).grid(row=0, column=3, padx=(8, 0))
        ttk.Label(top_bar, textvariable=self.status_var).grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Label(top_bar, textvariable=self.makespan_var).grid(row=1, column=2, columnspan=2, sticky="e", pady=(8, 0))

        general_frame = ttk.LabelFrame(self.root, text="Policy", padding=12)
        general_frame.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 12))
        general_frame.columnconfigure(1, weight=1)
        ttk.Label(general_frame, text="Policy name").grid(row=0, column=0, sticky="w")
        ttk.Entry(general_frame, textvariable=self.policy_name_var, width=40).grid(row=0, column=1, sticky="ew")
        ttk.Label(general_frame, text="Memcached node").grid(row=0, column=2, sticky="w", padx=(12, 0))
        memcached_node_box = ttk.Combobox(
            general_frame,
            textvariable=self.memcached_node_var,
            values=(NODE_A, NODE_B),
            width=16,
            state="readonly",
        )
        memcached_node_box.grid(row=0, column=3, sticky="w")
        ttk.Label(general_frame, text="Memcached cores").grid(row=0, column=4, sticky="w", padx=(12, 0))
        self.memcached_cores_box = ttk.Combobox(general_frame, textvariable=self.memcached_cores_var, width=12)
        self.memcached_cores_box.grid(row=0, column=5, sticky="w")
        ttk.Label(general_frame, text="Memcached threads").grid(row=0, column=6, sticky="w", padx=(12, 0))
        tk.Spinbox(general_frame, from_=1, to=8, width=6, textvariable=self.memcached_threads_var).grid(row=0, column=7, sticky="w")

        jobs_frame = ttk.LabelFrame(self.root, text="Jobs", padding=12)
        jobs_frame.grid(row=2, column=0, sticky="ew", padx=12, pady=(0, 12))
        headers = ("Job", "Order", "Node", "Cores", "Threads", "After", "Delay", "Est. runtime")
        for column, header in enumerate(headers):
            ttk.Label(jobs_frame, text=header).grid(row=0, column=column, sticky="w", padx=(0, 8), pady=(0, 8))
        for row_index, job_id in enumerate(sorted(JOB_CATALOG), start=1):
            ttk.Label(jobs_frame, text=job_id).grid(row=row_index, column=0, sticky="w", padx=(0, 8))
            widgets = JobRowWidgets(
                order_var=tk.StringVar(),
                node_var=tk.StringVar(),
                cores_var=tk.StringVar(),
                threads_var=tk.StringVar(),
                after_var=tk.StringVar(),
                delay_var=tk.StringVar(),
                duration_var=tk.StringVar(value="n/a"),
                cores_box=ttk.Combobox(jobs_frame, width=12),
            )
            self.job_rows[job_id] = widgets
            tk.Spinbox(jobs_frame, from_=1, to=99, width=6, textvariable=widgets.order_var).grid(row=row_index, column=1, sticky="w", padx=(0, 8))
            node_box = ttk.Combobox(
                jobs_frame,
                textvariable=widgets.node_var,
                values=(NODE_A, NODE_B),
                width=16,
                state="readonly",
            )
            node_box.grid(row=row_index, column=2, sticky="w", padx=(0, 8))
            widgets.cores_box.grid(row=row_index, column=3, sticky="w", padx=(0, 8))
            tk.Spinbox(jobs_frame, from_=1, to=8, width=6, textvariable=widgets.threads_var).grid(row=row_index, column=4, sticky="w", padx=(0, 8))
            ttk.Entry(jobs_frame, textvariable=widgets.after_var, width=24).grid(row=row_index, column=5, sticky="w", padx=(0, 8))
            tk.Spinbox(jobs_frame, from_=0, to=3600, width=6, textvariable=widgets.delay_var).grid(row=row_index, column=6, sticky="w", padx=(0, 8))
            ttk.Label(jobs_frame, textvariable=widgets.duration_var, width=12).grid(row=row_index, column=7, sticky="w")
            node_box.bind("<<ComboboxSelected>>", lambda _event, current_job=job_id: self._update_job_core_values(current_job))

        feedback_frame = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        feedback_frame.grid(row=3, column=0, sticky="nsew")
        feedback_frame.columnconfigure(0, weight=1)
        feedback_frame.columnconfigure(1, weight=1)
        feedback_frame.rowconfigure(0, weight=1)

        validation_frame = ttk.LabelFrame(feedback_frame, text="Validation", padding=12)
        validation_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        validation_frame.rowconfigure(0, weight=1)
        validation_frame.columnconfigure(0, weight=1)
        self.validation_text = tk.Text(validation_frame, wrap="word", width=58, height=18)
        self.validation_text.grid(row=0, column=0, sticky="nsew")
        self.validation_text.tag_configure("error", foreground="#b71c1c")
        self.validation_text.tag_configure("warning", foreground="#a35d00")
        self.validation_text.tag_configure("info", foreground="#1b1b1b")

        timeline_frame = ttk.LabelFrame(feedback_frame, text="Estimated node timelines", padding=12)
        timeline_frame.grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        timeline_frame.columnconfigure(0, weight=1)
        timeline_frame.columnconfigure(1, weight=1)
        for column, node in enumerate((NODE_A, NODE_B)):
            node_frame = ttk.LabelFrame(timeline_frame, text=node, padding=8)
            node_frame.grid(row=0, column=column, sticky="nsew", padx=(0 if column == 0 else 8, 0))
            canvas = tk.Canvas(node_frame, width=520, height=320, background="white", highlightthickness=1, highlightbackground="#cccccc")
            canvas.pack(fill="both", expand=True)
            self.node_canvases[node] = canvas

        all_vars: list[tk.Variable] = [
            self.policy_name_var,
            self.memcached_node_var,
            self.memcached_cores_var,
            self.memcached_threads_var,
        ]
        for widgets in self.job_rows.values():
            all_vars.extend(
                (
                    widgets.order_var,
                    widgets.node_var,
                    widgets.cores_var,
                    widgets.threads_var,
                    widgets.after_var,
                    widgets.delay_var,
                )
            )
        for variable in all_vars:
            variable.trace_add("write", lambda *_args: self._schedule_refresh())
        self.memcached_node_var.trace_add("write", lambda *_args: self._update_memcached_core_values())

    def _update_memcached_core_values(self) -> None:
        node = self.memcached_node_var.get() or NODE_B
        values = suggested_core_sets(node)
        self.memcached_cores_box.configure(values=values)
        current_value = self.memcached_cores_var.get().strip()
        try:
            is_valid = bool(current_value) and bool(validate_node_core_spec(current_value, node))
        except ValueError:
            is_valid = False
        if not is_valid:
            self.memcached_cores_var.set(values[0])

    def _update_job_core_values(self, job_id: str) -> None:
        widgets = self.job_rows[job_id]
        node = widgets.node_var.get() or JOB_CATALOG[job_id].default_node
        values = JOB_CATALOG[job_id].suggested_cores_by_node[node]
        widgets.cores_box.configure(values=values)
        current_value = widgets.cores_var.get().strip()
        try:
            is_valid = bool(current_value) and bool(validate_node_core_spec(current_value, node))
        except ValueError:
            is_valid = False
        if not is_valid:
            widgets.cores_var.set(values[0])

    def _safe_int(self, raw: str, fallback: int) -> int:
        try:
            return int(raw.strip())
        except (TypeError, ValueError, AttributeError):
            return fallback

    def _collect_state(self) -> tuple[PlannerState, tuple[str, ...]]:
        parse_errors: list[str] = []
        jobs: list[PlannerJobState] = []
        for job_id, widgets in self.job_rows.items():
            order = self._safe_int(widgets.order_var.get(), 0)
            if order <= 0:
                parse_errors.append(f"{job_id} order must be a positive integer")
                order = 1
            threads = self._safe_int(widgets.threads_var.get(), 0)
            if threads <= 0:
                parse_errors.append(f"{job_id} threads must be a positive integer")
            delay_s = self._safe_int(widgets.delay_var.get(), 0)
            jobs.append(
                PlannerJobState(
                    job_id=job_id,
                    order=order,
                    node=widgets.node_var.get().strip(),
                    cores=widgets.cores_var.get().strip(),
                    threads=threads,
                    after_text=widgets.after_var.get().strip() or "start",
                    delay_s=delay_s,
                )
            )
        memcached_threads = self._safe_int(self.memcached_threads_var.get(), 0)
        if memcached_threads <= 0:
            parse_errors.append("memcached threads must be a positive integer")
        state = PlannerState(
            policy_name=self.policy_name_var.get().strip() or "planner-policy",
            memcached_node=self.memcached_node_var.get().strip(),
            memcached_cores=self.memcached_cores_var.get().strip(),
            memcached_threads=memcached_threads,
            jobs=tuple(jobs),
        )
        return state, tuple(parse_errors)

    def _schedule_refresh(self) -> None:
        if self._refresh_after_id is not None:
            self.root.after_cancel(self._refresh_after_id)
        self._refresh_after_id = self.root.after(75, self.refresh_view)

    def reload_from_disk(self) -> None:
        model = load_schedule_model(str(self.policy_path))
        state = planner_state_from_model(model)
        self.policy_name_var.set(state.policy_name)
        self.memcached_node_var.set(state.memcached_node)
        self.memcached_cores_var.set(state.memcached_cores)
        self.memcached_threads_var.set(str(state.memcached_threads))
        self._update_memcached_core_values()
        for job in state.jobs:
            widgets = self.job_rows[job.job_id]
            widgets.order_var.set(str(job.order))
            widgets.node_var.set(job.node)
            self._update_job_core_values(job.job_id)
            widgets.cores_var.set(job.cores)
            widgets.threads_var.set(str(job.threads))
            widgets.after_var.set(job.after_text)
            widgets.delay_var.set(str(job.delay_s))
        self.refresh_view()

    def _report_to_text(self, report: AuditReport) -> None:
        assert self.validation_text is not None
        self.validation_text.configure(state="normal")
        self.validation_text.delete("1.0", "end")
        self.validation_text.insert("end", f"Status: {report.status}\n", "info")
        self.validation_text.insert("end", f"Estimated makespan: {report.makespan_s:.2f}s\n\n" if report.makespan_s is not None else "Estimated makespan: n/a\n\n", "info")
        if report.errors:
            self.validation_text.insert("end", "Errors\n", "error")
            for issue in report.errors:
                self.validation_text.insert("end", f"- {issue.message}\n", "error")
            self.validation_text.insert("end", "\n", "info")
        if report.warnings:
            self.validation_text.insert("end", "Warnings\n", "warning")
            for issue in report.warnings:
                self.validation_text.insert("end", f"- {issue.message}\n", "warning")
            self.validation_text.insert("end", "\n", "info")
        self.validation_text.insert("end", "Jobs\n", "info")
        for window in sorted(report.jobs.values(), key=lambda item: (item.start_s, item.end_s, item.label)):
            self.validation_text.insert(
                "end",
                (
                    f"- {window.label}: node={window.node} cores={window.cores} threads={window.threads} "
                    f"after={dependency_text(window.dependencies)} start={window.start_s:.2f}s "
                    f"end={window.end_s:.2f}s duration={window.duration_s:.2f}s\n"
                ),
                "info",
            )
        self.validation_text.configure(state="disabled")

    def _draw_node_timeline(self, node: str, report: AuditReport) -> None:
        canvas = self.node_canvases[node]
        canvas.delete("all")
        width = int(canvas.cget("width"))
        height = int(canvas.cget("height"))
        left = 50
        right = width - 20
        top = 24
        bottom = height - 30
        core_count = 8 if node == NODE_A else 4
        usable_width = max(right - left, 1)
        usable_height = max(bottom - top, 1)
        row_height = usable_height / core_count
        scale_max = max(report.makespan_s or 0.0, 1.0)
        error_jobs = {job_id for issue in report.errors for job_id in issue.jobs}

        for core in range(core_count + 1):
            y = top + (core * row_height)
            canvas.create_line(left, y, right, y, fill="#dddddd")
            if core < core_count:
                canvas.create_text(22, y + (row_height / 2), text=str(core), fill="#555555")
        canvas.create_line(left, top, left, bottom, fill="#999999")
        canvas.create_line(left, bottom, right, bottom, fill="#999999")
        canvas.create_text(left, bottom + 14, text="0s", anchor="w", fill="#555555")
        canvas.create_text(right, bottom + 14, text=f"{scale_max:.1f}s", anchor="e", fill="#555555")

        for window in report.windows_by_node.get(node, []):
            x1 = left + ((window.start_s / scale_max) * usable_width)
            x2 = left + ((window.end_s / scale_max) * usable_width)
            if x2 - x1 < 2:
                x2 = x1 + 2
            fill = self.color_by_job.get(window.job_id, "#64b5f6")
            outline = "#c62828" if window.job_id in error_jobs else "#333333"
            text_color = "#ffffff" if window.job_id != "memcached" else "#111111"
            segments: list[tuple[int, int]] = []
            start_core = window.core_ids[0]
            end_core = window.core_ids[0]
            for core_id in window.core_ids[1:]:
                if core_id == end_core + 1:
                    end_core = core_id
                    continue
                segments.append((start_core, end_core))
                start_core = core_id
                end_core = core_id
            segments.append((start_core, end_core))
            for segment_start, segment_end in segments:
                y1 = top + (segment_start * row_height)
                y2 = top + ((segment_end + 1) * row_height)
                canvas.create_rectangle(x1, y1, x2, y2, fill=fill, outline=outline, width=2 if window.job_id in error_jobs else 1)
                if (x2 - x1) >= 50:
                    canvas.create_text((x1 + x2) / 2, (y1 + y2) / 2, text=window.label, fill=text_color)

    def refresh_view(self) -> None:
        self._refresh_after_id = None
        state, parse_errors = self._collect_state()
        model = build_model_from_planner_state(state, config_path=self.policy_path, parse_errors=parse_errors)
        report = audit_schedule(model, self.runtime_table)
        self.status_var.set(f"Status: {report.status}")
        self.makespan_var.set(
            f"Estimated makespan: {report.makespan_s:.2f}s" if report.makespan_s is not None else "Estimated makespan: n/a"
        )
        for job_id, widgets in self.job_rows.items():
            runtime = estimate_runtime(job_id, self._safe_int(widgets.threads_var.get(), 0), self.runtime_table)
            widgets.duration_var.set("n/a" if runtime is None else f"{runtime:.2f}s")
        self._report_to_text(report)
        for node in (NODE_A, NODE_B):
            self._draw_node_timeline(node, report)

    def save_to_disk(self) -> None:
        state, parse_errors = self._collect_state()
        model = build_model_from_planner_state(state, config_path=self.policy_path, parse_errors=parse_errors)
        report = audit_schedule(model, self.runtime_table)
        if report.errors:
            messagebox.showerror("Cannot save schedule", "Fix the validation errors before saving the policy.")
            self.refresh_view()
            return
        write_policy_document(model, self.policy_path)
        self.status_var.set(f"Saved {self.policy_path.name}")
        self.refresh_view()


def launch_planner_gui(*, policy_path_str: str, times_csv_path_str: str) -> None:
    if TKINTER_IMPORT_ERROR is not None:
        raise RuntimeError("Tkinter is not available or could not be imported in this Python environment.") from TKINTER_IMPORT_ERROR
    policy_path = Path(policy_path_str).resolve()
    times_csv_path = Path(times_csv_path_str).resolve()
    try:
        root = tk.Tk()
    except tk.TclError as exc:
        raise RuntimeError("Tkinter GUI could not start. Make sure a graphical display is available.") from exc
    PlannerApp(root, policy_path=policy_path, times_csv_path=times_csv_path)
    root.mainloop()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch the Part 3 schedule planner GUI")
    parser.add_argument(
        "--policy",
        default=str(DEFAULT_POLICY_PATH),
        help="Path to the schedule or policy file (default: %(default)s)",
    )
    parser.add_argument(
        "--times-csv",
        default=str(DEFAULT_TIMES_CSV_PATH),
        help="Path to the Part 2 runtime CSV (default: %(default)s)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        launch_planner_gui(policy_path_str=args.policy, times_csv_path_str=args.times_csv)
    except RuntimeError as exc:
        parser.exit(1, f"{exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
