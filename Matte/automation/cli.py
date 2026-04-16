from __future__ import annotations

import argparse
from pathlib import Path

from .audit import audit_schedule, load_runtime_table, load_schedule_model, render_audit_report
from .catalog import JOB_CATALOG
from .cluster import ClusterController
from .collect import collect_live_pods, summarize_run
from .config import load_experiment_config, load_policy_config
from .export import export_submission
from .gui import launch_planner_gui
from .provision import check_client_provisioning
from .results import load_run_summaries, sort_best_runs
from .runner import ExperimentRunner
from .manifests import resolve_jobs


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Part 3 Python orchestrator")
    subparsers = parser.add_subparsers(dest="command", required=True)

    cluster_parser = subparsers.add_parser("cluster")
    cluster_sub = cluster_parser.add_subparsers(dest="cluster_command", required=True)
    cluster_up = cluster_sub.add_parser("up")
    cluster_up.add_argument("--config", required=True)
    cluster_down = cluster_sub.add_parser("down")
    cluster_down.add_argument("--config", required=True)

    provision_parser = subparsers.add_parser("provision")
    provision_sub = provision_parser.add_subparsers(dest="provision_command", required=True)
    provision_check = provision_sub.add_parser("check")
    provision_check.add_argument("--config", required=True)

    run_parser = subparsers.add_parser("run")
    run_sub = run_parser.add_subparsers(dest="run_command", required=True)
    run_once = run_sub.add_parser("once")
    run_once.add_argument("--config", required=True)
    run_once.add_argument("--policy", required=True)
    run_once.add_argument("--dry-run", action="store_true")
    run_batch = run_sub.add_parser("batch")
    run_batch.add_argument("--config", required=True)
    run_batch.add_argument("--policy", required=True)
    run_batch.add_argument("--runs", type=int, default=3)
    run_batch.add_argument("--dry-run", action="store_true")

    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("--config", required=True)
    collect_parser.add_argument("--policy", required=True)
    collect_parser.add_argument("--run-dir", required=True)
    collect_parser.add_argument("--live", action="store_true")

    audit_parser = subparsers.add_parser("audit")
    audit_parser.add_argument("--policy", required=True)
    audit_parser.add_argument("--times-csv", required=True)

    gui_parser = subparsers.add_parser("gui")
    gui_parser.add_argument("--policy", required=True)
    gui_parser.add_argument("--times-csv", required=True)

    show_parser = subparsers.add_parser("show")
    show_parser.add_argument("--policy", required=True)

    results_parser = subparsers.add_parser("results")
    results_sub = results_parser.add_subparsers(dest="results_command", required=True)
    results_best = results_sub.add_parser("best")
    results_best.add_argument("--experiment", required=True)
    results_best.add_argument("--results-root", default="part3/automation/runs")

    export_parser = subparsers.add_parser("export")
    export_sub = export_parser.add_subparsers(dest="export_command", required=True)
    export_submission_parser = export_sub.add_parser("submission")
    export_submission_parser.add_argument("--experiment", required=True)
    export_submission_parser.add_argument("--group", required=True)
    export_submission_parser.add_argument("--task", required=True)
    export_submission_parser.add_argument("--results-root", default="part3/automation/runs")
    export_submission_parser.add_argument("--output-root", default=".")
    export_submission_parser.add_argument("--run-id", action="append", dest="run_ids")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "cluster":
        experiment = load_experiment_config(args.config)
        cluster = ClusterController(experiment)
        if args.cluster_command == "up":
            cluster.cluster_up()
        else:
            cluster.cluster_down()
        return 0

    if args.command == "provision":
        experiment = load_experiment_config(args.config)
        cluster = ClusterController(experiment)
        statuses = check_client_provisioning(cluster)
        for status in statuses.values():
            print(status)
        return 0

    if args.command == "run":
        experiment = load_experiment_config(args.config)
        policy = load_policy_config(args.policy)
        runner = ExperimentRunner(experiment, policy)
        if args.run_command == "once":
            run_dir = runner.run_once(dry_run=args.dry_run)
            print(run_dir)
        else:
            run_dirs = runner.run_batch(args.runs, dry_run=args.dry_run)
            for run_dir in run_dirs:
                print(run_dir)
        return 0

    if args.command == "collect":
        experiment = load_experiment_config(args.config)
        policy = load_policy_config(args.policy)
        run_dir = Path(args.run_dir).resolve()
        if args.live:
            cluster = ClusterController(experiment)
            collect_live_pods(cluster, run_dir)
        summary = summarize_run(
            run_dir,
            experiment_id=experiment.experiment_id,
            policy_name=policy.policy_name,
            run_id=run_dir.name,
            expected_jobs=set(JOB_CATALOG),
        )
        print(summary["overall_status"])
        return 0

    if args.command == "audit":
        model = load_schedule_model(args.policy)
        runtime_table = load_runtime_table(args.times_csv)
        report = audit_schedule(model, runtime_table)
        print(render_audit_report(report))
        return 1 if report.errors else 0

    if args.command == "gui":
        launch_planner_gui(policy_path_str=args.policy, times_csv_path_str=args.times_csv)
        return 0

    if args.command == "show":
        policy = load_policy_config(args.policy)
        jobs = resolve_jobs(policy, "preview")
        print(f"Policy: {policy.policy_name}")
        print(
            "Memcached:",
            f"vm={policy.memcached.node}",
            f"cores={policy.memcached.cores}",
            f"threads={policy.memcached.threads}",
        )
        for phase in policy.phases:
            dependency = phase.after
            if phase.jobs_complete:
                dependency += ":" + ",".join(phase.jobs_complete)
            print(f"{phase.phase_id} after {dependency} delay={phase.delay_s}s")
            for job_id in phase.launch:
                job = jobs[job_id]
                print(
                    "  -",
                    job_id,
                    f"vm={job.node}",
                    f"cores={job.cores}",
                    f"threads={job.threads}",
                )
        return 0

    if args.command == "results" and args.results_command == "best":
        summaries = sort_best_runs(
            load_run_summaries(Path(args.results_root).resolve(), args.experiment)
        )
        if not summaries:
            print("No completed run summaries found.")
            return 0
        for summary in summaries:
            print(
                summary.get("run_id"),
                summary.get("policy_name"),
                summary.get("overall_status"),
                f"makespan={summary.get('makespan_s')}",
                f"max_p95_us={summary.get('max_observed_p95_us')}",
                summary.get("run_dir"),
            )
        return 0

    if args.command == "export" and args.export_command == "submission":
        output_dir = export_submission(
            results_root=Path(args.results_root).resolve(),
            experiment_id=args.experiment,
            group=args.group,
            task=args.task,
            output_root=Path(args.output_root).resolve(),
            selected_run_ids=args.run_ids,
        )
        print(output_dir)
        return 0

    parser.error("Unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
