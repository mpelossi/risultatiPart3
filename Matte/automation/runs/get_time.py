from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from automation.timing import build_get_time_report


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("Usage: python3 get_time.py <results.json>")
        return 1

    report = build_get_time_report(Path(args[0]))
    completed_by_name = {job.container_name: job for job in report.completed_jobs}

    for job_name in report.job_names:
        print("Job: ", str(job_name))
        timing = completed_by_name.get(job_name)
        if timing is None:
            print("Job {0} has not completed....".format(job_name))
            return 0
        print("Job time: ", timedelta(seconds=timing.runtime_s))

    if not report.is_complete:
        print("You haven't run all the PARSEC jobs. Exiting...")
        return 0

    print("Total time: {0}".format(report.total_runtime))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
