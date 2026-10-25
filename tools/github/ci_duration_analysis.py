#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Summarize all workflow jobs without inferring durations for unfinished work."""

import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen


def _next_page(link_header):
    for link, relation in re.findall(r'<([^>]+)>\s*;\s*rel="([^"]+)"', link_header or ""):
        if "next" in relation.split():
            return link
    return None


def fetch_jobs_from_api(api_url, token=None, max_pages=100):
    """Follow bounded, same-origin pagination so matrix jobs are never silently lost."""
    if max_pages <= 0:
        raise ValueError("max_pages must be positive")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    initial = urlsplit(api_url)
    origin = (initial.scheme.lower(), initial.netloc.lower())
    visited = set()
    jobs_by_id = {}
    total_count = 0
    next_url = api_url
    try:
        for _ in range(max_pages):
            current = urlsplit(next_url)
            if (current.scheme.lower(), current.netloc.lower()) != origin:
                raise ValueError("Refusing a pagination link outside the original API origin")
            if next_url in visited:
                raise ValueError("Repeated workflow-job pagination link")
            visited.add(next_url)
            # Bound each page fetch so an API outage cannot stall a timing report indefinitely.
            with urlopen(Request(next_url, headers=headers), timeout=30) as response:
                data = json.load(response)
                for job in data["jobs"]:
                    jobs_by_id[job["id"]] = job
                total_count = max(total_count, data.get("total_count", 0))
                link = _next_page(response.headers.get("Link"))
            if not link:
                break
            next_url = urljoin(next_url, link)
        else:
            raise ValueError("Workflow-job pagination exceeded the configured page limit")
    except URLError as error:
        raise ConnectionError(f"API request failed: {error}") from error
    if len(jobs_by_id) < total_count:
        raise ValueError(f"Incomplete workflow-job response: {len(jobs_by_id)} of {total_count} jobs")
    return parse_jobs_from_data({"jobs": list(jobs_by_id.values())})


def _parse_timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return timestamp.replace(tzinfo=timezone.utc) if timestamp.tzinfo is None else timestamp
    except ValueError:
        return None


def _duration_seconds(start, end):
    if start is None or end is None or end < start:
        return None
    return round((end - start).total_seconds(), 2)


def _parse_single_job(job):
    start = _parse_timestamp(job.get("started_at"))
    duration = _duration_seconds(start, _parse_timestamp(job.get("completed_at")))
    job_info = {
        "job_id": job.get("id"),
        "name": job.get("name"),
        "status": job.get("status"),
        "conclusion": job.get("conclusion"),
        "started_at": start.strftime("%Y-%m-%d %H:%M:%S") if start else None,
        "total_duration_seconds": duration,
        "total_duration_minutes": round(duration / 60, 2) if duration is not None else None,
        "steps": [],
    }
    for step in job.get("steps", []):
        job_info["steps"].append({
            "name": step.get("name"),
            "number": step.get("number"),
            "status": step.get("status"),
            "conclusion": step.get("conclusion"),
            "duration_seconds": _duration_seconds(
                _parse_timestamp(step.get("started_at")),
                _parse_timestamp(step.get("completed_at")),
            ),
        })
    return job_info


def _parse_jobs_batch(data):
    def start_order(job):
        start = _parse_timestamp(job.get("started_at"))
        return (start is None, start.timestamp() if start else 0)

    return [_parse_single_job(job) for job in sorted(data["jobs"], key=start_order)]


def parse_jobs_from_file(file_path):
    with Path(file_path).open("r", encoding="utf-8") as source:
        return _parse_jobs_batch(json.load(source))


def parse_jobs_from_data(data):
    return _parse_jobs_batch(data)


def _format_table(rows, headers):
    """Render the existing Markdown columns using only the Python standard library."""
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    lines.extend("| " + " | ".join("" if value is None else str(value) for value in row) + " |" for row in rows)
    return "\n".join(lines)


def print_job_summary(parsed_jobs):
    success_jobs = sorted(
        (job for job in parsed_jobs if job["conclusion"] == "success"),
        key=lambda job: job["total_duration_seconds"] or 0,
        reverse=True,
    )
    incomplete = sum(job["total_duration_seconds"] is None for job in parsed_jobs)
    print(f"A total of {len(parsed_jobs)} tasks were parsed, among which {len(success_jobs)} tasks were successful (success)")
    print(f"Jobs without completed timing: {incomplete}\n")
    headers = ["Sequence Number", "Job Name", "Job ID", "CONCLUSION", "STARTED_AT",
               "Total Duration (Seconds)", "Total Duration (Minutes)", "Expected"]
    rows = []
    for index, job in enumerate(success_jobs, 1):
        minutes = job["total_duration_minutes"]
        expected = None
        if minutes is not None:
            expected = (math.ceil(minutes / 20) + (1 if minutes % 20 >= 10 else 0)) * 20
        rows.append([index, job["name"], job["job_id"], job["conclusion"], job["started_at"],
                     job["total_duration_seconds"], minutes, expected])
    print("Overview of the Task:")
    print(_format_table(rows, headers))
    for index, job in enumerate(success_jobs, 1):
        print(f"\nTask {index}: The 3 steps with the longest duration for {job['name']}:")
        steps = sorted(job["steps"], key=lambda step: (
            step["duration_seconds"] is None, -(step["duration_seconds"] or 0)))[:3]
        if steps:
            rows = [[step["name"], step["number"], step["status"], step["conclusion"],
                     step["duration_seconds"]] for step in steps]
            print(_format_table(rows, ["Step name", "No", "Status", "Conclusion", "Total Duration (seconds)"]))
        else:
            print("There is no step information for this task.")
        print("\n" + "-" * 80)


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print("Usage: python ci_duration_analysis.py <owner> <run_id> [token]")
        sys.exit(1)
    owner, run_id = sys.argv[1:3]
    api_url = f"https://api.github.com/repos/{owner}/seatunnel/actions/runs/{run_id}/jobs?page=1&per_page=100"
    token = sys.argv[3] if len(sys.argv) == 4 else None
    try:
        print_job_summary(fetch_jobs_from_api(api_url, token))
    except Exception as error:
        print(f"Processing failed: {error}", file=sys.stderr)
        sys.exit(1)
