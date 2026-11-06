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

"""Regression coverage for complete CI timing reports, including unfinished matrix jobs."""

import io
import json
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from urllib.error import URLError

from ci_duration_analysis import fetch_jobs_from_api, main, parse_jobs_from_data, print_job_summary


class CiDurationAnalysisTest(unittest.TestCase):
    """Protect pagination completeness and the distinction between zero and unknown duration."""

    @staticmethod
    def job(job_id, **changes):
        job = {"id": job_id, "name": f"job-{job_id}", "status": "completed",
               "conclusion": "success", "started_at": "2026-01-01T00:00:00Z",
               "completed_at": "2026-01-01T00:12:30Z", "steps": []}
        job.update(changes)
        return job

    @staticmethod
    def response(jobs, total_count, link=None):
        response = io.StringIO(json.dumps({"jobs": jobs, "total_count": total_count}))
        response.headers = {"Link": link}
        return response

    def test_every_matrix_job_is_included_across_pages(self):
        url = "https://api.github.com/repos/apache/seatunnel/actions/runs/1/jobs?per_page=100&page=1"
        next_url = url.rsplit("page=", 1)[0] + "page=2"
        pages = [self.response([self.job(index) for index in range(100)], 101,
                               f'<{next_url}>; rel="next"'),
                 self.response([self.job(100)], 101)]
        with patch("ci_duration_analysis.urlopen", side_effect=pages) as fetch:
            jobs = fetch_jobs_from_api(url)
        self.assertEqual(list(range(101)), [job["job_id"] for job in jobs])
        self.assertEqual({750.0}, {job["total_duration_seconds"] for job in jobs})
        self.assertEqual(next_url, fetch.call_args_list[1].args[0].full_url)
        self.assertEqual(30, fetch.call_args_list[0].kwargs["timeout"])

    def test_queued_running_jobs_and_steps_keep_unknown_duration(self):
        queued = self.job(1, status="queued", conclusion=None, started_at=None, completed_at=None)
        running = self.job(2, status="in_progress", conclusion=None, completed_at=None,
                           steps=[{"name": "running", "number": 1, "status": "in_progress",
                                   "started_at": "2026-01-01T00:00:00Z", "completed_at": None}])
        complete = self.job(3, steps=[{"name": "pending", "number": 1,
                                     "started_at": None, "completed_at": None}])
        jobs = parse_jobs_from_data({"jobs": [queued, running, complete]})
        self.assertEqual([2, 3, 1], [job["job_id"] for job in jobs])
        self.assertEqual([None, 750.0, None], [job["total_duration_seconds"] for job in jobs])
        self.assertEqual(None, jobs[0]["steps"][0]["duration_seconds"])
        self.assertEqual(None, jobs[1]["steps"][0]["duration_seconds"])
        output = io.StringIO()
        with redirect_stdout(output):
            print_job_summary(jobs)
        self.assertIn("Jobs without completed timing: 2", output.getvalue())
        self.assertIn("job-3", output.getvalue())
        self.assertIn("pending", output.getvalue())

    def test_missing_page_is_an_explicit_failure(self):
        with patch("ci_duration_analysis.urlopen", return_value=self.response([self.job(1)], 2)):
            with self.assertRaisesRegex(ValueError, "Incomplete workflow-job response: 1 of 2"):
                fetch_jobs_from_api("https://api.github.com/jobs")

    def test_duplicate_page_items_cannot_hide_missing_jobs(self):
        pages = [self.response([self.job(1), self.job(2)], 3,
                               '<https://api.github.com/jobs?page=2>; rel="next"'),
                 self.response([self.job(2)], 3)]
        with patch("ci_duration_analysis.urlopen", side_effect=pages):
            with self.assertRaisesRegex(ValueError, "Incomplete workflow-job response: 2 of 3"):
                fetch_jobs_from_api("https://api.github.com/jobs?page=1")

    def test_repeated_or_foreign_next_links_fail_before_following_them(self):
        url = "https://api.github.com/jobs"
        for next_url, message in [(url, "Repeated"), ("https://other.example/jobs", "outside")]:
            with self.subTest(next_url=next_url):
                page = self.response([self.job(1)], 2, f'<{next_url}>; rel="next"')
                with patch("ci_duration_analysis.urlopen", return_value=page):
                    with self.assertRaisesRegex(ValueError, message):
                        fetch_jobs_from_api(url)

    def test_page_budget_failure_never_returns_a_truncated_report(self):
        page = self.response([self.job(1)], 2,
                             '<https://api.github.com/jobs?page=2>; rel="next"')
        with patch("ci_duration_analysis.urlopen", return_value=page):
            with self.assertRaisesRegex(ValueError, "page limit"):
                fetch_jobs_from_api("https://api.github.com/jobs?page=1", max_pages=1)

    def test_expected_bucket_boundaries_are_monotonic(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        samples = [(0, 0), (9, 20), (10, 20), (19, 20), (20, 20),
                   (29, 40), (30, 40), (40, 40)]
        for minutes, expected in samples:
            with self.subTest(minutes=minutes):
                end = (start + timedelta(minutes=minutes)).isoformat()
                jobs = parse_jobs_from_data({"jobs": [self.job(1, completed_at=end)]})
                output = io.StringIO()
                with redirect_stdout(output):
                    print_job_summary(jobs)
                row = next(line for line in output.getvalue().splitlines()
                           if line.startswith("| 1 | job-1 |"))
                self.assertEqual(str(expected), row.split("|")[-2].strip())

    def test_expected_uses_seconds_before_display_minute_rounding(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        for seconds, display_minutes, expected in [
                (0.01, 0.0, 20), (1199.99, 20.0, 20),
                (1200, 20.0, 20), (1200.01, 20.0, 40)]:
            with self.subTest(seconds=seconds):
                end = (start + timedelta(seconds=seconds)).isoformat()
                jobs = parse_jobs_from_data({"jobs": [self.job(1, completed_at=end)]})
                self.assertEqual(seconds, jobs[0]["total_duration_seconds"])
                self.assertEqual(display_minutes, jobs[0]["total_duration_minutes"])
                output = io.StringIO()
                with redirect_stdout(output):
                    print_job_summary(jobs)
                row = next(line for line in output.getvalue().splitlines()
                           if line.startswith("| 1 | job-1 |"))
                self.assertEqual(str(expected), row.split("|")[-2].strip())

    def test_parser_preserves_zero_invalid_timing_and_success_only_reporting(self):
        jobs = parse_jobs_from_data({"jobs": [
            self.job(1, started_at="invalid", completed_at=None),
            self.job(2, completed_at="2026-01-01T00:00:00Z"),
            self.job(3, conclusion="failure"),
            self.job(4, completed_at="2025-12-31T23:59:59Z"),
            self.job(5, started_at="2026-01-01T01:00:00+01:00",
                     completed_at="2026-01-01T00:00:01.234567Z"),
        ]})
        by_id = {job["job_id"]: job for job in jobs}
        self.assertIsNone(by_id[1]["started_at"])
        self.assertIsNone(by_id[1]["total_duration_seconds"])
        self.assertEqual(0.0, by_id[2]["total_duration_seconds"])
        self.assertIsNone(by_id[4]["total_duration_seconds"])
        self.assertEqual(1.23, by_id[5]["total_duration_seconds"])
        self.assertEqual(0.02, by_id[5]["total_duration_minutes"])
        output = io.StringIO()
        with redirect_stdout(output):
            print_job_summary(jobs)
        summary = output.getvalue()
        self.assertIn("Jobs without completed timing: 2", summary)
        self.assertNotIn("job-3", summary)
        unknown = next(line for line in summary.splitlines() if "| job-1 |" in line)
        self.assertEqual(["", "", ""], [cell.strip() for cell in unknown.split("|")[-4:-1]])
        zero = next(line for line in summary.splitlines() if "| job-2 |" in line)
        self.assertEqual(["0.0", "0.0", "0"], [cell.strip() for cell in zero.split("|")[-4:-1]])

    def test_cli_authentication_comes_only_from_environment(self):
        for token in [None, "test-only-environment-credential"]:
            with self.subTest(authenticated=token is not None):
                environment = {} if token is None else {"GITHUB_TOKEN": token}
                output, errors = io.StringIO(), io.StringIO()
                with patch.dict(os.environ, environment, clear=True), \
                        patch("ci_duration_analysis.urlopen", return_value=self.response([], 0)) as fetch, \
                        redirect_stdout(output), redirect_stderr(errors):
                    result = main(["apache", "42"])
                self.assertEqual(0, result)
                request = fetch.call_args.args[0]
                self.assertEqual("https://api.github.com/repos/apache/seatunnel/actions/runs/42/jobs?page=1&per_page=100",
                                 request.full_url)
                self.assertEqual(None if token is None else "Bearer " + token,
                                 request.get_header("Authorization"))
                self.assertIn("A total of 0 tasks were parsed", output.getvalue())
                self.assertEqual("", errors.getvalue())
                if token:
                    self.assertNotIn(token, output.getvalue())

    def test_cli_rejects_legacy_token_argument_without_echoing_it(self):
        token = "test-only-positional-credential"
        output, errors = io.StringIO(), io.StringIO()
        with patch("ci_duration_analysis.urlopen") as fetch, \
                redirect_stdout(output), redirect_stderr(errors):
            result = main(["apache", "42", token])
        self.assertEqual(1, result)
        fetch.assert_not_called()
        self.assertIn("GITHUB_TOKEN", errors.getvalue())
        self.assertNotIn(token, output.getvalue() + errors.getvalue())
        self.assertNotIn("[token]", errors.getvalue())

    def test_cli_request_errors_redact_environment_credential(self):
        token = "test-only-failing-credential"
        output, errors = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, {"GITHUB_TOKEN": token}, clear=True), \
                patch("ci_duration_analysis.urlopen", side_effect=URLError("request rejected " + token)), \
                redirect_stdout(output), redirect_stderr(errors):
            result = main(["apache", "42"])
        self.assertEqual(1, result)
        self.assertIn("request rejected [REDACTED]", errors.getvalue())
        self.assertNotIn(token, output.getvalue() + errors.getvalue())
