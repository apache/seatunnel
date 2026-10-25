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
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from ci_duration_analysis import fetch_jobs_from_api, parse_jobs_from_data, print_job_summary


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
