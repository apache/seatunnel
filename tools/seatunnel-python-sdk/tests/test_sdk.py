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

import unittest
from unittest import mock

import httpx

from seatunnel import SeaTunnelClient, SubmitJobQueryParams
from seatunnel.client import SeaTunnelAPIError, SeaTunnelConnectionError


class RecordingSession:
    def __init__(self, response=None, exc=None):
        self.response = response
        self.exc = exc
        self.calls = []
        self.closed = False

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if self.exc is not None:
            raise self.exc
        return self.response

    def close(self):
        self.closed = True


class SeaTunnelSdkTest(unittest.TestCase):
    def create_client(self, session):
        with mock.patch("seatunnel.client.httpx.Client", return_value=session):
            return SeaTunnelClient(base_url="http://localhost:8080")

    def test_submit_job_sends_body_and_query_params(self):
        response = httpx.Response(
            200,
            json={"jobId": 1},
            request=httpx.Request("POST", "http://localhost:8080/submit-job"),
        )
        session = RecordingSession(response=response)
        sdk = self.create_client(session)

        result = sdk.jobs.submit_job(
            conf="env { job.mode = \"batch\" }",
            params=SubmitJobQueryParams(jobName="demo-job"),
        )

        self.assertEqual({"jobId": 1}, result)
        method, url, kwargs = session.calls[0]
        self.assertEqual("POST", method)
        self.assertEqual("http://localhost:8080/submit-job", url)
        self.assertEqual("env { job.mode = \"batch\" }", kwargs["content"])
        self.assertEqual("demo-job", kwargs["params"]["jobName"])
        self.assertEqual("hocon", kwargs["params"]["format"])

    def test_overview_forwards_filter_query_params(self):
        response = httpx.Response(
            200,
            json={"status": "ok"},
            request=httpx.Request("GET", "http://localhost:8080/overview"),
        )
        session = RecordingSession(response=response)
        sdk = self.create_client(session)

        sdk.cluster.get_overview({"jobId": "12345", "pipelineId": "daily"})

        _, url, kwargs = session.calls[0]
        self.assertEqual("http://localhost:8080/overview", url)
        self.assertEqual({"jobId": "12345", "pipelineId": "daily"}, kwargs["params"])

    def test_get_logs_without_job_id_keeps_base_logs_endpoint(self):
        response = httpx.Response(
            200,
            json={"logs": []},
            request=httpx.Request("GET", "http://localhost:8080/logs"),
        )
        session = RecordingSession(response=response)
        sdk = self.create_client(session)

        sdk.cluster.get_logs()

        _, url, kwargs = session.calls[0]
        self.assertEqual("http://localhost:8080/logs", url)
        self.assertEqual({"format": "json"}, kwargs["params"])

    def test_get_logs_with_job_id_uses_path_segment_contract(self):
        response = httpx.Response(
            200,
            json={"logs": []},
            request=httpx.Request("GET", "http://localhost:8080/logs/733584788375666689"),
        )
        session = RecordingSession(response=response)
        sdk = self.create_client(session)

        sdk.cluster.get_logs(jobId=733584788375666689)

        _, url, kwargs = session.calls[0]
        self.assertEqual("http://localhost:8080/logs/733584788375666689", url)
        self.assertEqual({"format": "json"}, kwargs["params"])

    def test_http_status_error_uses_response_message_and_preserves_cause(self):
        response = httpx.Response(
            400,
            json={"message": "job submission failed"},
            request=httpx.Request("POST", "http://localhost:8080/submit-job"),
        )
        session = RecordingSession(response=response)
        sdk = self.create_client(session)

        with self.assertRaises(SeaTunnelAPIError) as context:
            sdk.jobs.submit_job("env {}", SubmitJobQueryParams())

        self.assertEqual(400, context.exception.status_code)
        self.assertEqual("job submission failed", context.exception.message)
        self.assertIsInstance(context.exception.__cause__, httpx.HTTPStatusError)

    def test_request_error_is_wrapped_as_connection_error(self):
        request = httpx.Request("GET", "http://localhost:8080/overview")
        session = RecordingSession(exc=httpx.RequestError("boom", request=request))
        sdk = self.create_client(session)

        with self.assertRaises(SeaTunnelConnectionError) as context:
            sdk.cluster.get_overview()

        self.assertIn("http://localhost:8080/overview", str(context.exception))
        self.assertIsInstance(context.exception.__cause__, httpx.RequestError)

    def test_context_manager_closes_underlying_session(self):
        response = httpx.Response(
            200,
            text="metric 1",
            headers={"Content-Type": "text/plain"},
            request=httpx.Request("GET", "http://localhost:8080/openmetrics"),
        )
        session = RecordingSession(response=response)
        sdk = self.create_client(session)

        with sdk as client:
            client.cluster.get_metrics()

        self.assertTrue(session.closed)


if __name__ == "__main__":
    unittest.main()
