#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Regression tests for the BedrockProvider temperature-rejection retry
(chat and chat_stream paths)."""

import copy
from unittest import mock

import pytest

botocore = pytest.importorskip("botocore")
from botocore.exceptions import ClientError  # noqa: E402

OK_RESPONSE = {
    "output": {"message": {"role": "assistant", "content": [{"text": "OK"}]}},
    "stopReason": "end_turn",
}

OK_STREAM = {"stream": []}


def _temperature_rejection() -> ClientError:
    return ClientError(
        error_response={
            "Error": {
                "Code": "ValidationException",
                "Message": "The model returned the following errors: "
                           "`temperature` is deprecated for this model.",
            }
        },
        operation_name="Converse",
    )


def _other_validation_error() -> ClientError:
    return ClientError(
        error_response={
            "Error": {
                "Code": "ValidationException",
                "Message": "The provided model identifier is invalid.",
            }
        },
        operation_name="Converse",
    )


def _throttling_error() -> ClientError:
    return ClientError(
        error_response={
            "Error": {"Code": "ThrottlingException",
                      "Message": "Too many requests, temperature unrelated"}
        },
        operation_name="Converse",
    )


def _make_provider(converse_effects=None, stream_effects=None):
    """Build a BedrockProvider with a stubbed boto3 client.

    The retry path mutates the kwargs dict in place before the second call,
    so mock's recorded call_args (which stores references) can't be used to
    inspect the first call's inferenceConfig. Record deep snapshots instead.
    """
    with mock.patch.dict("sys.modules", {"boto3": mock.MagicMock()}):
        from seatunnel_cli.llm_provider import BedrockProvider
        provider = BedrockProvider.__new__(BedrockProvider)
        provider._model_id = "test-model"
        provider._fast_model_id = "test-model"
        provider._no_temperature_models = set()
        provider.client = mock.MagicMock()
        provider.converse_snapshots = []
        provider.stream_snapshots = []

        def _run(effects, snapshots):
            def call(**kwargs):
                snapshots.append(copy.deepcopy(kwargs))
                effect = effects.pop(0)
                if isinstance(effect, Exception):
                    raise effect
                return effect
            return call

        if converse_effects is not None:
            provider.client.converse.side_effect = _run(
                list(converse_effects), provider.converse_snapshots)
        if stream_effects is not None:
            provider.client.converse_stream.side_effect = _run(
                list(stream_effects), provider.stream_snapshots)
        return provider


# ── chat (non-streaming) ──

def test_chat_temperature_rejection_retries_without_temperature():
    provider = _make_provider(
        converse_effects=[_temperature_rejection(), OK_RESPONSE, OK_RESPONSE])

    resp = provider.chat([{"role": "user", "content": [{"text": "hi"}]}])
    assert resp["stopReason"] == "end_turn"

    snaps = provider.converse_snapshots
    assert len(snaps) == 2
    assert "temperature" in snaps[0]["inferenceConfig"]
    assert "temperature" not in snaps[1]["inferenceConfig"]
    assert "test-model" in provider._no_temperature_models

    # model id cached: the next call must not send temperature at all
    provider.chat([{"role": "user", "content": [{"text": "hi"}]}])
    assert "temperature" not in provider.converse_snapshots[2]["inferenceConfig"]


def test_chat_other_validation_errors_are_not_swallowed():
    provider = _make_provider(converse_effects=[_other_validation_error()])
    with pytest.raises(ClientError):
        provider.chat([{"role": "user", "content": [{"text": "hi"}]}])
    assert len(provider.converse_snapshots) == 1
    assert not provider._no_temperature_models


def test_chat_non_validation_client_errors_are_not_swallowed():
    provider = _make_provider(converse_effects=[_throttling_error()])
    with pytest.raises(ClientError):
        provider.chat([{"role": "user", "content": [{"text": "hi"}]}])
    assert len(provider.converse_snapshots) == 1
    assert not provider._no_temperature_models


def test_chat_non_client_errors_propagate():
    provider = _make_provider(converse_effects=[RuntimeError("boom")])
    with pytest.raises(RuntimeError):
        provider.chat([{"role": "user", "content": [{"text": "hi"}]}])
    assert not provider._no_temperature_models


# ── chat_stream (the CLI's interactive path — scenario from #11508) ──

def test_chat_stream_temperature_rejection_retries_without_temperature():
    provider = _make_provider(
        stream_effects=[_temperature_rejection(), OK_STREAM, OK_STREAM])

    events = list(provider.chat_stream(
        [{"role": "user", "content": [{"text": "hi"}]}]))
    assert events == []  # empty stream consumed without error

    snaps = provider.stream_snapshots
    assert len(snaps) == 2
    assert "temperature" in snaps[0]["inferenceConfig"]
    assert "temperature" not in snaps[1]["inferenceConfig"]
    assert "test-model" in provider._no_temperature_models

    # cached: next stream call must not send temperature
    list(provider.chat_stream([{"role": "user", "content": [{"text": "hi"}]}]))
    assert "temperature" not in provider.stream_snapshots[2]["inferenceConfig"]


def test_chat_stream_other_errors_are_not_swallowed():
    provider = _make_provider(stream_effects=[_throttling_error()])
    with pytest.raises(ClientError):
        list(provider.chat_stream(
            [{"role": "user", "content": [{"text": "hi"}]}]))
    assert len(provider.stream_snapshots) == 1
    assert not provider._no_temperature_models
