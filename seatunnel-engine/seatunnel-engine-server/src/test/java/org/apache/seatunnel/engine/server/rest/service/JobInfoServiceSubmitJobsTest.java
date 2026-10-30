/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Runs {@link JobInfoService#submitJobs(byte[])} end to end up to the job submission, which is
 * recorded instead of reaching a cluster, to pin how the {@code params} of each entry become the
 * request parameters of its job.
 */
class JobInfoServiceSubmitJobsTest {

    private static final String JOB =
            "\"env\": {\"job.mode\": \"batch\"},"
                    + " \"source\": [{\"plugin_name\": \"FakeSource\", \"plugin_output\": \"fake\"}],"
                    + " \"sink\": [{\"plugin_name\": \"Console\", \"plugin_input\": [\"fake\"]}]";

    @Test
    void testEntriesWithAndWithoutParamsAreSubmitted() {
        RecordingJobInfoService service = new RecordingJobInfoService();

        JsonArray result =
                service.submitJobs(
                        bytes(
                                "[{"
                                        + JOB
                                        + "}, {\"params\": {\"jobName\": \"sales&ops%20daily\"}, "
                                        + JOB
                                        + "}]"));

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(2, service.submittedParams.size());

        Map<String, String> withoutParams = service.submittedParams.get(0);
        Assertions.assertTrue(withoutParams.containsKey(RestConstant.JOB_ID));
        Assertions.assertNull(withoutParams.get(RestConstant.JOB_ID));
        Assertions.assertEquals("false", withoutParams.get(RestConstant.IS_START_WITH_SAVE_POINT));
        Assertions.assertFalse(withoutParams.containsKey(RestConstant.JOB_NAME));

        Assertions.assertEquals(
                "sales&ops%20daily", service.submittedParams.get(1).get(RestConstant.JOB_NAME));
    }

    @Test
    void testSavepointWithoutJobIdIsRejectedBeforeSubmission() {
        RecordingJobInfoService service = new RecordingJobInfoService();

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        service.submitJobs(
                                bytes(
                                        "[{\"params\": {\"isStartWithSavePoint\": \"true\"}, "
                                                + JOB
                                                + "}]")));
        Assertions.assertTrue(service.submittedParams.isEmpty());
    }

    @Test
    void testParamsThatIsNotAnObjectIsRejectedBeforeSubmission() {
        RecordingJobInfoService service = new RecordingJobInfoService();

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> service.submitJobs(bytes("[{\"params\": \"x\", " + JOB + "}]")));
        Assertions.assertTrue(exception.getMessage().contains("'params' must be a JSON object"));
        Assertions.assertTrue(service.submittedParams.isEmpty());
    }

    @Test
    void testInvalidLaterEntryRejectsTheWholeRequest() {
        // Only the second entry is invalid; the first one must not be submitted either
        String[] invalidParams = {
            "{\"isStartWithSavePoint\": \"true\"}",
            "{\"restoreMode\": \"BOGUS\"}",
            "{\"restoreMode\": \"SAVEPOINT\"}",
            "{\"jobName\": {\"a\": 1}}",
            "{\"jobId\": 1.5}",
            "{\"jobId\": \"abc\"}",
            "{\"restoreMode\": \"CHECKPOINT\", \"restoreSourceJobId\": \"abc\"}",
            "{\"restoreMode\": \"CHECKPOINT\", \"restoreSourceJobId\": \"\"}",
            "{\"restoreMode\": \"SAVEPOINT\", \"restoreSourceJobId\": \" \"}"
        };
        for (String params : invalidParams) {
            RecordingJobInfoService service = new RecordingJobInfoService();

            Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            service.submitJobs(
                                    bytes(
                                            "[{\"params\": {\"jobName\": \"first\"}, "
                                                    + JOB
                                                    + "}, {\"params\": "
                                                    + params
                                                    + ", "
                                                    + JOB
                                                    + "}]")),
                    "second entry params " + params);
            Assertions.assertTrue(service.submittedParams.isEmpty(), "params " + params);
        }
    }

    @Test
    void testStringJobIdsMustBeLongs() {
        for (String key : new String[] {"jobId", "restoreSourceJobId"}) {
            RecordingJobInfoService service = new RecordingJobInfoService();
            String restoreMode =
                    "restoreSourceJobId".equals(key) ? "\"restoreMode\": \"CHECKPOINT\", " : "";

            IllegalArgumentException exception =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    service.submitJobs(
                                            bytes(
                                                    "[{\"params\": {"
                                                            + restoreMode
                                                            + "\""
                                                            + key
                                                            + "\": \"12x\"}, "
                                                            + JOB
                                                            + "}]")),
                            key);
            Assertions.assertEquals("'" + key + "' must be a long integer", exception.getMessage());
            Assertions.assertTrue(service.submittedParams.isEmpty(), key);
        }

        RecordingJobInfoService service = new RecordingJobInfoService();
        service.submitJobs(bytes("[{\"params\": {\"jobId\": \"123\"}, " + JOB + "}]"));
        Assertions.assertEquals("123", service.submittedParams.get(0).get(RestConstant.JOB_ID));
    }

    private static byte[] bytes(String body) {
        return body.getBytes(StandardCharsets.UTF_8);
    }

    /** Records the request parameters of every job instead of submitting it. */
    private static final class RecordingJobInfoService extends JobInfoService {

        private final List<Map<String, String>> submittedParams = new ArrayList<>();

        private RecordingJobInfoService() {
            super(mock(NodeEngineImpl.class));
        }

        @Override
        protected SeaTunnelServer getSeaTunnelServer(boolean shouldBeMaster) {
            return null;
        }

        @Override
        protected JsonObject submitJobInternal(
                Config config,
                Map<String, String> requestParams,
                SeaTunnelServer seaTunnelServer,
                Node node) {
            submittedParams.add(new HashMap<>(requestParams));
            return new JsonObject().add(RestConstant.JOB_NAME, "recorded");
        }
    }
}
