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

package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests how {@link RestUtil} reads the optional {@code params} of a {@code /submit-jobs} entry. */
class RestUtilSubmitJobsParamsTest {

    private static final String JOB =
            "\"env\": {\"job.mode\": \"batch\"},"
                    + " \"source\": [{\"plugin_name\": \"FakeSource\", \"plugin_output\": \"fake\"}],"
                    + " \"sink\": [{\"plugin_name\": \"Console\", \"plugin_input\": [\"fake\"]}]";

    @Test
    void testEntryWithoutParamsGetsTheDefaults() throws IOException {
        Map<String, String> requestParams = requestParamsOf("[{" + JOB + "}]");

        Assertions.assertTrue(requestParams.containsKey(RestConstant.JOB_ID));
        Assertions.assertNull(requestParams.get(RestConstant.JOB_ID));
        Assertions.assertEquals("false", requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT));
    }

    @Test
    void testParamValuesAreKeptVerbatim() throws IOException {
        String[] jobNames = {"sales&ops", "100%_load", "a+b", "x=y", "daily job", "a%2Bb"};
        for (String jobName : jobNames) {
            Map<String, String> requestParams =
                    requestParamsOf(
                            "[{\"params\": {\"jobName\": \"" + jobName + "\"}, " + JOB + "}]");

            Assertions.assertEquals(
                    jobName, requestParams.get(RestConstant.JOB_NAME), "jobName " + jobName);
        }
    }

    @Test
    void testNonStringParamValuesAreConvertedToString() throws IOException {
        Map<String, String> requestParams =
                requestParamsOf(
                        "[{\"params\": {\"jobId\": 123, \"isStartWithSavePoint\": true}, "
                                + JOB
                                + "}]");

        Assertions.assertEquals("123", requestParams.get(RestConstant.JOB_ID));
        Assertions.assertEquals("true", requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT));
    }

    @Test
    void testStartWithSavePointStillRequiresJobId() throws IOException {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        requestParamsOf(
                                "[{\"params\": {\"isStartWithSavePoint\": \"true\"}, "
                                        + JOB
                                        + "}]"));
    }

    @Test
    void testParamsMustBeAnObject() {
        String[][] cases = {
            {"\"jobName=do-not-echo\"", "string"}, {"[\"do-not-echo\"]", "array"}, {"1", "number"}
        };
        for (String[] params : cases) {
            IllegalArgumentException exception =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () -> requestParamsOf("[{\"params\": " + params[0] + ", " + JOB + "}]"),
                            "params " + params[0]);
            Assertions.assertEquals(
                    "'params' must be a JSON object, but was a JSON " + params[1],
                    exception.getMessage());
        }
    }

    @Test
    void testNestedParamValuesAreRejected() {
        String[][] cases = {
            {"{\"secret\": \"do-not-echo\"}", "object"}, {"[\"do-not-echo\"]", "array"}
        };
        for (String[] nested : cases) {
            IllegalArgumentException exception =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    requestParamsOf(
                                            "[{\"params\": {\"jobName\": "
                                                    + nested[0]
                                                    + "}, "
                                                    + JOB
                                                    + "}]"),
                            "jobName " + nested[0]);
            Assertions.assertEquals(
                    "'params.jobName' must be a string, number or boolean, but was a JSON "
                            + nested[1],
                    exception.getMessage());
        }
    }

    @Test
    void testParamsAreNotPartOfTheJobConfig() throws IOException {
        List<Tuple2<Map<String, String>, Config>> configs =
                RestUtil.buildConfigList(
                        RestUtil.convertByteToJsonNode(
                                ("[{\"params\": {\"jobName\": \"x\"}, " + JOB + "}]")
                                        .getBytes(StandardCharsets.UTF_8)));

        Assertions.assertEquals(1, configs.size());
        Assertions.assertFalse(configs.get(0)._2.hasPath("params"));
        Assertions.assertTrue(configs.get(0)._2.hasPath("env"));
    }

    private static Map<String, String> requestParamsOf(String body) throws IOException {
        List<Tuple2<Map<String, String>, Config>> configs =
                RestUtil.buildConfigList(
                        RestUtil.convertByteToJsonNode(body.getBytes(StandardCharsets.UTF_8)));
        Assertions.assertEquals(1, configs.size());
        Map<String, String> requestParams = new HashMap<>();
        RestUtil.buildRequestParams(requestParams, configs.get(0)._1);
        return requestParams;
    }
}
