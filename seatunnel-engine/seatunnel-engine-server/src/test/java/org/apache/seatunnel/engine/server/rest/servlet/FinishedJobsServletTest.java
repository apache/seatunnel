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

package org.apache.seatunnel.engine.server.rest.servlet;

import org.apache.seatunnel.engine.server.rest.service.JobInfoService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers the servlet side of finished-job pagination: parameter validation, the out-of-range
 * boundary, and the routing between the paged and unpaged service calls.
 */
class FinishedJobsServletTest {

    private JobInfoService jobInfoService;
    private FinishedJobsServlet servlet;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private StringWriter output;

    @BeforeEach
    void setUp() throws Exception {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        jobInfoService = mock(JobInfoService.class);
        servlet = new FinishedJobsServlet(nodeEngine, jobInfoService);

        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);
        output = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(output));
        when(request.getPathInfo()).thenReturn(null);
    }

    private void withParams(String... keyValuePairs) {
        Map<String, String[]> params = new HashMap<>();
        for (int index = 0; index < keyValuePairs.length; index += 2) {
            params.put(keyValuePairs[index], new String[] {keyValuePairs[index + 1]});
        }
        when(request.getParameterMap()).thenReturn(params);
    }

    /** Without a page parameter the caller still gets the whole listing as a bare array. */
    @Test
    void shouldReturnBareArrayWhenNoPageRequested() throws Exception {
        withParams();
        when(jobInfoService.getJobsByStateJson("")).thenReturn(new JsonArray().add("job"));

        servlet.doGet(request, response);

        Assertions.assertEquals("[\"job\"]", output.toString());
        verify(jobInfoService, never()).getJobsByStateJson(anyString(), anyInt(), anyInt());
    }

    /** With a page parameter the servlet must use the slicing overload and the envelope. */
    @Test
    void shouldUsePagedOverloadAndWriteEnvelopeWhenPageRequested() throws Exception {
        withParams("page", "1", "rows", "2");
        when(jobInfoService.getJobsByStateJson("", 0, 2))
                .thenReturn(page(new JsonArray().add("a").add("b"), 7));

        servlet.doGet(request, response);

        JsonObject written = Json.parse(output.toString()).asObject();
        Assertions.assertEquals(2, written.get("data").asArray().size());
        Assertions.assertEquals(7, written.get("total").asInt());
        verify(jobInfoService, never()).getJobsByStateJson(anyString());
    }

    /** A page starting exactly at total is allowed and yields an empty page, matching legacy. */
    @Test
    void shouldReturnEmptyPageWhenStartEqualsTotal() throws Exception {
        withParams("page", "3", "rows", "5");
        when(jobInfoService.getJobsByStateJson("", 10, 5)).thenReturn(page(new JsonArray(), 10));

        servlet.doGet(request, response);

        JsonObject written = Json.parse(output.toString()).asObject();
        Assertions.assertEquals(0, written.get("data").asArray().size());
        Assertions.assertEquals(10, written.get("total").asInt());
    }

    @Test
    void shouldRejectPageStartingBeyondTotal() throws Exception {
        withParams("page", "4", "rows", "5");
        when(jobInfoService.getJobsByStateJson("", 15, 5)).thenReturn(page(new JsonArray(), 10));

        Assertions.assertEquals("Page number exceeds total pages", assertRejected().getMessage());
    }

    @Test
    void shouldRejectZeroRows() {
        withParams("page", "1", "rows", "0");

        Assertions.assertEquals(
                "Parameter 'rows' must be greater than 0", assertRejected().getMessage());
    }

    @Test
    void shouldRejectNegativeRows() {
        withParams("page", "1", "rows", "-5");

        Assertions.assertEquals(
                "Parameter 'rows' must be greater than 0", assertRejected().getMessage());
    }

    @Test
    void shouldRejectNonPositivePage() {
        withParams("page", "0");

        Assertions.assertEquals(
                "Parameter 'page' must be greater than 0", assertRejected().getMessage());
    }

    @Test
    void shouldRejectNonNumericInputWithAMessageNamingTheParameter() {
        withParams("page", "abc");

        Assertions.assertEquals(
                "Parameter 'page' must be an integer, but was: abc", assertRejected().getMessage());
    }

    /**
     * The offset is computed in long arithmetic, so a page and row count whose product overflows an
     * int is rejected rather than wrapping to a small positive offset and serving the wrong page.
     */
    @Test
    void shouldRejectOffsetThatOverflowsAnInt() throws Exception {
        withParams("page", String.valueOf(Integer.MAX_VALUE), "rows", "10");

        Assertions.assertEquals("Page number exceeds total pages", assertRejected().getMessage());
        verify(jobInfoService, never()).getJobsByStateJson(anyString(), anyInt(), anyInt());
    }

    private IllegalArgumentException assertRejected() {
        return Assertions.assertThrows(
                IllegalArgumentException.class, () -> servlet.doGet(request, response));
    }

    private JobInfoService.JobPage page(JsonArray data, int total) {
        return new JobInfoService.JobPage(data, total);
    }
}
