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

package org.apache.seatunnel.engine.server.rest.filter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Covers the error payload the REST filter writes when a request fails. */
public class ExceptionHandlingFilterTest {

    @Test
    void testErrorResponseKeepsTheMessageAndDropsTheStackFrames() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        ExceptionHandlingFilter filter = new ExceptionHandlingFilter();
        filter.init(null);
        FilterChain chain =
                (req, resp) -> {
                    throw new IllegalArgumentException("Job id is required");
                };

        filter.doFilter(mock(ServletRequest.class), response, chain);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        String json = body.toString();
        Assertions.assertTrue(json.contains("Job id is required"), json);
        Assertions.assertTrue(json.contains("IllegalArgumentException"), json);
        Assertions.assertTrue(json.contains("\"status\":\"fail\""), json);
        Assertions.assertFalse(json.contains("at org.apache.seatunnel"), json);
    }

    @Test
    void testUnexpectedFailureIsReportedAsAServerErrorWithoutStackFrames() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        ExceptionHandlingFilter filter = new ExceptionHandlingFilter();
        filter.init(null);
        FilterChain chain =
                (req, resp) -> {
                    throw new IllegalStateException("cluster is not ready");
                };

        filter.doFilter(mock(ServletRequest.class), response, chain);

        verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        String json = body.toString();
        Assertions.assertTrue(json.contains("cluster is not ready"), json);
        Assertions.assertFalse(json.contains("at org.apache.seatunnel"), json);
    }
}
