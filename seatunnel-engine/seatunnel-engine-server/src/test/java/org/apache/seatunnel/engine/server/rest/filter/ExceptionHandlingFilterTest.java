/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.rest.filter;

import org.junit.jupiter.api.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExceptionHandlingFilterTest {

    @Test
    void shouldReturnMessageWithoutStackTraceForBadRequest() throws Exception {
        assertErrorResponse(new IllegalArgumentException("jobId is required"), 400);
    }

    @Test
    void shouldReturnMessageWithoutStackTraceForServerError() throws Exception {
        assertErrorResponse(new IllegalStateException("worker is unavailable"), 500);
    }

    private void assertErrorResponse(Exception exception, int expectedStatus) throws Exception {
        ExceptionHandlingFilter filter = new ExceptionHandlingFilter();
        filter.init(null);
        FilterChain chain = mock(FilterChain.class);
        doThrow(exception)
                .when(chain)
                .doFilter(any(ServletRequest.class), any(ServletResponse.class));

        ServletRequest request = mock(ServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        StringWriter output = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(output));

        filter.doFilter(request, response, chain);

        verify(response).setStatus(expectedStatus);
        String body = output.toString();
        assertTrue(body.contains(exception.getMessage()));
        assertFalse(body.contains("\\tat "));
    }
}
