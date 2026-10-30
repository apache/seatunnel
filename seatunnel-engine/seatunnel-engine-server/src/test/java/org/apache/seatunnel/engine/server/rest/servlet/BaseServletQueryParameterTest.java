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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BaseServletQueryParameterTest {

    private final BaseServlet servlet = new BaseServlet(null);

    @Test
    void testNoQueryString() {
        Assertions.assertTrue(parse(null).isEmpty());
        Assertions.assertTrue(parse("").isEmpty());
    }

    @Test
    void testDecodesNamesAndValues() {
        Map<String, String> expected = new HashMap<>();
        expected.put("format", "hocon");
        expected.put("jobName", "sales&ops 100%");
        expected.put("job id", "a+b=c");

        Assertions.assertEquals(
                expected, parse("format=hocon&jobName=sales%26ops+100%25&job+id=a%2Bb%3Dc"));
    }

    @Test
    void testKeysWithoutValueAndEmptyPairs() {
        Map<String, String> expected = new HashMap<>();
        expected.put("a", "");
        expected.put("b", "");
        expected.put("", "x");

        Assertions.assertEquals(expected, parse("a&&b=&=x&"));
    }

    @Test
    void testLastValueWinsForRepeatedNames() {
        Assertions.assertEquals("2", parse("jobId=1&jobId=2").get("jobId"));
    }

    @Test
    void testInvalidEncodingIsRejected() {
        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> parse("jobName=%zz"));
        Assertions.assertTrue(exception.getMessage().contains("%zz"));
    }

    @Test
    void testDoesNotReadTheRequestParametersOrBody() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getQueryString()).thenReturn("format=hocon");

        servlet.getQueryParameterMap(request);

        // getParameterMap() would parse and consume a form-encoded body
        verify(request, never()).getParameterMap();
        verify(request, never()).getParameter(anyString());
    }

    private Map<String, String> parse(String queryString) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getQueryString()).thenReturn(queryString);
        return servlet.getQueryParameterMap(request);
    }
}
