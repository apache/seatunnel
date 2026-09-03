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

package org.apache.seatunnel.engine.server.rest.servlet;

import org.apache.seatunnel.engine.server.diagnostic.WorkerResourceSnapshot;
import org.apache.seatunnel.engine.server.rest.service.WorkerResourceService;

import org.junit.jupiter.api.Test;

import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerResourceServletTest {

    @Test
    void shouldWriteWorkerResourceSnapshotAsJson() throws Exception {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        WorkerResourceService service = mock(WorkerResourceService.class);
        WorkerResourceSnapshot snapshot =
                new WorkerResourceSnapshot(true, 1234L, Collections.emptyList());
        when(service.getWorkerResources()).thenReturn(snapshot);
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        StringWriter output = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(output));

        new WorkerResourceServlet(nodeEngine, service).doGet(request, response);

        assertEquals("{\"available\":true,\"collectedAt\":1234,\"workers\":[]}", output.toString());
        verify(response).setCharacterEncoding("UTF-8");
        verify(response).setContentType("application/json; charset=UTF-8");
    }
}
