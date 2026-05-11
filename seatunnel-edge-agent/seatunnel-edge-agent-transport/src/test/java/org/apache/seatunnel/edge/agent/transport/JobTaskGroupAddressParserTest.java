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

package org.apache.seatunnel.edge.agent.transport;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class JobTaskGroupAddressParserTest {

    @Test
    void parseDistinctHostsNullJsonFails() {
        Assertions.assertThrows(
                NullPointerException.class,
                () -> JobTaskGroupAddressParser.parseDistinctHosts(null));
    }

    @Test
    void parseDistinctHostsTrimsHostValues() throws IOException {
        List<String> hosts =
                JobTaskGroupAddressParser.parseDistinctHosts("[{\"host\":\"  edge-1  \"}]");
        Assertions.assertEquals(Collections.singletonList("edge-1"), hosts);
    }

    @Test
    void parseDistinctHostsEmptyArrayYieldsEmptyList() throws IOException {
        Assertions.assertEquals(
                Collections.emptyList(), JobTaskGroupAddressParser.parseDistinctHosts("[]"));
    }

    @Test
    void parseDistinctHostsDedupesPreservesOrder() throws IOException {
        String json =
                "["
                        + "{\"jobId\":1,\"pipelineId\":0,\"taskGroupId\":0,\"host\":\"a.example\",\"port\":5801},"
                        + "{\"jobId\":1,\"pipelineId\":0,\"taskGroupId\":1,\"host\":\"b.example\",\"port\":5801},"
                        + "{\"jobId\":1,\"pipelineId\":0,\"taskGroupId\":2,\"host\":\"a.example\",\"port\":5801}"
                        + "]";
        List<String> hosts = JobTaskGroupAddressParser.parseDistinctHosts(json);
        Assertions.assertEquals(Arrays.asList("a.example", "b.example"), hosts);
    }

    @Test
    void parseDistinctHostsSkipsBadEntries() throws IOException {
        String json = "[{\"host\":\"ok\"},{},{\"host\":\"\"}]";
        List<String> hosts = JobTaskGroupAddressParser.parseDistinctHosts(json);
        Assertions.assertEquals(Arrays.asList("ok"), hosts);
    }

    @Test
    void parseDistinctHostsRejectsNonArray() {
        Assertions.assertThrows(
                IOException.class, () -> JobTaskGroupAddressParser.parseDistinctHosts("{}"));
    }
}
