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

package org.apache.seatunnel.engine.server;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;

class TestUtilsTest {

    @Test
    void getAvailablePortReturnsBindablePort() throws Exception {
        int port = TestUtils.getAvailablePort();

        try (ServerSocket ignored = new ServerSocket(port)) {
            Assertions.assertTrue(port > 0);
        }
    }

    @Test
    void getAvailablePortReturnsNonOverlappingPortRanges() {
        int firstPort = TestUtils.getAvailablePort(10);
        int secondPort = TestUtils.getAvailablePort(10);

        Assertions.assertTrue(
                firstPort + 9 < secondPort || secondPort + 9 < firstPort,
                "Allocated port ranges should not overlap");
    }
}
