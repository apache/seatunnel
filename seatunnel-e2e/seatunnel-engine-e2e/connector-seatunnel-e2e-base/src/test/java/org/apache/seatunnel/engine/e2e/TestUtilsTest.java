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

package org.apache.seatunnel.engine.e2e;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;

class TestUtilsTest {

    @Test
    void allocatesBindablePort() throws Exception {
        int port = TestUtils.getAvailablePort(1);

        try (ServerSocket ignored = new ServerSocket(port)) {
            Assertions.assertEquals(port, ignored.getLocalPort());
        }
    }

    @Test
    void allocatesNonOverlappingPortRanges() {
        int firstPort = TestUtils.getAvailablePort(10);
        int secondPort = TestUtils.getAvailablePort(10);

        Assertions.assertTrue(firstPort + 9 < secondPort || secondPort + 9 < firstPort);
    }

    @Test
    void rejectsInvalidPortRangeSize() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> TestUtils.getAvailablePort(0));
    }
}
