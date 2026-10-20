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

package org.apache.seatunnel.engine.common.config.spec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SessionSpecificationTest {

    @Test
    void acceptsValidSessionBounds() {
        SessionSpecification specification =
                new SessionSpecification(
                        "seatunnel-session-cluster",
                        "seatunnel-session-cluster",
                        2,
                        32,
                        new WorkerSpecification(2048, 2, 8),
                        true,
                        120_000,
                        3_000);

        Assertions.assertEquals(2, specification.getMinWorkers());
        Assertions.assertEquals(32, specification.getMaxWorkers());
        Assertions.assertTrue(specification.isDynamicWorkerEnabled());
        Assertions.assertEquals(8, specification.getWorkerSpecification().getSlots());
    }

    @Test
    void rejectsMaxBelowMin() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SessionSpecification(
                                "seatunnel-session-cluster",
                                "seatunnel-session-cluster",
                                4,
                                2,
                                new WorkerSpecification(2048, 2, 8),
                                true,
                                120_000,
                                3_000));
    }

    @Test
    void rejectsNonPositiveTimeouts() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SessionSpecification(
                                "seatunnel-session-cluster",
                                "seatunnel-session-cluster",
                                2,
                                4,
                                new WorkerSpecification(2048, 2, 8),
                                true,
                                0,
                                3_000));
    }
}
