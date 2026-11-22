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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NestedRowAccountingTest extends AbstractSeaTunnelServerTest<NestedRowAccountingTest> {

    @Test
    void testNullableMapArraysReachSinkWithCorrectByteMetrics() {
        long jobId = System.currentTimeMillis();
        startJob(jobId, "batch_nested_rows_to_console.conf", false);

        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertEquals(
                                    JobStatus.FINISHED,
                                    server.getCoordinatorService().getJobStatus(jobId));
                            JobMetrics metrics =
                                    server.getCoordinatorService().getJobMetrics(jobId);
                            assertEquals(3L, metrics.get(SOURCE_RECEIVED_COUNT).get(0).value());
                            assertEquals(3L, metrics.get(SINK_WRITE_COUNT).get(0).value());
                            assertEquals(17L, metrics.get(SOURCE_RECEIVED_BYTES).get(0).value());
                            assertEquals(17L, metrics.get(SINK_WRITE_BYTES).get(0).value());
                        });
    }
}
