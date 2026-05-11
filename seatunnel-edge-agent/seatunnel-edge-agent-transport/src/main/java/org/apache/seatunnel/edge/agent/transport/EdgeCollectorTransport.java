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

import java.io.IOException;

/**
 * Collector-facing EdgeSocket transport API intended for wiring from {@code
 * seatunnel-edge-agent-core}.
 *
 * <p>Prefer {@link SeaTunnelEdgeTransportClients#newEdgeTransportClient} with {@link
 * org.apache.seatunnel.engine.client.SeaTunnelClient}, or construct {@link EdgeTransportClient}
 * with {@link JobTaskGroupAddressesLookup} backed by {@code
 * SeaTunnelClient#getJobTaskGroupAddresses(Long)}.
 */
public interface EdgeCollectorTransport extends AutoCloseable {

    /**
     * Refreshes ingress candidates from task-group discovery JSON (see {@link
     * JobTaskGroupAddressesLookup}).
     */
    void discoverEndpoints() throws IOException;

    /**
     * Ensures a session exists (connect + {@link EdgeSocketProtocol} auth). Idempotent when already
     * connected.
     */
    void open();

    /**
     * Sends {@code __BATCH__} then polls {@code __COMMIT__} until {@code ACK:&lt;watermark&gt;}
     * covers {@code batchId}.
     */
    void sendBatchAndAwaitAck(long batchId, String payload)
            throws IOException, InterruptedException;

    /**
     * Lightweight TCP reachability check across discovered endpoints (connect-only; session not
     * kept).
     */
    boolean probeReachable() throws IOException;

    @Override
    void close();
}
