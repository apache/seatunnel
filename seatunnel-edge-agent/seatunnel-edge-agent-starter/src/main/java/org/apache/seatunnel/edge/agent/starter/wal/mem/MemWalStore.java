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

package org.apache.seatunnel.edge.agent.starter.wal.mem;

import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePositionStore;
import org.apache.seatunnel.edge.agent.starter.wal.WalRecord;
import org.apache.seatunnel.edge.agent.starter.wal.WalRecordStatus;
import org.apache.seatunnel.edge.agent.starter.wal.WalStore;

import java.util.ArrayList;
import java.util.List;

/** In-memory {@link WalStore} for the {@code NON} delivery guarantee. */
public class MemWalStore implements WalStore {

    private final MemSourcePositionStore sourcePositionStore = new MemSourcePositionStore();
    private final Object lock = new Object();
    private final List<WalRecord> pending = new ArrayList<>();
    private long nextId = 1;

    @Override
    public EdgeSourcePositionStore sourcePositionStore() {
        return sourcePositionStore;
    }

    @Override
    public long append(EdgeEvent event) {
        synchronized (lock) {
            long id = nextId++;
            pending.add(
                    WalRecord.builder()
                            .id(id)
                            .batchId(id)
                            .sourceId(event.getSourceId())
                            .payload(event.getPayload())
                            .eventTime(event.getEventTime())
                            .status(WalRecordStatus.PENDING)
                            .build());
            return id;
        }
    }

    @Override
    public List<WalRecord> claimPending(int maxRecords, int maxAttempts) {
        synchronized (lock) {
            List<WalRecord> claimed = new ArrayList<>(pending);
            pending.clear();
            return claimed;
        }
    }

    @Override
    public int markExceededAsDead(int maxAttempts, int maxRecords) {
        return 0;
    }

    @Override
    public void ack(long recordId) {}

    @Override
    public int resurrectSending(int maxRecords) {
        return 0;
    }

    @Override
    public int resurrectSending(int maxRecords, long staleThresholdMs) {
        return 0;
    }

    @Override
    public int cleanupAcked(long retentionMs, int maxRecords) {
        return 0;
    }
}
