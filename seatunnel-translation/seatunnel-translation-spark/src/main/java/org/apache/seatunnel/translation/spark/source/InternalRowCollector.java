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

package org.apache.seatunnel.translation.spark.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.state.CheckpointLock;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.serialization.InternalRowSerialization;

import org.apache.spark.sql.catalyst.InternalRow;

public class InternalRowCollector implements Collector<SeaTunnelRow> {
    private final Handover<InternalRow> handover;
    private final CheckpointLock checkpointLock;
    private final InternalRowSerialization rowSerialization = new InternalRowSerialization();

    public InternalRowCollector(Handover<InternalRow> handover, CheckpointLock checkpointLock) {
        this.handover = handover;
        this.checkpointLock = checkpointLock;
    }

    @Override
    public void collect(SeaTunnelRow record) {
        try {
            // TODO: Lock InternalRowCollector while checkpoint is running
            handover.produce(rowSerialization.serialize(record));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CheckpointLock getCheckpointLock() {
        return this.checkpointLock;
    }
}
