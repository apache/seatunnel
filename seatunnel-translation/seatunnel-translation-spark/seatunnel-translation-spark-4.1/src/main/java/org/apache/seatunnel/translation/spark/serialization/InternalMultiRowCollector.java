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

package org.apache.seatunnel.translation.spark.serialization;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Handover;

import org.apache.spark.sql.catalyst.InternalRow;

import java.util.Map;

public class InternalMultiRowCollector extends InternalRowCollector {
    private final Map<String, InternalRowConverter> rowSerializationMap;

    public InternalMultiRowCollector(
            Handover<InternalRow> handover,
            Object checkpointLock,
            Map<String, InternalRowConverter> rowSerializationMap,
            Map<String, String> envOptionsInfo) {
        super(handover, checkpointLock, null, envOptionsInfo);
        this.rowSerializationMap = rowSerializationMap;
    }

    @Override
    public void collect(SeaTunnelRow record) {
        try {
            synchronized (checkpointLock) {
                InternalRowConverter rowSerialization =
                        rowSerializationMap.get(record.getTableId());
                flowControlGate.audit(record);
                handover.produce(rowSerialization.convert(record));
            }
            collectTotalCount.incrementAndGet();
            emptyThisPollNext = false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, InternalRowConverter> getRowSerializationMap() {
        return rowSerializationMap;
    }
}
