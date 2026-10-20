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

package org.apache.seatunnel.connectors.seatunnel.websocket.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import lombok.AllArgsConstructor;

import java.io.IOException;

/**
 * Bridges a {@link DeserializationSchema} to the reader's {@link Collector}.
 *
 * <p>A single JSON message may carry an array of records, and only {@link
 * JsonDeserializationSchema#collect} is able to fan such an array out into several rows, so that
 * branch must be kept.
 */
@AllArgsConstructor
public class WebSocketDeserializationCollector {

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    /** Counts how many rows the given message produced, used by the bounded stop conditions. */
    public int collect(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        if (deserializationSchema instanceof JsonDeserializationSchema) {
            CountingCollector countingCollector = new CountingCollector(out);
            ((JsonDeserializationSchema) deserializationSchema).collect(message, countingCollector);
            return countingCollector.count;
        }
        SeaTunnelRow row = deserializationSchema.deserialize(message);
        if (row == null) {
            return 0;
        }
        out.collect(row);
        return 1;
    }

    private static class CountingCollector implements Collector<SeaTunnelRow> {

        private final Collector<SeaTunnelRow> delegate;
        private int count;

        private CountingCollector(Collector<SeaTunnelRow> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            delegate.collect(record);
            count++;
        }

        @Override
        public Object getCheckpointLock() {
            return delegate.getCheckpointLock();
        }
    }
}
