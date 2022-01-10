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

package org.apache.seatunnel.flink.sink;

import org.apache.hadoop.hbase.client.ConnectionConfiguration;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

public class WriteOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long bufferFlushMaxSizeInBytes;
    private final long bufferFlushMaxRows;
    private final long bufferFlushIntervalMillis;

    private WriteOptions(
            long bufferFlushMaxSizeInBytes,
            long bufferFlushMaxMutations,
            long bufferFlushIntervalMillis) {
        this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
        this.bufferFlushMaxRows = bufferFlushMaxMutations;
        this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
    }

    public long getBufferFlushMaxSizeInBytes() {
        return bufferFlushMaxSizeInBytes;
    }

    public long getBufferFlushMaxRows() {
        return bufferFlushMaxRows;
    }

    public long getBufferFlushIntervalMillis() {
        return bufferFlushIntervalMillis;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        // default is 2mb which is defined in hbase
        private long bufferFlushMaxSizeInBytes = ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT;
        private long bufferFlushMaxRows = -1;
        private long bufferFlushIntervalMillis = -1;

        /**
         * Optional. Sets when to flush a buffered request based on the memory size of rows currently added.
         * Default to <code>2mb</code>.
         */
        public Builder setBufferFlushMaxSizeInBytes(long bufferFlushMaxSizeInBytes) {
            checkArgument(
                    bufferFlushMaxSizeInBytes > 0,
                    "Max byte size of buffered rows must be larger than 0.");
            this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
            return this;
        }

        public Builder setBufferFlushMaxRows(long bufferFlushMaxRows) {
            checkArgument(
                    bufferFlushMaxRows > 0,
                    "Max number of buffered rows must be larger than 0.");
            this.bufferFlushMaxRows = bufferFlushMaxRows;
            return this;
        }

        public Builder setBufferFlushIntervalMillis(long bufferFlushIntervalMillis) {
            checkArgument(
                    bufferFlushIntervalMillis > 0,
                    "Interval (in milliseconds) between each flush must be larger than 0.");
            this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
            return this;
        }

        public WriteOptions build() {
            return new WriteOptions(
                    bufferFlushMaxSizeInBytes,
                    bufferFlushMaxRows,
                    bufferFlushIntervalMillis);
        }
    }
}
