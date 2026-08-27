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

package org.apache.seatunnel.translation.spark.source.reader;

import org.apache.seatunnel.translation.spark.source.reader.batch.ParallelBatchPartitionReader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.io.IOException;

public class SeaTunnelInputPartitionReader implements InputPartitionReader<InternalRow> {

    private final ParallelBatchPartitionReader partitionReader;

    public SeaTunnelInputPartitionReader(ParallelBatchPartitionReader partitionReader) {
        this.partitionReader = partitionReader;
    }

    @Override
    public boolean next() throws IOException {
        try {
            return partitionReader.next();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InternalRow get() {
        return partitionReader.get();
    }

    @Override
    public void close() throws IOException {
        partitionReader.close();
    }
}
