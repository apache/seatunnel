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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.utils.MathUtils;

import static org.apache.paimon.index.BucketAssigner.computeAssigner;

public class RowAssignerChannelComputer implements ChannelComputer<InternalRow> {
    private static final long serialVersionUID = 1L;

    private final TableSchema schema;
    private Integer numAssigners;

    private transient int numChannels;
    private transient RowPartitionKeyExtractor extractor;

    public RowAssignerChannelComputer(TableSchema schema, Integer numAssigners) {
        this.schema = schema;
        this.numAssigners = numAssigners;
    }

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.numAssigners = MathUtils.min(numAssigners, numChannels);
        this.extractor = new RowPartitionKeyExtractor(schema);
    }

    @Override
    public int channel(InternalRow record) {
        int partitionHash = extractor.partition(record).hashCode();
        int keyHash = extractor.trimmedPrimaryKey(record).hashCode();
        return computeAssigner(partitionHash, keyHash, numChannels, numAssigners);
    }

    @Override
    public String toString() {
        return "shuffle by key hash";
    }
}
