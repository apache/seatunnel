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

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.ParallelSource;

import org.apache.spark.sql.sources.v2.reader.streaming.Offset;

import java.util.List;

public class SparkState<SplitT extends SourceSplit, StateT> extends Offset {

    protected final ParallelSource<SeaTunnelRow, SplitT, StateT> parallelSource;
    protected volatile Integer checkpointId;

    public SparkState(ParallelSource<SeaTunnelRow, SplitT, StateT> parallelSource, int checkpointId) {
        this.parallelSource = parallelSource;
        this.checkpointId = checkpointId;
    }

    @Override
    public String json() {
        try {
            List<byte[]> bytes = this.parallelSource.snapshotState(checkpointId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
