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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.BaseSourceFunction;
import org.apache.seatunnel.translation.source.ParallelSource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.types.Row;

/** The parallel source function implementation of {@link BaseSeaTunnelSourceFunction} */
public class SeaTunnelParallelSource extends BaseSeaTunnelSourceFunction
        implements ParallelSourceFunction<Row> {

    protected static final String PARALLEL_SOURCE_STATE_NAME = "parallel-source-states";

    public SeaTunnelParallelSource(SeaTunnelSource<SeaTunnelRow, ?, ?> source, Config envConfigs) {
        // TODO: Make sure the source is uncoordinated.
        super(source, envConfigs);
    }

    @Override
    protected BaseSourceFunction<SeaTunnelRow> createInternalSource() {
        return new ParallelSource<>(
                source,
                restoredState,
                getRuntimeContext().getNumberOfParallelSubtasks(),
                getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    protected String getStateName() {
        return PARALLEL_SOURCE_STATE_NAME;
    }
}
