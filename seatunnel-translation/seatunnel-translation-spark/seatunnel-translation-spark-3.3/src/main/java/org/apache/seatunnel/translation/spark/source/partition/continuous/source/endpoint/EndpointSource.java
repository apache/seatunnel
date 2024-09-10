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

package org.apache.seatunnel.translation.spark.source.partition.continuous.source.endpoint;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.translation.source.BaseSourceFunction;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class EndpointSource<T, SplitT extends SourceSplit, StateT extends Serializable>
        implements BaseSourceFunction<T> {
    private final SeaTunnelSource<T, SplitT, StateT> source;
    protected final SourceSplitEnumerator<SplitT, StateT> splitEnumerator;
    protected final Integer parallelism;
    protected final String jobId;

    public EndpointSource(
            SeaTunnelSource<T, SplitT, StateT> source, Integer parallelism, String jobId) {
        this.source = source;
        this.parallelism = parallelism;
        this.jobId = jobId;
        SourceSplitEnumerator.Context<SplitT> enumeratorContext =
                new EndpointSplitEnumeratorContext<>(parallelism, jobId);
        try {
            splitEnumerator = source.restoreEnumerator(enumeratorContext, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void run(Collector<T> collector) throws Exception {}

    @Override
    public Map<Integer, List<byte[]>> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
