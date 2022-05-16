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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.translation.sink.SinkConverter;

import org.apache.flink.api.connector.sink.Sink;

import java.util.Map;

public class FlinkSinkConverter<SeaTunnelRowT, FlinkRowT, StateT, CommitInfoT, AggregatedCommitInfoT>
        implements SinkConverter<SeaTunnelSink<SeaTunnelRowT, StateT, CommitInfoT,
        AggregatedCommitInfoT>, Sink<FlinkRowT, StateT, CommitInfoT, AggregatedCommitInfoT>> {

    @Override
    @SuppressWarnings("unchecked")
    public Sink<FlinkRowT, StateT, CommitInfoT, AggregatedCommitInfoT> convert(
            SeaTunnelSink<SeaTunnelRowT, StateT, CommitInfoT, AggregatedCommitInfoT> sink, Map<String, String> configuration) {
        return (Sink<FlinkRowT, StateT, CommitInfoT, AggregatedCommitInfoT>) new FlinkSink<>(sink, configuration);

    }
}
