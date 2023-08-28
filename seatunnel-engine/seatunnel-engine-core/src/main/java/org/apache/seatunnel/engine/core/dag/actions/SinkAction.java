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

package org.apache.seatunnel.engine.core.dag.actions;

import org.apache.seatunnel.api.sink.SeaTunnelSink;

import lombok.NonNull;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SinkAction<IN, StateT, CommitInfoT, AggregatedCommitInfoT> extends AbstractAction {
    private final SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> sink;

    public SinkAction(
            long id,
            @NonNull String name,
            @NonNull SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            @NonNull Set<URL> jarUrls) {
        this(id, name, new ArrayList<>(), sink, jarUrls);
    }

    public SinkAction(
            long id,
            @NonNull String name,
            @NonNull List<Action> upstreams,
            @NonNull SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            @NonNull Set<URL> jarUrls) {
        this(id, name, upstreams, sink, jarUrls, null);
    }

    public SinkAction(
            long id,
            @NonNull String name,
            @NonNull List<Action> upstreams,
            @NonNull SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            @NonNull Set<URL> jarUrls,
            SinkConfig config) {
        super(id, name, upstreams, jarUrls, config);
        this.sink = sink;
    }

    public SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> getSink() {
        return sink;
    }

    @Override
    public SinkConfig getConfig() {
        return (SinkConfig) super.getConfig();
    }
}
