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

package org.apache.seatunnel.e2e.sink.inmemory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InMemoryAggregatedCommitter
        implements SinkAggregatedCommitter<InMemoryCommitInfo, InMemoryAggregatedCommitInfo>,
                SupportMultiTableSinkAggregatedCommitter<InMemoryConnection> {

    private static final List<String> events = new ArrayList<>();
    private static final List<InMemoryMultiTableResourceManager> resourceManagers =
            new ArrayList<>();
    private ReadonlyConfig config;

    public InMemoryAggregatedCommitter(ReadonlyConfig config) {
        this.config = config;
    }

    public static List<String> getEvents() {
        return events;
    }

    public static List<InMemoryMultiTableResourceManager> getResourceManagers() {
        return resourceManagers;
    }

    private InMemoryMultiTableResourceManager resourceManager;

    @Override
    public MultiTableResourceManager<InMemoryConnection> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        events.add("initMultiTableResourceManager" + queueSize);
        return new InMemoryMultiTableResourceManager();
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<InMemoryConnection> multiTableResourceManager,
            int queueIndex) {
        events.add("setMultiTableResourceManager" + queueIndex);
        this.resourceManager = (InMemoryMultiTableResourceManager) multiTableResourceManager;
        resourceManagers.add(this.resourceManager);
    }

    @Override
    public List<InMemoryAggregatedCommitInfo> commit(
            List<InMemoryAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        if (config.get(InMemorySinkFactory.THROW_EXCEPTION_OF_COMMITTER)) {
            throw new IOException("commit failed");
        }
        return new ArrayList<>();
    }

    @Override
    public InMemoryAggregatedCommitInfo combine(List<InMemoryCommitInfo> commitInfos) {
        return new InMemoryAggregatedCommitInfo();
    }

    @Override
    public void abort(List<InMemoryAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {}

    @Override
    public void close() throws IOException {}
}
