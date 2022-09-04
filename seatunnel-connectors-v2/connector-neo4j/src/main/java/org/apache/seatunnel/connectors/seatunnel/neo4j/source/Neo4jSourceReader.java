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

package org.apache.seatunnel.connectors.seatunnel.neo4j.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.List;

public class Neo4jSourceReader implements SourceReader<SeaTunnelRow, Neo4jSourceSplit> {
    @Override
    public void open() throws Exception {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {

    }

    @Override
    public List<Neo4jSourceSplit> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void addSplits(List<Neo4jSourceSplit> splits) {

    }

    @Override
    public void handleNoMoreSplits() {

    }

    @Override
    public void handleSourceEvent(SourceEvent sourceEvent) {
        SourceReader.super.handleSourceEvent(sourceEvent);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        SourceReader.super.notifyCheckpointAborted(checkpointId);
    }
}
