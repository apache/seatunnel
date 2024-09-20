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

package org.apache.seatunnel.translation.spark.source.partition.continuous.source.rpc;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.apache.spark.rpc.RpcEndpointRef;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class RpcSplitEnumerator<SplitT extends SourceSplit, StateT extends Serializable>
        implements SourceSplitEnumerator<SplitT, StateT> {
    private final RpcEndpointRef driverRef;

    public RpcSplitEnumerator(RpcEndpointRef driverRef) {
        this.driverRef = driverRef;
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {}

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<SplitT> splits, int subtaskId) {}

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {}

    @Override
    public StateT snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
