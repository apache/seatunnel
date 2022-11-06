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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.IOException;
import java.util.List;

public class RabbitmqSplitEnumerator implements SourceSplitEnumerator {

    @Override
    public void open() {
        //do nothing
    }

    @Override
    public void run() throws Exception {
        //do nothing
    }

    @Override
    public void close() throws IOException {
        //do nothing
    }

    @Override
    public void addSplitsBack(List splits, int subtaskId) {
        //do nothing
    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        //do nothing
    }

    @Override
    public void registerReader(int subtaskId) {
        //do nothing
    }

    @Override
    public Object snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        //do nothing
    }
}
