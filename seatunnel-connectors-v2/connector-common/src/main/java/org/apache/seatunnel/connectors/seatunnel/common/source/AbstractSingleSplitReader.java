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

package org.apache.seatunnel.connectors.seatunnel.common.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;

import java.util.Collections;
import java.util.List;

public abstract class AbstractSingleSplitReader<T> implements SourceReader<T, SingleSplit> {

    protected volatile boolean noMoreSplits = false;

    @Override
    public void pollNext(Collector<T> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            if (noMoreSplits) {
                return;
            }
            internalPollNext(output);
            noMoreSplits = true;
        }
    }

    public void internalPollNext(Collector<T> output) throws Exception {}

    @Override
    public final List<SingleSplit> snapshotState(long checkpointId) throws Exception {
        return Collections.singletonList(new SingleSplit(snapshotStateToBytes(checkpointId)));
    }

    protected byte[] snapshotStateToBytes(long checkpointId) throws Exception {
        // default nothing
        return null;
    }

    @Override
    public final void addSplits(List<SingleSplit> splits) {
        if (splits.size() > 1) {
            throw new UnsupportedOperationException(
                    "The single-split reader don't support reading multiple splits");
        }
        byte[] restoredState = splits.get(0).getState();
        if (restoredState != null && restoredState.length > 0) {
            restoreState(restoredState);
        }
    }

    protected void restoreState(byte[] restoredState) {
        // default nothing
    }

    @Override
    public final void handleNoMoreSplits() {
        // nothing
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // default nothing
    }

    @Override
    public final void handleSourceEvent(SourceEvent sourceEvent) {
        // nothing
    }
}
