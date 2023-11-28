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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.scan;

import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset.RedoLogOffset;

import io.debezium.pipeline.source.spi.ChangeEventSource;

/**
 * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high watermark
 * for each {@link SnapshotSplit}.
 */
public class SnapshotSplitChangeEventSourceContext
        implements ChangeEventSource.ChangeEventSourceContext {

    private RedoLogOffset lowWatermark;
    private RedoLogOffset highWatermark;

    public RedoLogOffset getLowWatermark() {
        return lowWatermark;
    }

    public void setLowWatermark(RedoLogOffset lowWatermark) {
        this.lowWatermark = lowWatermark;
    }

    public RedoLogOffset getHighWatermark() {
        return highWatermark;
    }

    public void setHighWatermark(RedoLogOffset highWatermark) {
        this.highWatermark = highWatermark;
    }

    @Override
    public boolean isRunning() {
        return lowWatermark != null && highWatermark != null;
    }
}
