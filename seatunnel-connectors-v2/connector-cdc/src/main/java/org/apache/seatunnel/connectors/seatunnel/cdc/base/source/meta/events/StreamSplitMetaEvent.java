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

package org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.events;

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.reader.IncrementalSourceReader;

import java.util.List;

/**
 * The {@link SourceEvent} that {@link IncrementalSourceEnumerator} sends to {@link
 * IncrementalSourceReader} to pass change log metadata, i.e. {@link FinishedSnapshotSplitInfo}.
 */
public class StreamSplitMetaEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final String splitId;

    /** The metadata of stream split is divided to multiple groups. */
    private final int metaGroupId;
    /**
     * The serialized metadata of stream split, it's serialized/deserialized by {@link
     * FinishedSnapshotSplitInfo#serialize()} and {@link
     * FinishedSnapshotSplitInfo#deserialize(byte[])}.
     */
    private final List<byte[]> metaGroup;

    public StreamSplitMetaEvent(String splitId, int metaGroupId, List<byte[]> metaGroup) {
        this.splitId = splitId;
        this.metaGroupId = metaGroupId;
        this.metaGroup = metaGroup;
    }

    public String getSplitId() {
        return splitId;
    }

    public int getMetaGroupId() {
        return metaGroupId;
    }

    public List<byte[]> getMetaGroup() {
        return metaGroup;
    }
}
