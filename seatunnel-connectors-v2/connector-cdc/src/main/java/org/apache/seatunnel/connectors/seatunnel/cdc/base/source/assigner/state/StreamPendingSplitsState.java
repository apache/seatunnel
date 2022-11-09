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

package org.apache.seatunnel.connectors.seatunnel.cdc.base.source.assigner.state;

import java.util.Objects;

/** A {@link PendingSplitsState} for pending stream splits. */
public class StreamPendingSplitsState extends PendingSplitsState {

    private final boolean isStreamSplitAssigned;

    public StreamPendingSplitsState(boolean isStreamSplitAssigned) {
        this.isStreamSplitAssigned = isStreamSplitAssigned;
    }

    public boolean isStreamSplitAssigned() {
        return isStreamSplitAssigned;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StreamPendingSplitsState that = (StreamPendingSplitsState) o;
        return isStreamSplitAssigned == that.isStreamSplitAssigned;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isStreamSplitAssigned);
    }

    @Override
    public String toString() {
        return "StreamPendingSplitsState{" + "isStreamSplitAssigned=" + isStreamSplitAssigned + '}';
    }
}
