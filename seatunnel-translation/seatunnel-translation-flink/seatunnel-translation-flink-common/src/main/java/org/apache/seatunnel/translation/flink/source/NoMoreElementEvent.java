/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.api.source.SourceReader.Context;

import org.apache.flink.api.connector.source.SourceEvent;

/**
 * This event represents that there is no more data to read, the execution process is as follows:
 *
 * <p>1. When a {@link org.apache.seatunnel.api.source.SourceReader} has no more data to read, it
 * will invoke {@link Context#signalNoMoreElement()} and send this event to {@link
 * FlinkSourceEnumerator}.<br>
 * 2. After {@link FlinkSourceEnumerator} received this event and invoke {@link
 * org.apache.flink.api.connector.source.SplitEnumeratorContext#sendEventToSourceReader(int,
 * SourceEvent)} send this event to {@link FlinkSourceReader}.<br>
 * 3. After {@link FlinkSourceReader} received this event and change {@link
 * org.apache.flink.core.io.InputStatus} from MORE_AVAILABLE to END_INPUT.<br>
 */
public final class NoMoreElementEvent implements SourceEvent {
    private final int subTaskIndex;

    public NoMoreElementEvent(int subTaskIndex) {
        this.subTaskIndex = subTaskIndex;
    }

    public int getSubTaskIndex() {
        return subTaskIndex;
    }
}
