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

package org.apache.seatunnel.engine.server.task.context;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.engine.server.task.flow.SourceFlowLifeCycle;

public class SourceReaderContext implements SourceReader.Context {

    private final int index;

    private final Boundedness boundedness;

    private final SourceFlowLifeCycle<?, ?> sourceActionLifeCycle;

    public SourceReaderContext(int index, Boundedness boundedness,
                               SourceFlowLifeCycle<?, ?> sourceActionLifeCycle) {
        this.index = index;
        this.boundedness = boundedness;
        this.sourceActionLifeCycle = sourceActionLifeCycle;
    }

    @Override
    public int getIndexOfSubtask() {
        return index;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public void signalNoMoreElement() {
        sourceActionLifeCycle.signalNoMoreElement();
    }

    @Override
    public void sendSplitRequest() {
        sourceActionLifeCycle.requestSplit();
    }

    @Override
    public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
        sourceActionLifeCycle.sendSourceEventToEnumerator(sourceEvent);
    }
}
