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

package org.apache.seatunnel.translation.source;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;

public class CoordinatedReaderContext implements SourceReader.Context {

    protected final CoordinatedSource<?, ?, ?> coordinatedSource;
    protected final Boundedness boundedness;
    protected final Integer subtaskId;

    public CoordinatedReaderContext(
            CoordinatedSource<?, ?, ?> coordinatedSource,
            Boundedness boundedness,
            Integer subtaskId) {
        this.coordinatedSource = coordinatedSource;
        this.boundedness = boundedness;
        this.subtaskId = subtaskId;
    }

    @Override
    public int getIndexOfSubtask() {
        return this.subtaskId;
    }

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    @Override
    public void signalNoMoreElement() {
        coordinatedSource.handleNoMoreElement(subtaskId);
    }

    @Override
    public void sendSplitRequest() {
        coordinatedSource.handleSplitRequest(subtaskId);
    }

    @Override
    public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
        coordinatedSource.handleReaderEvent(subtaskId, sourceEvent);
    }

    @Override
    public MetricsContext getMetricsContext() {
        // TODO Waiting for Flink and Spark to implement MetricsContext
        // https://github.com/apache/seatunnel/issues/3431
        return new AbstractMetricsContext() {};
    }
}
