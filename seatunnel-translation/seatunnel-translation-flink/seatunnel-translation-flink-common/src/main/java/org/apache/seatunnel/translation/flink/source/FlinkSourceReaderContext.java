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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;

import org.apache.flink.api.connector.source.SourceReaderContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of {@link org.apache.seatunnel.api.source.SourceReader.Context} for flink
 * engine.
 */
public class FlinkSourceReaderContext implements SourceReader.Context {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkSourceReaderContext.class);

    private final SourceReaderContext readerContext;

    private final SeaTunnelSource source;

    public FlinkSourceReaderContext(SourceReaderContext readerContext, SeaTunnelSource source) {
        this.readerContext = readerContext;
        this.source = source;
    }

    @Override
    public int getIndexOfSubtask() {
        return readerContext.getIndexOfSubtask();
    }

    @Override
    public org.apache.seatunnel.api.source.Boundedness getBoundedness() {
        return source.getBoundedness();
    }

    @Override
    public void signalNoMoreElement() {
        readerContext.sendSourceEventToCoordinator(
                new NoMoreElementEvent(readerContext.getIndexOfSubtask()));
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            LOGGER.warn("Encountered unexpected interrupt exception");
        }
    }

    @Override
    public void sendSplitRequest() {
        readerContext.sendSplitRequest();
    }

    @Override
    public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
        readerContext.sendSourceEventToCoordinator(new SourceEventWrapper(sourceEvent));
    }

    @Override
    public MetricsContext getMetricsContext() {
        return new AbstractMetricsContext() {};
    }
}
