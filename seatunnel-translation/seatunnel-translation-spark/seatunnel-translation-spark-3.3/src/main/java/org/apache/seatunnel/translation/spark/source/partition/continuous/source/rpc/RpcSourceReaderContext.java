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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;

import org.apache.spark.rpc.RpcEndpointRef;

public class RpcSourceReaderContext implements SourceReader.Context {
    private final RpcEndpointRef driverRef;

    public RpcSourceReaderContext(RpcEndpointRef driverRef) {
        this.driverRef = driverRef;
    }

    @Override
    public int getIndexOfSubtask() {
        return 0;
    }

    @Override
    public Boundedness getBoundedness() {
        return null;
    }

    @Override
    public void signalNoMoreElement() {}

    @Override
    public void sendSplitRequest() {}

    @Override
    public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

    @Override
    public MetricsContext getMetricsContext() {
        return null;
    }

    @Override
    public EventListener getEventListener() {
        return null;
    }
}
