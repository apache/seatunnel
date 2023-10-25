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

package org.apache.seatunnel.connectors.seatunnel.common.multitablesink;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.sink.SinkWriter;

public class SinkContextProxy implements SinkWriter.Context {

    private final int index;

    private final SinkWriter.Context context;

    public SinkContextProxy(int index, SinkWriter.Context context) {
        this.index = index;
        this.context = context;
    }

    @Override
    public int getIndexOfSubtask() {
        return index;
    }

    @Override
    public MetricsContext getMetricsContext() {
        return context.getMetricsContext();
    }
}
