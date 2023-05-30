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

package org.apache.seatunnel.engine.server.metrics;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.Unit;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SeaTunnelMetricsContext extends AbstractMetricsContext
        implements DynamicMetricsProvider {

    @Override
    public void provideDynamicMetrics(MetricDescriptor tagger, MetricsCollectionContext context) {
        metrics.forEach(
                (name, metric) -> {
                    if (metric instanceof Counter) {
                        context.collect(
                                tagger.copy(),
                                name,
                                ProbeLevel.INFO,
                                toProbeUnit(metric.unit()),
                                ((Counter) metric).getCount());
                    } else if (metric instanceof Meter) {
                        context.collect(
                                tagger.copy(),
                                name,
                                ProbeLevel.INFO,
                                toProbeUnit(metric.unit()),
                                ((Meter) metric).getRate());
                    } else {
                        throw new SeaTunnelException(
                                "The value of Metric does not support "
                                        + metric.getClass().getSimpleName()
                                        + " data type");
                    }
                });
    }

    private ProbeUnit toProbeUnit(Unit unit) {
        return ProbeUnit.valueOf(unit.name());
    }
}
