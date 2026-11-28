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

package org.apache.seatunnel.connectors.seatunnel.adjust;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.ADDITIONAL_METRICS;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.ALLOW_INSECURE_HTTP;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.DIMENSIONS;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.METRICS;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.METRIC_SCALES;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.UNTIL_TODAY;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.UTC_OFFSET;

/** Exposes the Adjust report source and its dynamic selection options. */
@AutoService(Factory.class)
public final class AdjustSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AdjustSource.ID;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(TOKEN, START_DATE, DIMENSIONS, METRICS)
                .optional(
                        ADDITIONAL_METRICS,
                        METRIC_SCALES,
                        UNTIL_TODAY,
                        UTC_OFFSET,
                        BASE_URL,
                        ALLOW_INSECURE_HTTP,
                        REQUEST_TIMEOUT_MS,
                        MAX_ATTEMPTS)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        AdjustSource source = new AdjustSource(new AdjustReportPlan(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AdjustSource.class;
    }
}
