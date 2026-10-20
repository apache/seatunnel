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

package org.apache.seatunnel.benchmark.connector.sink;

import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.benchmark.connector.BenchmarkConnectorOptions;

import com.google.auto.service.AutoService;

/** Factory for {@link BenchmarkSink}. */
@AutoService(Factory.class)
public final class BenchmarkSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return BenchmarkSink.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        BenchmarkConnectorOptions.RESULT_PATH,
                        Conditions.notBlank(BenchmarkConnectorOptions.RESULT_PATH))
                .required(
                        BenchmarkConnectorOptions.RUN_ID,
                        Conditions.matches(BenchmarkConnectorOptions.RUN_ID, "[A-Za-z0-9._-]+"))
                .required(
                        BenchmarkConnectorOptions.EXPECTED_ROWS,
                        Conditions.greaterThan(BenchmarkConnectorOptions.EXPECTED_ROWS, 0L))
                .optional(
                        BenchmarkConnectorOptions.RATE_PER_SECOND,
                        Conditions.greaterOrEqual(BenchmarkConnectorOptions.RATE_PER_SECOND, 0L))
                .optional(
                        BenchmarkConnectorOptions.MAX_TRACKED_LATENCY_MILLIS,
                        Conditions.greaterThan(
                                BenchmarkConnectorOptions.MAX_TRACKED_LATENCY_MILLIS, 0))
                .optional(
                        BenchmarkConnectorOptions.MAX_P99_LATENCY_MILLIS,
                        Conditions.greaterOrEqual(
                                BenchmarkConnectorOptions.MAX_P99_LATENCY_MILLIS, 0L))
                .optional(
                        BenchmarkConnectorOptions.MAX_LATENCY_GROWTH_RATIO,
                        Conditions.greaterOrEqual(
                                BenchmarkConnectorOptions.MAX_LATENCY_GROWTH_RATIO, 1D))
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new BenchmarkSink(context.getCatalogTable(), context.getOptions());
    }
}
