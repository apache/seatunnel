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

package org.apache.seatunnel.api.table.factory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;

import com.google.auto.service.AutoService;

/** Test-only sink factory that captures the env options from the factory context. */
@AutoService(Factory.class)
public class EnvProbeSinkFactory<IN, StateT, CommitInfoT, AggregatedCommitInfoT>
        implements TableSinkFactory<IN, StateT, CommitInfoT, AggregatedCommitInfoT> {

    public static final String IDENTIFIER = "EnvProbeSink";

    private static volatile ReadonlyConfig capturedEnvOptions;
    private static volatile TableSink<?, ?, ?, ?> tableSink;

    public static void reset() {
        capturedEnvOptions = null;
        tableSink = null;
    }

    public static void setTableSink(TableSink<?, ?, ?, ?> sink) {
        tableSink = sink;
    }

    public static ReadonlyConfig capturedEnvOptions() {
        return capturedEnvOptions;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public TableSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> createSink(
            TableSinkFactoryContext context) {
        if (context != null) {
            capturedEnvOptions = context.getEnvOptions();
        }
        return (TableSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT>) tableSink;
    }
}
