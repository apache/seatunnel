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
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.connector.TableSource;

import com.google.auto.service.AutoService;

import java.io.Serializable;

/** Test-only source factory that captures the env options from the factory context. */
@AutoService(Factory.class)
public class EnvProbeSourceFactory implements TableSourceFactory {

    public static final String IDENTIFIER = "EnvProbeSource";

    private static volatile ReadonlyConfig capturedEnvOptions;
    private static volatile TableSource<?, ?, ?> tableSource;

    public static void reset() {
        capturedEnvOptions = null;
        tableSource = null;
    }

    public static void setTableSource(TableSource<?, ?, ?> source) {
        tableSource = source;
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
    public <
                    T,
                    SplitT extends org.apache.seatunnel.api.source.SourceSplit,
                    StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        if (context != null) {
            capturedEnvOptions = context.getEnvOptions();
        }
        return (TableSource<T, SplitT, StateT>) tableSource;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SeaTunnelSource.class;
    }
}
