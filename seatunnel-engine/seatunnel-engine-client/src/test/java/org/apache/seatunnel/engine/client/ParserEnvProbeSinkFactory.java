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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import org.mockito.Mockito;

import com.google.auto.service.AutoService;

/** Test-only sink factory that captures the env options from the factory context. */
@AutoService(Factory.class)
public class ParserEnvProbeSinkFactory implements TableSinkFactory<Object, Object, Object, Object> {

    public static final String IDENTIFIER = "ParserEnvProbeSink";

    private static volatile ReadonlyConfig capturedEnvOptions;

    public static void reset() {
        capturedEnvOptions = null;
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
    public TableSink<Object, Object, Object, Object> createSink(TableSinkFactoryContext context) {
        if (context != null) {
            capturedEnvOptions = context.getEnvOptions();
        }
        TableSink<Object, Object, Object, Object> tableSink = Mockito.mock(TableSink.class);
        Mockito.when(tableSink.createSink())
                .thenReturn(Mockito.mock(org.apache.seatunnel.api.sink.SeaTunnelSink.class));
        return tableSink;
    }
}
