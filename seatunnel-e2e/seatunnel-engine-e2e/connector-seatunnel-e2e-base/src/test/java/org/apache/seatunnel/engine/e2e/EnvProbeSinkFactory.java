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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/** E2E-only sink factory proving the sink factory context receives the whole env block. */
@AutoService(Factory.class)
public class EnvProbeSinkFactory implements TableSinkFactory<SeaTunnelRow, Void, Void, Void> {

    public static final String IDENTIFIER = "EnvProbeSink";

    private static final AtomicReference<String> CAPTURED_ENV = new AtomicReference<>();

    private static final ConcurrentLinkedQueue<List<Object>> RECEIVED_ROWS =
            new ConcurrentLinkedQueue<>();

    public static void reset() {
        CAPTURED_ENV.set(null);
        RECEIVED_ROWS.clear();
    }

    public static String capturedEnv() {
        return CAPTURED_ENV.get();
    }

    public static List<List<Object>> receivedRows() {
        return Arrays.asList(RECEIVED_ROWS.toArray(new List[0]));
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
    public TableSink<SeaTunnelRow, Void, Void, Void> createSink(TableSinkFactoryContext context) {
        CAPTURED_ENV.set(EnvProbeSupport.probe(context.getEnvOptions()));
        return EnvProbeSink::new;
    }

    /** Minimal sink that records every written row for in-process assertions. */
    public static class EnvProbeSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

        @Override
        public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
                throws IOException {
            return new EnvProbeSinkWriter();
        }

        @Override
        public String getPluginName() {
            return IDENTIFIER;
        }
    }

    private static class EnvProbeSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

        @Override
        public void write(SeaTunnelRow element) {
            RECEIVED_ROWS.add(Arrays.asList(element.getFields()));
        }

        @Override
        public void close() {
            // nothing to release
        }
    }
}
