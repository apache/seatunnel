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

package org.apache.seatunnel.engine.e2e.checkpoint;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

/**
 * Factory of the test-only BOUNDED source that can hold a BATCH job open until the test releases
 * it.
 *
 * <p>The factory is registered through {@code @AutoService} like the other test-tree plugins of
 * this module, so the in-JVM Zeta cluster discovers it from the test classpath.
 */
@AutoService(Factory.class)
public class GatedBatchSourceFactory implements TableSourceFactory {

    /** Plugin name referenced by the test conf's {@code source} block. */
    public static final String IDENTIFIER = "GatedBatchSource";

    /**
     * Key that binds the source to the gate armed by the test, see {@link BatchSourceGate}. While
     * the key is armed the reader keeps emitting and never reports "no more element".
     */
    public static final Option<String> HOLD_KEY =
            Options.key("hold_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Hold key shared with the test; the reader keeps running until the test"
                                    + " releases it");

    /** Delay between two emitted rows, i.e. between two gate checks. */
    public static final Option<Long> EMIT_INTERVAL_MS =
            Options.key("emit_interval_ms")
                    .longType()
                    .defaultValue(100L)
                    .withDescription("Interval in milliseconds between two emitted rows");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(HOLD_KEY).optional(EMIT_INTERVAL_MS).build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>) new GatedBatchSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return GatedBatchSource.class;
    }
}
