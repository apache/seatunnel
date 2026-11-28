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

package org.apache.seatunnel.engine.e2e.pipelineslot;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

/**
 * Factory of the test-only sink used to keep one pipeline of a batch job busy after its sibling
 * pipeline finished.
 *
 * <p>The factory is registered through {@code @AutoService} like the other test-tree sinks of this
 * module, so the in-JVM Zeta cluster discovers it from the test classpath.
 */
@AutoService(Factory.class)
public class PipelineFinishSlotReleaseSinkFactory implements TableSinkFactory {

    /** Plugin name referenced by the test confs' {@code sink} blocks. */
    public static final String IDENTIFIER = "PipelineFinishSlotReleaseTest";

    /** Counter key; lets the test tell the pipelines of different jobs apart. */
    public static final Option<String> TAG =
            Options.key("tag")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Counter key used by the test to observe this writer");

    /** Per-row sleep that keeps the sink (and therefore its pipeline) busy. */
    public static final Option<Long> SLEEP_MS =
            Options.key("sleep_ms")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Milliseconds to sleep after every row");

    /** Optional gate that parks the writer before its first row. */
    public static final Option<String> HOLD_GATE =
            Options.key("hold_gate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Gate key; when set the writer blocks before its first row until the test releases it");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(TAG).optional(SLEEP_MS, HOLD_GATE).build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        return () ->
                new PipelineFinishSlotReleaseSink(
                        options.get(TAG), options.get(SLEEP_MS), options.get(HOLD_GATE));
    }
}
