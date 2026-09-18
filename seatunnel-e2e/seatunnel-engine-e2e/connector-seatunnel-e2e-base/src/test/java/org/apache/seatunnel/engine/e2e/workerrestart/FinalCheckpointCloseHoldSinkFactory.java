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

package org.apache.seatunnel.engine.e2e.workerrestart;

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
 * Factory of the test-only sink that can hold its writer inside {@code close()} after the final
 * checkpoint of a BATCH pipeline completed.
 *
 * <p>The factory is registered through {@code @AutoService} like the other test-tree sinks of this
 * module, so the in-JVM Zeta cluster discovers it from the test classpath.
 */
@AutoService(Factory.class)
public class FinalCheckpointCloseHoldSinkFactory implements TableSinkFactory {

    /** Plugin name referenced by the test conf's {@code sink} block. */
    public static final String IDENTIFIER = "FinalCheckpointCloseHoldTest";

    /**
     * Key that binds the sink to the gate armed by the test, see {@link
     * FinalCheckpointCloseHoldGate}.
     */
    public static final Option<String> HOLD_KEY =
            Options.key("hold_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Hold key shared with the test; writer close blocks until the test releases it");

    /** Directory that receives one text file per writer instance with the rows it consumed. */
    public static final Option<String> OUTPUT_PATH =
            Options.key("output_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Directory that receives one line per written row");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(HOLD_KEY, OUTPUT_PATH).build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        return () ->
                new FinalCheckpointCloseHoldSink(options.get(HOLD_KEY), options.get(OUTPUT_PATH));
    }
}
