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

package org.apache.seatunnel.engine.e2e.savepointcancel;

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
 * Factory of the test-only sink that holds one checkpoint snapshot, see {@link
 * SavepointCancelHoldSink}.
 */
@AutoService(Factory.class)
public class SavepointCancelHoldSinkFactory implements TableSinkFactory {

    /** Plugin name referenced by the test conf's {@code sink} block. */
    public static final String IDENTIFIER = "SavepointCancelHoldTest";

    /** Key that binds the sink to the gate armed by the test. */
    public static final Option<String> HOLD_KEY =
            Options.key("hold_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Hold key shared with the test; the writer parks on this key's gate");

    /** One-based number of the checkpoint snapshot that must be held. */
    public static final Option<Integer> BLOCK_ON_SNAPSHOT =
            Options.key("block_on_snapshot")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "One-based number of the checkpoint snapshot whose writer"
                                    + " acknowledgement is held until the test releases the gate");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(HOLD_KEY).optional(BLOCK_ON_SNAPSHOT).build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        return () ->
                new SavepointCancelHoldSink(options.get(HOLD_KEY), options.get(BLOCK_ON_SNAPSHOT));
    }
}
