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

package org.apache.seatunnel.engine.e2e.savepointworker;

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
 * Factory of the test-only sink that parks one checkpoint snapshot until the test releases it.
 *
 * <p>The hold is coordinated through files instead of static state: the sink task may be deployed
 * with a fresh classloader after a savepoint restore, so a static registry owned by the test JVM
 * would not necessarily be visible from the restored writer. A directory shared by the in-JVM
 * cluster and the test thread is classloader-independent.
 */
@AutoService(Factory.class)
public class SavepointWorkerHoldSinkFactory implements TableSinkFactory {

    /** Plugin name referenced by the test job configuration. */
    public static final String IDENTIFIER = "SavepointWorkerHoldTest";

    /** Directory that contains the hold and release marker files. */
    public static final Option<String> HOLD_DIR =
            Options.key("hold_dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Directory shared with the test that contains the hold/release markers");

    /** File stem shared with the test for the hold and release marker files. */
    public static final Option<String> HOLD_KEY =
            Options.key("hold_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("File stem shared with the test for the hold markers");

    /** One-based number of the checkpoint snapshot whose acknowledgement must be held. */
    public static final Option<Integer> BLOCK_ON_SNAPSHOT =
            Options.key("block_on_snapshot")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "One-based number of the checkpoint snapshot whose writer"
                                    + " acknowledgement is held until the release marker exists");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOLD_DIR, HOLD_KEY)
                .optional(BLOCK_ON_SNAPSHOT)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        return () ->
                new SavepointWorkerHoldSink(
                        options.get(HOLD_DIR),
                        options.get(HOLD_KEY),
                        options.get(BLOCK_ON_SNAPSHOT));
    }
}
