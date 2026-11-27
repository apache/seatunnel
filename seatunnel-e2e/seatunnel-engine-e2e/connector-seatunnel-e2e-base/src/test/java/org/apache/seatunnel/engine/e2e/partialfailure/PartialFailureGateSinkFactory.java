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

package org.apache.seatunnel.engine.e2e.partialfailure;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/**
 * Factory of the test-only sink that either injects a runtime failure into one pipeline of a
 * multi-pipeline job or provides the durable "the healthy sibling is really progressing" evidence
 * for it.
 *
 * <p>The factory is registered through {@code @AutoService} like the other test-tree sinks of this
 * module, so the in-JVM Zeta cluster discovers it from the test classpath.
 */
@AutoService(Factory.class)
public class PartialFailureGateSinkFactory implements TableSinkFactory {

    /** Plugin name referenced by the test conf's {@code sink} block. */
    public static final String IDENTIFIER = "PartialFailureGateTest";

    /**
     * Selects the role of the sink. {@code true} makes the writer park on {@link #HOLD_KEY} when it
     * receives its first row and throw a runtime failure once the test releases the gate; {@code
     * false} makes it write every row to {@link #OUTPUT_PATH}.
     */
    public static final Option<Boolean> FAIL_AFTER_RELEASE =
            Options.key("fail_after_release")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "true injects the runtime failure after the test releases the gate, false writes the rows to output_path");

    /**
     * Key that binds the failing writer to the gate armed by the test, see {@link
     * PartialFailureGate}.
     */
    public static final Option<String> HOLD_KEY =
            Options.key("hold_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Hold key shared with the test; the failing writer parks until the test releases it");

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
        return OptionRule.builder().optional(FAIL_AFTER_RELEASE, HOLD_KEY, OUTPUT_PATH).build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        boolean failAfterRelease = options.get(FAIL_AFTER_RELEASE);
        String holdKey = options.getOptional(HOLD_KEY).orElse(null);
        String outputPath = options.getOptional(OUTPUT_PATH).orElse(null);
        if (failAfterRelease) {
            checkArgument(
                    holdKey != null && !holdKey.isEmpty(),
                    "fail_after_release=true requires the hold_key option");
        } else {
            checkArgument(
                    outputPath != null && !outputPath.isEmpty(),
                    "fail_after_release=false requires the output_path option");
        }
        return () -> new PartialFailureGateSink(holdKey, outputPath, failAfterRelease);
    }
}
