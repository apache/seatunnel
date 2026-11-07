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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.SupportSinkDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

/**
 * Test-only sink factory (registered via META-INF/services) implementing {@link
 * SupportSinkDryRunValidation}, since no real sink connector on the test classpath supports connect
 * dry-run validation.
 */
public class DryRunTestSinkFactory
        implements TableSinkFactory<SeaTunnelRow, Void, Void, Void>, SupportSinkDryRunValidation {

    static final Option<Boolean> FAIL_VALIDATION =
            Options.key("fail_validation").booleanType().defaultValue(false);
    static final Option<String> EXPECTED_TABLE =
            Options.key("expected_table").stringType().noDefaultValue();

    @Override
    public String factoryIdentifier() {
        return "DryRunTestSink";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().optional(FAIL_VALIDATION, EXPECTED_TABLE).build();
    }

    @Override
    public void validateConnectionForDryRun(TableSinkFactoryContext context) {
        if (context.getOptions().get(FAIL_VALIDATION)) {
            throw new IllegalStateException("simulated sink validation failure: target missing");
        }
        context.getOptions()
                .getOptional(EXPECTED_TABLE)
                .ifPresent(
                        expectedTable -> {
                            String actualTable =
                                    context.getCatalogTable().getTableId().getTableName();
                            if (!expectedTable.equals(actualTable)) {
                                throw new IllegalStateException(
                                        "expected table "
                                                + expectedTable
                                                + " but received "
                                                + actualTable);
                            }
                        });
    }
}
