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
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.SupportSourceDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.Collections;
import java.util.List;

/**
 * Test-only source factory (registered via META-INF/services) used to exercise the connect dry-run
 * failure paths that no real connector on the test classpath can produce: empty schema inference
 * and connection-validation failures.
 */
public class DryRunTestSourceFactory implements TableSourceFactory, SupportSourceDryRunValidation {

    static final Option<Boolean> EMPTY_SCHEMA =
            Options.key("empty_schema").booleanType().defaultValue(false);
    static final Option<Boolean> FAIL_CONNECTION =
            Options.key("fail_connection").booleanType().defaultValue(false);
    static final Option<Boolean> SENSITIVE_CONNECTION_FAILURE =
            Options.key("sensitive_connection_failure").booleanType().defaultValue(false);

    @Override
    public String factoryIdentifier() {
        return "DryRunTestSource";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(EMPTY_SCHEMA, FAIL_CONNECTION, SENSITIVE_CONNECTION_FAILURE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SeaTunnelSource.class;
    }

    @Override
    public List<CatalogTable> inferSchemaForDryRun(TableSourceFactoryContext context) {
        if (context.getOptions().get(EMPTY_SCHEMA)) {
            return Collections.emptyList();
        }
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        return Collections.singletonList(CatalogTableUtil.getCatalogTable("test_table", rowType));
    }

    @Override
    public void validateConnectionForDryRun(
            TableSourceFactoryContext context, List<CatalogTable> catalogTables) {
        if (context.getOptions().get(FAIL_CONNECTION)) {
            throw new IllegalStateException("simulated connection failure: invalid credentials");
        }
        if (context.getOptions().get(SENSITIVE_CONNECTION_FAILURE)) {
            throw new IllegalStateException(
                    "No suitable driver found for "
                            + "jdbc:mysql://alice:secret-password@db.example.com:3306/orders?"
                            + "token=secret-token");
        }
    }
}
