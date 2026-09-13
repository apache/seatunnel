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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Test-only transform factory that verifies how many distinct input schemas reach the factory. */
public class DryRunTestTransformFactory implements TableTransformFactory {

    static final Option<Integer> EXPECTED_INPUT_COUNT =
            Options.key("expected_input_count").intType().noDefaultValue();
    static final Option<String> PRODUCED_TABLE =
            Options.key("produced_table").stringType().noDefaultValue();

    private static final List<String> CREATED_TABLES = new ArrayList<>();

    @Override
    public String factoryIdentifier() {
        return "DryRunTestTransform";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(EXPECTED_INPUT_COUNT, PRODUCED_TABLE).build();
    }

    @Override
    public TableTransform<?> createTransform(TableTransformFactoryContext context) {
        int expectedInputCount = context.getOptions().get(EXPECTED_INPUT_COUNT);
        if (context.getCatalogTables().size() != expectedInputCount) {
            throw new IllegalStateException(
                    "expected "
                            + expectedInputCount
                            + " input schemas but received "
                            + context.getCatalogTables().size());
        }
        String producedTableName = context.getOptions().get(PRODUCED_TABLE);
        CatalogTable producedTable =
                CatalogTableUtil.getCatalogTable(
                        producedTableName, context.getCatalogTables().get(0).getSeaTunnelRowType());
        synchronized (CREATED_TABLES) {
            CREATED_TABLES.add(producedTableName);
        }
        return () ->
                new SeaTunnelTransform<Object>() {
                    @Override
                    public CatalogTable getProducedCatalogTable() {
                        return producedTable;
                    }

                    @Override
                    public List<CatalogTable> getProducedCatalogTables() {
                        return Collections.singletonList(producedTable);
                    }

                    @Override
                    public String getPluginName() {
                        return "DryRunTestTransform";
                    }
                };
    }

    static void resetCreatedTables() {
        synchronized (CREATED_TABLES) {
            CREATED_TABLES.clear();
        }
    }

    static List<String> getCreatedTables() {
        synchronized (CREATED_TABLES) {
            return new ArrayList<>(CREATED_TABLES);
        }
    }
}
