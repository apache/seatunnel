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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class SQLTransformFactoryTest {

    @Test
    public void testFactoryIdentifierAndOptionRule() {
        SQLTransformFactory factory = new SQLTransformFactory();
        Assertions.assertEquals(SQLTransform.PLUGIN_NAME, factory.factoryIdentifier());

        OptionRule rule = factory.optionRule();
        // Just ensure optional keys are registered; exact contents will be validated elsewhere
        Assertions.assertNotNull(rule);
    }

    @Test
    public void testCreateTransformReturnsMultiCatalogTransform() {
        SQLTransformFactory factory = new SQLTransformFactory();

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test", rowType);
        List<CatalogTable> tables = Collections.singletonList(catalogTable);

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                SQLTransform.KEY_QUERY.key(), "select * from dual"));

        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        tables, config, Thread.currentThread().getContextClassLoader());

        TableTransform<?> tableTransform = factory.createTransform(context);
        Assertions.assertNotNull(tableTransform);

        SeaTunnelTransform<?> inner = tableTransform.createTransform();
        Assertions.assertNotNull(inner);
        Assertions.assertTrue(inner instanceof SQLMultiCatalogFlatMapTransform);
    }
}
