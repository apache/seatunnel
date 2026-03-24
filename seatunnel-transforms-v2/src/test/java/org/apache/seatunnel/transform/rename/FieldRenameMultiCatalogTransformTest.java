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

package org.apache.seatunnel.transform.rename;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.IdentityMapTransform;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

class FieldRenameMultiCatalogTransformTest {

    @Test
    void testCreateIdentityTransform() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable("test", "test", "test", "test", rowType);
        List<CatalogTable> tables = Collections.singletonList(catalogTable);
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                TransformCommonOptions.TABLE_MATCH_REGEX.key(), ".exclude"));

        TestRenameMultiCatalogTransform transform =
                new TestRenameMultiCatalogTransform(tables, config);

        Assertions.assertInstanceOf(
                IdentityMapTransform.class,
                transform
                        .getTransformMap()
                        .get(tables.get(0).getTableId().toTablePath().toString()));
    }

    private static class TestRenameMultiCatalogTransform extends FieldRenameMultiCatalogTransform {

        private TestRenameMultiCatalogTransform(
                List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
            super(inputCatalogTables, config);
        }

        private Map<String, SeaTunnelTransform<SeaTunnelRow>> getTransformMap() {
            return this.transformMap;
        }
    }
}
