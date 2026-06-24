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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.FactoryUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class SqlServerIncrementalSourceFactoryTest {
    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new SqlServerIncrementalSourceFactory()).optionRule());
    }

    /**
     * SQLServer CDC source creation must accept the driver-specific databaseName URL syntax during
     * the submission-time catalog validation step.
     */
    @Test
    public void testCreateOptionalCatalogWithSqlServerStyleUrl() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:sqlserver://localhost:1433;databaseName=seatunnel");
        config.put("username", "sa");
        config.put("password", "Password!");
        config.put("database-names", Arrays.asList("seatunnel"));
        config.put("table-names", Arrays.asList("seatunnel.dbo.orders"));

        Optional<Catalog> catalog =
                FactoryUtil.createOptionalCatalog(
                        "SqlServer",
                        ReadonlyConfig.fromMap(config),
                        Thread.currentThread().getContextClassLoader(),
                        "SqlServer");

        Assertions.assertTrue(catalog.isPresent());
        catalog.ifPresent(Catalog::close);
    }
}
