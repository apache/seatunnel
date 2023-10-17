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

package org.apache.seatunnel.connectors.doris.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Disabled
class DorisCatalogFactoryTest {

    private static DorisCatalogFactory factory = null;

    @BeforeAll
    static void beforeAll() {
        if (factory == null) {
            factory = new DorisCatalogFactory();
        }
    }

    @Test
    void createCatalog() {

        Map<String, Object> map = new HashMap<>();
        map.put("fe-hosts", "10.16.10.6");
        map.put("query-port", 9939);
        map.put("http-port", 8939);
        map.put("username", "root");
        map.put("password", "");

        try (Catalog catalog = factory.createCatalog("doris", ReadonlyConfig.fromMap(map))) {
            catalog.open();
            Assertions.assertEquals(catalog.getFactory(), Optional.empty());
        }
    }

    @Test
    void factoryIdentifier() {
        Assertions.assertEquals(factory.factoryIdentifier(), "Doris");
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull(factory.optionRule());
    }
}
