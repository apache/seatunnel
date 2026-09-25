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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.source;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.Collections;

class Db2IncrementalSourceFactoryTest {
    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new Db2IncrementalSourceFactory()).optionRule());
    }

    /** Verifies Db2 checkpoint tables use the empty-catalog identifier returned by discovery. */
    @Test
    public void testToTableIdDropsCatalog() {
        Db2SourceConfigFactory configFactory = Mockito.mock(Db2SourceConfigFactory.class);
        Mockito.when(configFactory.create(0)).thenReturn(Mockito.mock(Db2SourceConfig.class));
        Db2Dialect dialect = new Db2Dialect(configFactory, Collections.emptyList());

        Assertions.assertEquals(
                new TableId("", "DB2INST1", "CUSTOMERS"),
                dialect.toTableId(TablePath.of("SAMPLE", "DB2INST1", "CUSTOMERS")));
    }
}
