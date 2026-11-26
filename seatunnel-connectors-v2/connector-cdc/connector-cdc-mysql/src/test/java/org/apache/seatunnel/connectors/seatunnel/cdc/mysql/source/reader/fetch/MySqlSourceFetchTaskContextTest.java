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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch;

import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.TableId;

import java.util.Arrays;
import java.util.Collections;

class MySqlSourceFetchTaskContextTest {

    @Test
    void shouldScopeTableIncludeListToIncrementalSplitTables() {
        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        factory.hostname("localhost");
        factory.port(3306);
        factory.username("user");
        factory.password("pass");
        factory.databaseList("mysql_cdc");
        factory.tableList("mysql_cdc.t1", "mysql_cdc.t2", "mysql_cdc.t3", "mysql_cdc.t4");
        MySqlSourceConfig sourceConfig = factory.create(0);

        TableId t1 = TableId.parse("mysql_cdc.t1");
        TableId t2 = TableId.parse("mysql_cdc.t2");
        TableId t3 = TableId.parse("mysql_cdc.t3");
        TableId t4 = TableId.parse("mysql_cdc.t4");
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Arrays.asList(t1, t3),
                        BinlogOffset.INITIAL_OFFSET,
                        BinlogOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList());

        MySqlConnectorConfig resolved =
                MySqlSourceFetchTaskContext.resolveConnectorConfig(sourceConfig, split);

        Assertions.assertTrue(resolved.getTableFilters().dataCollectionFilter().isIncluded(t1));
        Assertions.assertFalse(resolved.getTableFilters().dataCollectionFilter().isIncluded(t2));
        Assertions.assertTrue(resolved.getTableFilters().dataCollectionFilter().isIncluded(t3));
        Assertions.assertFalse(resolved.getTableFilters().dataCollectionFilter().isIncluded(t4));
    }
}
