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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.TestTemplate;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MysqlCDCIT extends AbstractMysqlCDCITBase {

    public MysqlCDCIT() {
        // Initialize the container
        this.MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);
        this.inventoryDatabase =
                new UniqueDatabase(
                        MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SEATUNNEL, EngineType.FLINK},
            disabledReason = "Covered by testMysqlCdcCheckDataE2e")
    public void testMysqlCdcCheckDataE2eOnSpark(TestContainer container) {
        checkSnapshotThenIncrementalData(container, "/mysqlcdc_to_mysql_on_spark.conf");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SEATUNNEL, EngineType.FLINK},
            disabledReason =
                    "Covers Spark running all readers of a parallel CDC source in one task, "
                            + "where the snapshot phase ends without an engine checkpoint")
    public void testMysqlCdcCheckDataE2eWithParallelismOnSpark(TestContainer container) {
        checkSnapshotThenIncrementalData(
                container, "/mysqlcdc_to_mysql_with_parallelism_on_spark.conf");
    }
}
