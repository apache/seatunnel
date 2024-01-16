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

package org.apache.seatunnel.connectors.seatunnel.starrocks.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Disabled("Please Test it in your local environment")
public class StarRocksCatalogTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StarRocksCatalogTest.class);

    @Test
    public void testCatalog() {
        StarRocksCatalog catalog =
                new StarRocksCatalog(
                        "starrocks", "root", "123456", "jdbc:mysql://47.108.65.163:9030/", "");
        List<String> databases = catalog.listDatabases();
        LOGGER.info("find databases: " + databases);

        if (!catalog.databaseExists("default")) {
            catalog.createDatabase(TablePath.of("default", null), true);
        }

        databases = catalog.listDatabases();
        LOGGER.info("find databases: " + databases);

        catalog.createTable(
                "CREATE TABLE IF NOT EXISTS `default`.`test` (\n"
                        + "`recruit_date`  DATE           NOT NULL COMMENT \"YYYY-MM-DD\",\n"
                        + "    `region_num`    TINYINT        COMMENT \"range [-128, 127]\",\n"
                        + "    `num_plate`     SMALLINT       COMMENT \"range [-32768, 32767] \",\n"
                        + "    `tel`           INT            COMMENT \"range [-2147483648, 2147483647]\",\n"
                        + "    `id`            BIGINT         COMMENT \"range [-2^63 + 1 ~ 2^63 - 1]\",\n"
                        + "    `password`      LARGEINT       COMMENT \"range [-2^127 + 1 ~ 2^127 - 1]\",\n"
                        + "    `name`          CHAR(20)       NOT NULL COMMENT \"range char(m),m in (1-255)\",\n"
                        + "    `profile`       VARCHAR(500)   NOT NULL COMMENT \"upper limit value 1048576 bytes\",\n"
                        + "    `hobby`         STRING         NOT NULL COMMENT \"upper limit value 65533 bytes\",\n"
                        + "    `leave_time`    DATETIME       COMMENT \"YYYY-MM-DD HH:MM:SS\",\n"
                        + "    `channel`       FLOAT          COMMENT \"4 bytes\",\n"
                        + "    `income`        DOUBLE         COMMENT \"8 bytes\",\n"
                        + "    `account`       DECIMAL(12,4)  COMMENT \"\",\n"
                        + "    `ispass`        BOOLEAN        COMMENT \"true/false\"\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`recruit_date`, `region_num`)\n"
                        + "PARTITION BY RANGE(`recruit_date`)\n"
                        + "(\n"
                        + "    PARTITION p20220311 VALUES [('2022-03-11'), ('2022-03-12')),\n"
                        + "    PARTITION p20220312 VALUES [('2022-03-12'), ('2022-03-13')),\n"
                        + "    PARTITION p20220313 VALUES [('2022-03-13'), ('2022-03-14')),\n"
                        + "    PARTITION p20220314 VALUES [('2022-03-14'), ('2022-03-15')),\n"
                        + "    PARTITION p20220315 VALUES [('2022-03-15'), ('2022-03-16'))\n"
                        + ")"
                        + "      DISTRIBUTED BY HASH (`id`)\n"
                        + "      PROPERTIES (\n"
                        + "            \"replication_num\" = \"1\" \n"
                        + "      );");
        CatalogTable table = catalog.getTable(TablePath.of("default", "test"));
        LOGGER.info("find table: " + table);
    }
}
