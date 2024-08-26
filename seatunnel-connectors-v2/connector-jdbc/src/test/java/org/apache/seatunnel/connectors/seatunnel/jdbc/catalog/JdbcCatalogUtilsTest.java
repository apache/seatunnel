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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Disabled("Configure your own environment")
public class JdbcCatalogUtilsTest {

    @Test
    public void testCatalogUtils() throws SQLException, ClassNotFoundException {
        List<JdbcSourceTableConfig> tablesConfig = new ArrayList<>();
        // JdbcSourceTableConfig(tablePath=null, query=select age, name,gender from `dbs`.`user`,
        // partitionColumn=null, partitionNumber=10, partitionStart=null, partitionEnd=null,
        // useSelectCount=false, skipAnalyze=false)
        JdbcSourceTableConfig tableConfig =
                JdbcSourceTableConfig.builder()
                        .query("SELECT age, user1.name AS name1, gender FROM user1 ; ")
                        .useSelectCount(false)
                        .build();
        tablesConfig.add(tableConfig);
        JdbcCatalogUtils.getTables(
                JdbcConnectionConfig.builder()
                        .url(
                                "jdbc:mysql://xxxx:3306/xxx?serverTimezone=Asia/Shanghai&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true")
                        .username("root")
                        .password("xxxxxx")
                        .build(),
                tablesConfig);
    }
}
