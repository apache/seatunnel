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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ClickhouseUtilTest {
    @Test
    public void testExtractTablePathFromSqlWithSimpleQuery() {
        String sql1 = "SELECT * FROM my_db.my_table";
        TablePath result = ClickhouseUtil.extractTablePathFromSql(sql1);
        Assertions.assertNotNull(result);
        Assertions.assertEquals("my_db", result.getDatabaseName());
        Assertions.assertEquals("my_table", result.getTableName());

        String sql2 = "SELECT id, name FROM my_db.my_table WHERE id > 100";
        TablePath result2 = ClickhouseUtil.extractTablePathFromSql(sql2);
        Assertions.assertNotNull(result2);
        Assertions.assertEquals("my_db", result2.getDatabaseName());
        Assertions.assertEquals("my_table", result2.getTableName());

        String sql3 = "SELECT t.id, t.name FROM my_db.my_table AS t WHERE t.id > 100";
        TablePath result3 = ClickhouseUtil.extractTablePathFromSql(sql3);
        Assertions.assertNotNull(result);
        Assertions.assertEquals("my_db", result3.getDatabaseName());
        Assertions.assertEquals("my_table", result3.getTableName());

        String sql4 =
                "SELECT * FROM my_db.my_table global join my_db2.my_table2 ON my_db.my_table.id = my_db2.my_table2.id";
        TablePath result4 = ClickhouseUtil.extractTablePathFromSql(sql4);
        Assertions.assertNotNull(result);
        Assertions.assertEquals("default", result4.getDatabaseName());
        Assertions.assertEquals("default", result4.getTableName());
    }
}
