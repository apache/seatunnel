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

package org.apache.seatunnel.connectors.seatunnel.common.sql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

class SqlTableClauseMergerTest {

    @Test
    void mergeOverridesExistingProperty() {
        String sql =
                "CREATE TABLE t (\n"
                        + " id INT\n"
                        + ") ENGINE=OLAP\n"
                        + "PROPERTIES (\n"
                        + "    \"replication_num\" = \"1\"\n"
                        + ")";
        Map<String, String> tableOptions = new LinkedHashMap<>();
        tableOptions.put("replication_num", "3");

        String merged =
                SqlTableClauseMerger.merge(
                        sql, ClauseMergeFormat.DOUBLE_QUOTED_PROPERTIES, tableOptions);

        Assertions.assertTrue(merged.contains("\"replication_num\" = \"3\""));
        Assertions.assertFalse(merged.contains("\"replication_num\" = \"1\""));
    }

    @Test
    void mergeAddsNewProperty() {
        String sql =
                "CREATE TABLE t (\n"
                        + " id INT\n"
                        + ") ENGINE=OLAP\n"
                        + "PROPERTIES (\n"
                        + "    \"replication_num\" = \"1\"\n"
                        + ")";
        Map<String, String> tableOptions = new LinkedHashMap<>();
        tableOptions.put("storage_format", "V2");

        String merged =
                SqlTableClauseMerger.merge(
                        sql, ClauseMergeFormat.DOUBLE_QUOTED_PROPERTIES, tableOptions);

        Assertions.assertTrue(merged.contains("\"replication_num\" = \"1\""));
        Assertions.assertTrue(merged.contains("\"storage_format\" = \"V2\""));
    }

    @Test
    void appendPropertiesWhenClauseMissing() {
        String sql = "CREATE TABLE t (id INT) ENGINE=OLAP";
        Map<String, String> tableOptions = new LinkedHashMap<>();
        tableOptions.put("replication_num", "3");

        String merged =
                SqlTableClauseMerger.merge(
                        sql, ClauseMergeFormat.DOUBLE_QUOTED_PROPERTIES, tableOptions);

        Assertions.assertTrue(merged.contains("PROPERTIES ("));
        Assertions.assertTrue(merged.contains("\"replication_num\" = \"3\""));
    }

    @Test
    void emptyOptionsReturnsOriginalSql() {
        String sql = "CREATE TABLE t (id INT) ENGINE=OLAP";
        Assertions.assertEquals(
                sql,
                SqlTableClauseMerger.merge(
                        sql, ClauseMergeFormat.DOUBLE_QUOTED_PROPERTIES, new LinkedHashMap<>()));
    }
}
