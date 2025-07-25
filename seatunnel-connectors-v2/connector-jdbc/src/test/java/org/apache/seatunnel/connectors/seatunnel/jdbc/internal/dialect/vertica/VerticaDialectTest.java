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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.vertica;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

public class VerticaDialectTest {

    @Test
    void testUpsertStatementByTableSchema() {
        final String dataBaseName = "test_database";
        final String tableName = "test_table";
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id",
                                        BasicType.LONG_TYPE,
                                        22L,
                                        0,
                                        false,
                                        null,
                                        "id",
                                        "BIGINT",
                                        new HashMap<>()))
                        .column(
                                PhysicalColumn.of(
                                        "name",
                                        BasicType.STRING_TYPE,
                                        128L,
                                        0,
                                        false,
                                        null,
                                        "name",
                                        "VARCHAR",
                                        new HashMap<>()))
                        .column(
                                PhysicalColumn.of(
                                        "age",
                                        BasicType.INT_TYPE,
                                        (Long) null,
                                        0,
                                        true,
                                        null,
                                        "age",
                                        "INT",
                                        new HashMap<>()))
                        .column(
                                PhysicalColumn.of(
                                        "createTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3L,
                                        0,
                                        true,
                                        null,
                                        "createTime",
                                        "TIME",
                                        new HashMap<>()))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .constraintKey(
                                Collections.singletonList(
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.INDEX_KEY,
                                                "name",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "name", null)))))
                        .build();

        VerticaDialect dialect = new VerticaDialect();
        final String[] doUpdateKeyFields = {"id"};
        final String[] doNothingKeyFields = {"id", "name", "age"};

        String doUpdateSql =
                dialect.getUpsertStatementByTableSchema(
                                dataBaseName, tableName, tableSchema, doUpdateKeyFields)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Expected doUpdateSql String to be present"));
        Assertions.assertEquals(
                doUpdateSql,
                " MERGE INTO test_database.\"test_table\" TARGET USING (SELECT CAST(:id AS BIGINT) AS \"id\", CAST(:name AS VARCHAR) AS \"name\", CAST(:age AS INT) AS \"age\", CAST(:createTime AS TIME) AS \"createTime\" ) SOURCE ON (TARGET.\"id\"=SOURCE.\"id\")  WHEN MATCHED THEN UPDATE SET \"name\"=SOURCE.\"name\", \"age\"=SOURCE.\"age\", \"createTime\"=SOURCE.\"createTime\" WHEN NOT MATCHED THEN INSERT (\"id\", \"name\", \"age\", \"createTime\") VALUES (SOURCE.\"id\", SOURCE.\"name\", SOURCE.\"age\", SOURCE.\"createTime\")");

        String upsertCreateTimeSQL =
                dialect.getUpsertStatementByTableSchema(
                                dataBaseName, tableName, tableSchema, doNothingKeyFields)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Expected doNothingSql String to be present"));
        Assertions.assertEquals(
                upsertCreateTimeSQL,
                " MERGE INTO test_database.\"test_table\" TARGET USING (SELECT CAST(:id AS BIGINT) AS \"id\", CAST(:name AS VARCHAR) AS \"name\", CAST(:age AS INT) AS \"age\", CAST(:createTime AS TIME) AS \"createTime\" ) SOURCE ON (TARGET.\"id\"=SOURCE.\"id\" AND TARGET.\"name\"=SOURCE.\"name\" AND TARGET.\"age\"=SOURCE.\"age\")  WHEN MATCHED THEN UPDATE SET \"createTime\"=SOURCE.\"createTime\" WHEN NOT MATCHED THEN INSERT (\"id\", \"name\", \"age\", \"createTime\") VALUES (SOURCE.\"id\", SOURCE.\"name\", SOURCE.\"age\", SOURCE.\"createTime\")");
    }
}
