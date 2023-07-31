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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.GenericContainer;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

@Disabled("Oracle mode of OceanBase Enterprise Edition does not provide docker environment")
public class JdbcOceanBaseOracleIT extends JdbcOceanBaseITBase {

    @Override
    String imageName() {
        return null;
    }

    @Override
    String host() {
        return "e2e_oceanbase_oracle";
    }

    @Override
    int port() {
        return 2883;
    }

    @Override
    String username() {
        return "root";
    }

    @Override
    String password() {
        return "";
    }

    @Override
    List<String> configFile() {
        return Lists.newArrayList("/jdbc_oceanbase_oracle_source_and_sink.conf");
    }

    @Override
    GenericContainer<?> initContainer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void startUp() {
        jdbcCase = getJdbcCase();

        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(() -> this.initializeJdbcConnection(jdbcCase.getJdbcUrl()));

        createSchemaIfNeeded();
        createNeededTables();
        insertTestData();
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    String createSqlTemplate() {
        return "create table %s\n"
                + "(\n"
                + "    VARCHAR_10_COL                varchar2(10),\n"
                + "    CHAR_10_COL                   char(10),\n"
                + "    CLOB_COL                      clob,\n"
                + "    NUMBER_3_SF_2_DP              number(3, 2),\n"
                + "    INTEGER_COL                   integer,\n"
                + "    FLOAT_COL                     float(10),\n"
                + "    REAL_COL                      real,\n"
                + "    BINARY_FLOAT_COL              binary_float,\n"
                + "    BINARY_DOUBLE_COL             binary_double,\n"
                + "    DATE_COL                      date,\n"
                + "    TIMESTAMP_WITH_3_FRAC_SEC_COL timestamp(3),\n"
                + "    TIMESTAMP_WITH_LOCAL_TZ       timestamp with local time zone\n"
                + ")";
    }

    @Override
    String[] getFieldNames() {
        return new String[] {
            "VARCHAR_10_COL",
            "CHAR_10_COL",
            "CLOB_COL",
            "NUMBER_3_SF_2_DP",
            "INTEGER_COL",
            "FLOAT_COL",
            "REAL_COL",
            "BINARY_FLOAT_COL",
            "BINARY_DOUBLE_COL",
            "DATE_COL",
            "TIMESTAMP_WITH_3_FRAC_SEC_COL",
            "TIMESTAMP_WITH_LOCAL_TZ"
        };
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames = getFieldNames();

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                String.format("f%s", i),
                                String.format("f%s", i),
                                String.format("f%s", i),
                                BigDecimal.valueOf(1.1),
                                i,
                                Float.parseFloat("2.2"),
                                Float.parseFloat("2.2"),
                                Float.parseFloat("22.2"),
                                Double.parseDouble("2.2"),
                                Date.valueOf(LocalDate.now()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                Timestamp.valueOf(LocalDateTime.now())
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }
}
