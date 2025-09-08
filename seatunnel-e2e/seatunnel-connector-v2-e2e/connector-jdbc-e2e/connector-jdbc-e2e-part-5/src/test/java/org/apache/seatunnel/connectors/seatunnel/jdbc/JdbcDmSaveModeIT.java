/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import java.util.List;

public class JdbcDmSaveModeIT extends JdbcDmIT {

    private static final String CREATE_SQL =
            "create table if not exists %s"
                    + "(\n"
                    + "    DM_BIT              BIT,\n"
                    + "    DM_INT              INT,\n"
                    + "    DM_INTEGER          INTEGER,\n"
                    + "    DM_PLS_INTEGER      PLS_INTEGER,\n"
                    + "    DM_TINYINT          TINYINT,\n"
                    + "\n"
                    + "    DM_BYTE             BYTE,\n"
                    + "    DM_SMALLINT         SMALLINT,\n"
                    + "    DM_BIGINT           BIGINT,\n"
                    + "\n"
                    + "    DM_NUMERIC          NUMERIC,\n"
                    + "    DM_NUMBER           NUMBER,\n"
                    + "    DM_DECIMAL          DECIMAL,\n"
                    + "    DM_DEC              DEC,\n"
                    + "\n"
                    + "    DM_REAL             REAL,\n"
                    + "    DM_FLOAT            FLOAT,\n"
                    + "    DM_DOUBLE_PRECISION DOUBLE PRECISION,\n"
                    + "    DM_DOUBLE           DOUBLE,\n"
                    + "\n"
                    + "    DM_CHAR             CHAR,\n"
                    + "    DM_CHARACTER        CHARACTER,\n"
                    + "    DM_VARCHAR          VARCHAR,\n"
                    + "    DM_VARCHAR2         VARCHAR2,\n"
                    + "    DM_TEXT             TEXT,\n"
                    + "    DM_LONG             LONG,\n"
                    + "    DM_LONGVARCHAR      LONGVARCHAR,\n"
                    + "    DM_CLOB             CLOB,\n"
                    + "\n"
                    + "    DM_TIMESTAMP        TIMESTAMP,\n"
                    + "    DM_DATETIME         DATETIME,\n"
                    + "    DM_DATE             DATE,\n"
                    + "\n"
                    + "    DM_BLOB             BLOB,\n"
                    + "    DM_BINARY           BINARY,\n"
                    + "    DM_VARBINARY        VARBINARY,\n"
                    + "    DM_LONGVARBINARY    LONGVARBINARY,\n"
                    + "    DM_IMAGE            IMAGE,\n"
                    + "    DM_BFILE            BFILE,\n"
                    + "    constraint PK_T_COL primary key (\"DM_INT\")"
                    + ")";

    private static final String DM_SINK = "e2e_table_sink1";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_dm_source_and_sink_savemode.conf");

    @Override
    JdbcCase getJdbcCase() {
        JdbcCase jdbcCase = super.getJdbcCase();
        jdbcCase.setUseSaveModeCreateTable(true);
        jdbcCase.setSinkTable(DM_SINK);
        jdbcCase.setConfigFile(CONFIG_FILE);
        jdbcCase.setCreateSql(CREATE_SQL);
        return jdbcCase;
    }
}
