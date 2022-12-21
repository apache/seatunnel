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

package io.debezium.connector.dameng;

import dm.jdbc.driver.DmDriver;
import io.debezium.config.Configuration;
import io.debezium.connector.dameng.logminer.LogContent;
import io.debezium.connector.dameng.logminer.LogFile;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@SuppressWarnings("MagicNumber")
public class DamengConnection extends JdbcConnection {
    private static final String URL_PATTERN = "jdbc:dm://${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}/${"
        + JdbcConfiguration.DATABASE + "}?useUnicode=true&characterEncoding=PG_UTF8";

    private static final JdbcConnection.ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(
        URL_PATTERN,
        DmDriver.class.getName(),
        DamengConnection.class.getClassLoader(),
        JdbcConfiguration.PORT.withDefault(DamengConnectorConfig.PORT.defaultValueAsString()));

    public DamengConnection(Configuration config) {
        this(config, FACTORY);
    }

    public DamengConnection(Configuration config,
                            ConnectionFactory connectionFactory) {
        super(config, connectionFactory);
    }

    @Override
    public DamengConnection connect() throws SQLException {
        return (DamengConnection) super.connect();
    }

    public Scn currentCheckpointLsn() throws SQLException {
        String selectCurrentCheckpointLsnSQL = "SELECT CKPT_LSN FROM V$RLOG";
        JdbcConnection.ResultSetMapper<Scn> mapper = rs -> {
            rs.next();
            return Scn.valueOf(rs.getBigDecimal(1));
        };
        return queryAndMap(selectCurrentCheckpointLsnSQL, mapper);
    }

    public Scn earliestLsn() throws SQLException {
        String selectEarliestLsnSQL = "SELECT NAME, FIRST_CHANGE#, NEXT_CHANGE#, SEQUENCE# " +
            "FROM V$ARCHIVED_LOG " +
            "WHERE NAME IS NOT NULL AND STATUS='A' " +
            "ORDER BY SEQUENCE# ASC LIMIT 1";
        JdbcConnection.ResultSetMapper<Scn> mapper = rs -> {
            rs.next();
            return Scn.valueOf(rs.getBigDecimal(2));
        };
        return queryAndMap(selectEarliestLsnSQL, mapper);
    }

    public List<TableId> listTables(RelationalTableFilters tableFilters) throws SQLException {
        String sql = "SELECT OWNER, TABLE_NAME FROM ALL_TABLES";
        JdbcConnection.ResultSetMapper<List<TableId>> mapper = rs -> {
            List<TableId> capturedTableIds = new ArrayList<>();
            while (rs.next()) {
                String owner = rs.getString(1);
                String table = rs.getString(2);

                TableId tableId = new TableId(null, owner, table);
                if (tableFilters == null || tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                    capturedTableIds.add(tableId);
                    log.info("\t including '{}' for further processing", tableId);
                } else {
                    log.info("\t '{}' is filtered out of capturing", tableId);
                }
            }
            return capturedTableIds;
        };

        return queryAndMap(sql, mapper);
    }

    public boolean isCaseSensitive() throws SQLException {
        String sql = "SELECT SF_GET_CASE_SENSITIVE_FLAG()";
        JdbcConnection.ResultSetMapper<Boolean> mapper = rs -> {
            rs.next();
            return rs.getInt(1) == 1;
        };
        return queryAndMap(sql, mapper);
    }

    public Scn getFirstScn(Scn firstChange, Scn nextChange) throws SQLException {
        String sql =
            "SELECT " +
            "   NAME, FIRST_CHANGE#, NEXT_CHANGE#, SEQUENCE# " +
            "FROM " +
                "(SELECT * FROM v$archived_log " +
                "WHERE FIRST_CHANGE# <= ? AND NEXT_CHANGE# >= ? " +
                "ORDER BY FIRST_CHANGE# DESC" +
                ") t " +
            "WHERE ROWNUM <= 1";
        StatementPreparer preparer = statement -> {
            statement.setBigDecimal(1, firstChange.bigDecimalValue());
            statement.setBigDecimal(2, nextChange.bigDecimalValue());
        };
        ResultSetMapper<Scn> mapper = rs -> {
            if (rs.next()) {
                return Scn.valueOf(rs.getBigDecimal(2));
            }
            return Scn.valueOf(0);
        };
        return prepareQueryAndMap(sql, preparer, mapper);
    }

    public Optional<LogFile> getLogFile(Scn firstChange, Scn nextChange) throws SQLException {
        String sql =
            "SELECT " +
            "   NAME, FIRST_CHANGE#, NEXT_CHANGE#, SEQUENCE# " +
            "FROM " +
                "(SELECT * FROM v$archived_log " +
                "WHERE (FIRST_CHANGE# >= ? AND NEXT_CHANGE# > ? AND NAME IS NOT NULL AND STATUS='A') " +
                "ORDER BY SEQUENCE#" +
                ") t " +
            "WHERE ROWNUM <= 1";
        StatementPreparer preparer = statement -> {
            statement.setBigDecimal(1, firstChange.bigDecimalValue());
            statement.setBigDecimal(2, nextChange.bigDecimalValue());
        };
        ResultSetMapper<Optional<LogFile>> mapper = rs -> {
            LogFile logFile = null;
            if (rs.next()) {
                logFile = LogFile.builder()
                    .name(rs.getString(1))
                    .firstScn(Scn.valueOf(rs.getBigDecimal(2)))
                    .nextScn(Scn.valueOf(rs.getBigDecimal(3)))
                    .sequence(rs.getLong(4))
                    .build();
            }
            return Optional.ofNullable(logFile);
        };
        return prepareQueryAndMap(sql, preparer, mapper);
    }

    public DamengConnection addLogFile(LogFile file) throws SQLException {
        String sql = "DBMS_LOGMNR.ADD_LOGFILE(logfilename=>'" + file.getName() + "', options=>DBMS_LOGMNR.ADDFILE)";
        execute(sql);
        return this;
    }

    public DamengConnection startLogMiner(Scn startScn) throws SQLException {
        execute("DBMS_LOGMNR.START_LOGMNR(STARTSCN => " + startScn.toString() + ", OPTIONS => 2130)");
        return this;
    }

    public DamengConnection endLogMiner() throws SQLException {
        execute("DBMS_LOGMNR.END_LOGMNR()");
        return this;
    }

    public DamengConnection readLogContent(Scn startScn,
                                           String[] schemas,
                                           String[] tables,
                                           Consumer<LogContent> consumer) throws SQLException {
        ResultSetMapper<LogContent> mapper = rs -> LogContent.builder()
            .scn(Scn.valueOf(rs.getBigDecimal(1)))
            .startScn(Optional.ofNullable(rs.getBigDecimal(2))
                .map(e -> Scn.valueOf(e))
                .orElse(null))
            .commitScn(Optional.ofNullable(rs.getBigDecimal(3))
                .map(e -> Scn.valueOf(e))
                .orElse(null))
            .timestamp(rs.getTimestamp(4))
            .startTimestamp(rs.getTimestamp(5))
            .commitTimestamp(rs.getTimestamp(6))
            .xid(rs.getString(7))
            .rollBack(rs.getBoolean(8))
            .operation(rs.getString(9))
            .operationCode(rs.getInt(10))
            .segOwner(rs.getString(11))
            .tableName(rs.getString(12))
            .sqlRedo(rs.getString(13))
            .sqlUndo(rs.getString(14))
            .ssn(rs.getInt(15))
            .csf(rs.getInt(16))
            .status(rs.getInt(17))
            .build();
        ResultSetConsumer resultSetConsumer = rs -> {
            while (rs.next()) {
                LogContent logContent = mapper.apply(rs);
                consumer.accept(logContent);
            }
        };
        return readLogContent(startScn, schemas, tables, resultSetConsumer);
    }

    private static String join(String[] strings,
                               String around,
                               String splitter) {
        return Arrays.stream(strings)
            .map(s -> around + s + around)
            .collect(Collectors.joining(splitter));
    }

    public DamengConnection readLogContent(Scn startScn,
                                           String[] schemas,
                                           String[] tables,
                                           ResultSetConsumer consumer) throws SQLException {
        String sql =
            "SELECT " +
                "   SCN, START_SCN, COMMIT_SCN, " +
                "   TIMESTAMP, START_TIMESTAMP, COMMIT_TIMESTAMP, " +
                "   XID, ROLL_BACK, OPERATION, OPERATION_CODE, SEG_OWNER, " +
                "   TABLE_NAME, SQL_REDO, SQL_UNDO, " +
                "   SSN, CSF, STATUS " +
                "FROM V$LOGMNR_CONTENTS " +
                "WHERE OPERATION != 'SELECT_FOR_UPDATE' " +
                "   AND SCN > ? " +
                "   AND (OPERATION = 'COMMIT' " +
                "       OR (OPERATION = 'ROLLBACK' AND PXID != '0000000000000000') " +
                "       OR (SEG_OWNER IN (%s) AND TABLE_NAME IN (%s)))";
        sql = String.format(sql,
            join(schemas, "'", ","),
            join(tables, "'", ","));

        StatementPreparer preparer = statement -> {
            statement.setFetchSize(500);
            statement.setBigDecimal(1, startScn.bigDecimalValue());
        };
        prepareQuery(sql, preparer, consumer);
        return this;
    }
}
