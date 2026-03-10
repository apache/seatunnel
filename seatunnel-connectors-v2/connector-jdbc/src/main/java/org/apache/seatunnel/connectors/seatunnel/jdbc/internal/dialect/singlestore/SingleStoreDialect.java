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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.singlestore;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlJdbcRowConverter;

/**
 * JDBC dialect for SingleStore (formerly MemSQL), a high-performance real-time analytical database.
 *
 * <p>SingleStore is designed to be MySQL-compatible. This dialect extends {@link MysqlDialect} and
 * reuses the following MySQL behaviors without modification. Compatibility has been validated for
 * the connector's use cases; if you use Schema Evolution or other advanced DDL, validate against
 * your SingleStore version.
 *
 * <ul>
 *   <li>Type mapping and {@link
 *       org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter} –
 *       delegated to the MySQL implementation while reporting SingleStore in {@code converterName}
 *       for clearer error messages.
 *   <li>Upsert syntax – {@code INSERT ... ON DUPLICATE KEY UPDATE} (see {@link
 *       MysqlDialect#getUpsertStatement(String, String, String[], String[])}).
 *   <li>Split / sampling – {@code SHOW TABLE STATUS}, {@code CRC32} for hash-based split (see
 *       {@link MysqlDialect#approximateRowCntStatement} and {@link MysqlDialect#hashModForField}).
 *   <li>Batch – {@code rewriteBatchedStatements=true} (see {@link MysqlDialect#defaultParameter}).
 *   <li>Schema change (DDL) – ALTER TABLE ADD/MODIFY/DROP COLUMN logic is inherited from MySQL;
 *       behavior on SingleStore should be validated if you use Schema Evolution.
 * </ul>
 *
 * @see MysqlDialect
 * @see org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlJdbcRowConverter
 * @see org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter
 */
public class SingleStoreDialect extends MysqlDialect {

    public SingleStoreDialect() {}

    /**
     * @param fieldIde field identifier mode (e.g. LOWERCASE, UPPERCASE, ORIGINAL); null or empty is
     *     treated as original by the base dialect.
     */
    public SingleStoreDialect(String fieldIde) {
        super(fieldIde);
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new SingleStoreJdbcRowConverter();
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.SINGLESTORE;
    }

    /**
     * SingleStore-specific JDBC row converter. Extends {@link MysqlJdbcRowConverter} for MySQL
     * compatibility while reporting SingleStore in converter name for clearer error messages.
     */
    private static class SingleStoreJdbcRowConverter extends MysqlJdbcRowConverter {
        @Override
        public String converterName() {
            return DatabaseIdentifier.SINGLESTORE;
        }
    }
}
