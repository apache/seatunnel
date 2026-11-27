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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;

import javax.annotation.Nullable;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_BLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_CLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_NCLOB;

/** Oracle converter for generated DML, retaining the existing streaming non-null LOB handling. */
public class OracleDmlRowConverter extends OracleJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    /**
     * Matches short stream bind families so sparse LOB nulls do not split driver batches. Generated
     * DML supplies the target column type; custom SQL continues using the original typed LOB nulls.
     */
    @Override
    protected void setNullToStatementByDataType(
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        if (seaTunnelDataType.getSqlType() == SqlType.STRING) {
            if (ORACLE_CLOB.equals(sourceType)) {
                statement.setNull(statementIndex, Types.VARCHAR);
                return;
            }
            if (ORACLE_NCLOB.equals(sourceType)) {
                // setNString also establishes the national-character form for null-first batches.
                statement.setNString(statementIndex, null);
                return;
            }
        } else if (seaTunnelDataType.getSqlType() == SqlType.BYTES
                && ORACLE_BLOB.equals(sourceType)) {
            statement.setBytes(statementIndex, null);
            return;
        }
        super.setNullToStatementByDataType(
                statement, seaTunnelDataType, statementIndex, sourceType);
    }
}
