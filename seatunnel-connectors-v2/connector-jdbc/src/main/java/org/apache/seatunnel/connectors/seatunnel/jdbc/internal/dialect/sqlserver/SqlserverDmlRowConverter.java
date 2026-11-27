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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver;

import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import javax.annotation.Nullable;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/** Preserves decimal parameter scales in generated SQL Server DML batches. */
public final class SqlserverDmlRowConverter extends SqlserverJdbcRowConverter {

    /**
     * A scale-zero null changes the driver's parameter declaration and fragments decimal batches.
     * Custom SQL keeps the original converter because expressions can depend on parameter types.
     */
    @Override
    protected void setNullToStatementByDataType(
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        // Only known decimal targets can safely accept a typed decimal null. In particular,
        // an untyped null can reach DATE columns where a DECIMAL null is rejected.
        boolean decimalTarget =
                sourceType != null
                        && (sourceType.equals(SqlServerTypeConverter.SQLSERVER_DECIMAL)
                                || sourceType.startsWith(
                                        SqlServerTypeConverter.SQLSERVER_DECIMAL + "(")
                                || sourceType.equals(SqlServerTypeConverter.SQLSERVER_NUMERIC)
                                || sourceType.startsWith(
                                        SqlServerTypeConverter.SQLSERVER_NUMERIC + "("));
        if (decimalTarget && seaTunnelDataType instanceof DecimalType) {
            int scale = ((DecimalType) seaTunnelDataType).getScale();
            // Logical types can carry scales outside SQL Server's supported range.
            if (scale >= 0 && scale <= SqlServerTypeConverter.MAX_PRECISION) {
                statement.setObject(statementIndex, null, Types.DECIMAL, scale);
                return;
            }
        }
        super.setNullToStatementByDataType(
                statement, seaTunnelDataType, statementIndex, sourceType);
    }
}
