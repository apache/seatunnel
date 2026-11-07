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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.tidb;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TiDBCatalog extends MySqlCatalog {

    public TiDBCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String driverClass) {
        super(catalogName, username, pwd, urlInfo, driverClass);
    }

    /**
     * Keeps TiDB VECTOR columns unsupported, as they were before MySQL 9.0 VECTOR support.
     *
     * <p>{@link MySqlTypeConverter} now maps VECTOR to raw bytes in MySQL 9.0's little-endian float
     * layout. TiDB has its own VECTOR type whose wire format is not verified to match, so it must
     * keep failing fast here instead of silently delivering bytes of a different meaning.
     */
    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String dataType = resultSet.getString("DATA_TYPE");
        if (MySqlTypeConverter.MYSQL_VECTOR.equalsIgnoreCase(dataType)) {
            throw CommonError.convertToSeaTunnelTypeError(
                    DatabaseIdentifier.TIDB, dataType, resultSet.getString("COLUMN_NAME"));
        }
        return super.buildColumn(resultSet);
    }
}
