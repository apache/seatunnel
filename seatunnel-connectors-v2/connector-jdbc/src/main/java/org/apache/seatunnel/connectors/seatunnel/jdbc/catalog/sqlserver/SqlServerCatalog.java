/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;

import org.apache.commons.lang3.tuple.Pair;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SqlServerCatalog extends AbstractJdbcCatalog {

    static {
        SYS_DATABASES.add("master");
        SYS_DATABASES.add("tempdb");
        SYS_DATABASES.add("model");
        SYS_DATABASES.add("msdb");
    }

    public SqlServerCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {
        super(catalogName, username, pwd, urlInfo);
    }

    @Override
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        throw new UnsupportedOperationException("Unsupported create table");
    }

    @Override
    public SeaTunnelDataType<?> fromJdbcType(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        Pair<SqlServerType, Map<String, Object>> pair =
                SqlServerType.parse(metadata.getColumnTypeName(colIndex));
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(
                SqlServerDataTypeConvertor.PRECISION, metadata.getPrecision(colIndex));
        dataTypeProperties.put(SqlServerDataTypeConvertor.SCALE, metadata.getScale(colIndex));
        return new SqlServerDataTypeConvertor().toSeaTunnelType(pair.getLeft(), dataTypeProperties);
    }
}
