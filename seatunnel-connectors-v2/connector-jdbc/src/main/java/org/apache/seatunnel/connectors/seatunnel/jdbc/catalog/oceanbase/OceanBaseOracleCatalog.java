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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oceanbase;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalog;

import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

public class OceanBaseOracleCatalog extends OracleCatalog {

    static {
        SYS_DATABASES.clear();
        SYS_DATABASES.add("information_schema");
        SYS_DATABASES.add("mysql");
        SYS_DATABASES.add("oceanbase");
        SYS_DATABASES.add("LBACSYS");
        SYS_DATABASES.add("ORAAUDITOR");
        SYS_DATABASES.add("SYS");
    }

    public OceanBaseOracleCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getListDatabaseSql() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        String schemaName = rs.getString(1);
        String tableName = rs.getString(2);
        if (StringUtils.isNotBlank(schemaName) && !SYS_DATABASES.contains(schemaName)) {
            return schemaName + "." + tableName;
        }
        return null;
    }
}
