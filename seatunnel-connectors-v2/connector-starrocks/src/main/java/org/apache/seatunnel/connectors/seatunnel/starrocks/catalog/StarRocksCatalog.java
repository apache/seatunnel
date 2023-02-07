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

package org.apache.seatunnel.connectors.seatunnel.starrocks.catalog;

import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.MySqlCatalog;

import java.sql.Connection;
import java.sql.DriverManager;

public class StarRocksCatalog extends MySqlCatalog {

    public StarRocksCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
    }

    public StarRocksCatalog(String catalogName, String username, String pwd, String defaultUrl) {
        super(catalogName, username, pwd, defaultUrl);
    }

    public void createTable(String sql) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        try (Connection conn = DriverManager.getConnection(baseUrl + getDefaultDatabase(), username, pwd)) {
            conn.createStatement().execute(sql);
        } catch (Exception e) {
            throw new CatalogException(
                String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }
}
