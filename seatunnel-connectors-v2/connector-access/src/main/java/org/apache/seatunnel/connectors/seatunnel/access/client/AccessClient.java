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

package org.apache.seatunnel.connectors.seatunnel.access.client;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class AccessClient implements Serializable {
    private String driver;
    private String url;
    private String username;
    private String password;
    private String query;
    private Connection connection;

    public AccessClient(String driver, String url, String username, String password, String query) {
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
        this.query = query;
    }

    public Connection getAccessConnection(String url, String username, String password) {
        try {
            Class.forName(driver);
            this.connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return this.connection;
    }

    public ResultSetMetaData selectMetaData() throws Exception {
        connection = this.getAccessConnection(url, username, password);
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(query);
        ResultSetMetaData metaData = result.getMetaData();
        statement.close();
        return metaData;
    }

    public ResultSetMetaData getTableSchema(String tableName) throws Exception {
        connection = this.getAccessConnection(url, username, password);
        Statement statement = connection.createStatement();
        ResultSet result =
                statement.executeQuery(String.format("select * from %s limit 1", tableName));
        ResultSetMetaData metaData = result.getMetaData();
        statement.close();
        return metaData;
    }
}
