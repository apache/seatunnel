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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file;

import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.DistributedEngine;

import java.util.List;
import java.util.Map;

public class ClickhouseTable {

    private String database;
    private String tableName;
    private String engine;
    private String engineFull;
    private String createTableDDL;
    private List<String> dataPaths;
    private final DistributedEngine distributedEngine;
    private Map<String, String> tableSchema;

    public ClickhouseTable(String database,
                           String tableName,
                           DistributedEngine distributedEngine,
                           String engine,
                           String createTableDDL,
                           String engineFull,
                           List<String> dataPaths,
                           Map<String, String> tableSchema) {
        this.database = database;
        this.tableName = tableName;
        this.distributedEngine = distributedEngine;
        this.engine = engine;
        this.engineFull = engineFull;
        this.createTableDDL = createTableDDL;
        this.dataPaths = dataPaths;
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getEngineFull() {
        return engineFull;
    }

    public void setEngineFull(String engineFull) {
        this.engineFull = engineFull;
    }

    public String getCreateTableDDL() {
        return createTableDDL;
    }

    public void setCreateTableDDL(String createTableDDL) {
        this.createTableDDL = createTableDDL;
    }

    public List<String> getDataPaths() {
        return dataPaths;
    }

    public void setDataPaths(List<String> dataPaths) {
        this.dataPaths = dataPaths;
    }

    public Map<String, String> getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(Map<String, String> tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getLocalTableName() {
        if (distributedEngine != null) {
            return distributedEngine.getTable();
        } else {
            return tableName;
        }
    }
}
