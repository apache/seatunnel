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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink.file;

import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.DistributedEngine;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class TimeplusTable implements Serializable {

    private String database;
    private String tableName;
    private String engine;
    private String engineFull;
    private String createTableDDL;
    private List<String> dataPaths;
    private String sortingKey;
    private final DistributedEngine distributedEngine;
    private Map<String, String> tableSchema;

    public TimeplusTable(
            String database,
            String tableName,
            DistributedEngine distributedEngine,
            String engine,
            String createTableDDL,
            String engineFull,
            List<String> dataPaths,
            String sortingKey,
            Map<String, String> tableSchema) {
        this.database = database;
        this.tableName = tableName;
        this.distributedEngine = distributedEngine;
        this.engine = engine;
        this.engineFull = engineFull;
        this.createTableDDL = createTableDDL;
        this.dataPaths = dataPaths;
        this.sortingKey = sortingKey;
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
