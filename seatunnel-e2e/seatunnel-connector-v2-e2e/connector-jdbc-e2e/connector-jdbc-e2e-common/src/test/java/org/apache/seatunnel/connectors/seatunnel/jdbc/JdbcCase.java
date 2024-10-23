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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.lang3.tuple.Pair;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Builder
@Setter
@Getter
public class JdbcCase {
    private String dockerImage;
    private String networkAliases;
    private String driverClass;
    private String host;
    private String userName;
    private String password;
    private int port;
    private int localPort;
    private String database;
    private String schema;
    private String sourceTable;
    private String sinkTable;
    private String jdbcTemplate;
    private String jdbcUrl;
    private String createSql;
    private String sinkCreateSql;
    private String additionalSqlOnSource;
    private String additionalSqlOnSink;
    private String insertSql;
    private List<String> configFile;
    private Pair<String[], List<SeaTunnelRow>> testData;
    private Map<String, String> containerEnv;
    private boolean useSaveModeCreateTable;

    private String catalogDatabase;
    private String catalogSchema;
    private String catalogTable;

    // The full path of the table created when initializing data
    // According to whether jdbc api supports setting
    private String tablePathFullName;
}
