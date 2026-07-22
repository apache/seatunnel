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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.reader;

import org.testcontainers.containers.JdbcDatabaseContainer;

/** Minimal Testcontainers wrapper around Vitess vttestserver. */
public class VitessContainer extends JdbcDatabaseContainer<VitessContainer> {

    public static final String IMAGE = "vitess/vttestserver";
    public static final String DEFAULT_TAG = "v17.0.2-mysql80";
    public static final Integer VITESS_PORT = 15991;
    public static final Integer GRPC_PORT = VITESS_PORT + 1;
    public static final Integer MYSQL_PORT = VITESS_PORT + 3;

    private String keyspace = "test";

    public VitessContainer() {
        this(DEFAULT_TAG);
    }

    public VitessContainer(String tag) {
        super(IMAGE + ":" + tag);
    }

    @Override
    protected void configure() {
        addEnv("PORT", VITESS_PORT.toString());
        addEnv("KEYSPACES", keyspace);
        addEnv("NUM_SHARDS", "1");
        addEnv("MYSQL_BIND_HOST", "0.0.0.0");
    }

    @Override
    public String getDriverClassName() {
        return "com.mysql.cj.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:mysql://" + getHost() + ":" + getMysqlPort() + "/" + keyspace;
    }

    @Override
    public String getUsername() {
        return "";
    }

    @Override
    public String getPassword() {
        return "";
    }

    public Integer getGrpcPort() {
        return getMappedPort(GRPC_PORT);
    }

    public Integer getMysqlPort() {
        return getMappedPort(MYSQL_PORT);
    }

    public String getKeyspace() {
        return keyspace;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    public VitessContainer withKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }
}
