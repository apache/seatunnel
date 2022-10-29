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

package org.apache.seatunnel.connectors.seatunnel.cassandra.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Data
@ToString
@NoArgsConstructor
public class CassandraConfig implements Serializable {

    public static final String HOST = "host";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String DATACENTER = "datacenter";
    public static final String KEYSPACE = "keyspace";
    public static final String TABLE = "table";
    public static final String CQL = "cql";
    public static final String FIELDS = "fields";
    public static final String CONSISTENCY_LEVEL = "consistency_level";
    public static final String BATCH_SIZE = "batch_size";
    public static final String BATCH_TYPE = "batch_type";
    public static final String ASYNC_WRITE = "async_write";

    private String host;
    private String username;
    private String password;
    private String datacenter;
    private String keyspace;
    private String table;
    private String cql;
    private List<String> fields;
    private ConsistencyLevel consistencyLevel;
    private Integer batchSize;
    private DefaultBatchType batchType;
    private Boolean asyncWrite;

    public CassandraConfig(@NonNull String host, @NonNull String keyspace) {
        this.host = host;
        this.keyspace = keyspace;
    }

    public static CassandraConfig getCassandraConfig(Config config) {
        CassandraConfig cassandraConfig = new CassandraConfig(
            config.getString(HOST),
            config.getString(KEYSPACE)
        );
        if (config.hasPath(USERNAME)) {
            cassandraConfig.setUsername(config.getString(USERNAME));
        }
        if (config.hasPath(PASSWORD)) {
            cassandraConfig.setPassword(config.getString(PASSWORD));
        }
        if (config.hasPath(DATACENTER)) {
            cassandraConfig.setDatacenter(config.getString(DATACENTER));
        } else {
            cassandraConfig.setDatacenter("datacenter1");
        }
        if (config.hasPath(TABLE)) {
            cassandraConfig.setTable(config.getString(TABLE));
        }
        if (config.hasPath(CQL)) {
            cassandraConfig.setCql(config.getString(CQL));
        }
        if (config.hasPath(FIELDS)) {
            cassandraConfig.setFields(config.getStringList(FIELDS));
        }
        if (config.hasPath(CONSISTENCY_LEVEL)) {
            cassandraConfig.setConsistencyLevel(DefaultConsistencyLevel.valueOf(config.getString(CONSISTENCY_LEVEL)));
        } else {
            cassandraConfig.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE);
        }
        if (config.hasPath(BATCH_SIZE)) {
            cassandraConfig.setBatchSize(config.getInt(BATCH_SIZE));
        } else {
            cassandraConfig.setBatchSize(Integer.parseInt("5000"));
        }
        if (config.hasPath(BATCH_TYPE)) {
            cassandraConfig.setBatchType(DefaultBatchType.valueOf(config.getString(BATCH_TYPE)));
        } else {
            cassandraConfig.setBatchType(DefaultBatchType.UNLOGGED);
        }
        if (config.hasPath(ASYNC_WRITE)) {
            cassandraConfig.setAsyncWrite(config.getBoolean(ASYNC_WRITE));
        } else {
            cassandraConfig.setAsyncWrite(true);
        }
        return cassandraConfig;
    }
}
