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
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Setter
@Getter
public class CassandraParameters implements Serializable {
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

    public void buildWithConfig(Config config) {
        this.host = config.getString(CassandraConfig.HOST.key());
        this.keyspace = config.getString(CassandraConfig.KEYSPACE.key());

        if (config.hasPath(CassandraConfig.USERNAME.key())) {
            this.username = config.getString(CassandraConfig.USERNAME.key());
        }
        if (config.hasPath(CassandraConfig.PASSWORD.key())) {
            this.password = config.getString(CassandraConfig.PASSWORD.key());
        }
        if (config.hasPath(CassandraConfig.DATACENTER.key())) {
            this.datacenter = config.getString(CassandraConfig.DATACENTER.key());
        } else {
            this.datacenter = CassandraConfig.DATACENTER.defaultValue();
        }
        if (config.hasPath(CassandraConfig.TABLE.key())) {
            this.table = config.getString(CassandraConfig.TABLE.key());
        }
        if (config.hasPath(CassandraConfig.CQL.key())) {
            this.cql = config.getString(CassandraConfig.CQL.key());
        }
        if (config.hasPath(CassandraConfig.FIELDS.key())) {
            this.fields = config.getStringList(CassandraConfig.FIELDS.key());
        }
        if (config.hasPath(CassandraConfig.CONSISTENCY_LEVEL.key())) {
            this.consistencyLevel =
                    DefaultConsistencyLevel.valueOf(
                            config.getString(CassandraConfig.CONSISTENCY_LEVEL.key()));
        } else {
            this.consistencyLevel =
                    DefaultConsistencyLevel.valueOf(
                            CassandraConfig.CONSISTENCY_LEVEL.defaultValue());
        }
        if (config.hasPath(CassandraConfig.BATCH_SIZE.key())) {
            this.batchSize = config.getInt(CassandraConfig.BATCH_SIZE.key());
        } else {
            this.batchSize = CassandraConfig.BATCH_SIZE.defaultValue();
        }
        if (config.hasPath(CassandraConfig.BATCH_TYPE.key())) {
            this.batchType =
                    DefaultBatchType.valueOf(config.getString(CassandraConfig.BATCH_TYPE.key()));
        } else {
            this.batchType = DefaultBatchType.valueOf(CassandraConfig.BATCH_TYPE.defaultValue());
        }
        if (config.hasPath(CassandraConfig.ASYNC_WRITE.key())) {
            this.asyncWrite = config.getBoolean(CassandraConfig.ASYNC_WRITE.key());
        } else {
            this.asyncWrite = true;
        }
    }
}
