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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

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

    public void buildWithConfig(ReadonlyConfig config) {
        this.host = config.get(CassandraBaseOptions.HOST);
        this.keyspace = config.get(CassandraBaseOptions.KEYSPACE);
        this.username = config.get(CassandraBaseOptions.USERNAME);
        this.password = config.get(CassandraBaseOptions.PASSWORD);
        this.datacenter = config.get(CassandraBaseOptions.DATACENTER);
        this.table = config.get(CassandraSinkOptions.TABLE);
        this.cql = config.get(CassandraSourceOptions.CQL);
        this.fields = config.get(CassandraSinkOptions.FIELDS);
        this.consistencyLevel =
                DefaultConsistencyLevel.valueOf(config.get(CassandraBaseOptions.CONSISTENCY_LEVEL));
        this.batchSize = config.get(CassandraSinkOptions.BATCH_SIZE);
        this.batchType = DefaultBatchType.valueOf(config.get(CassandraSinkOptions.BATCH_TYPE));
        this.asyncWrite = config.get(CassandraSinkOptions.ASYNC_WRITE);
    }
}
