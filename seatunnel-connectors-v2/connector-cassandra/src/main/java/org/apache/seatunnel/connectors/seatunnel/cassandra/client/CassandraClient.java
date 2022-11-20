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

package org.apache.seatunnel.connectors.seatunnel.cassandra.client;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorException;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.commons.lang3.StringUtils;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class CassandraClient {
    public static CqlSessionBuilder getCqlSessionBuilder(String nodeAddress, String keyspace, String username, String password, String dataCenter) {
        List<CqlSessionBuilder> cqlSessionBuilderList = Arrays.stream(nodeAddress.split(",")).map(address -> {
            String[] nodeAndPort = address.split(":", 2);
            if (StringUtils.isEmpty(username) && StringUtils.isEmpty(password)) {
                return CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(nodeAndPort[0], Integer.parseInt(nodeAndPort[1])))
                    .withKeyspace(keyspace)
                    .withLocalDatacenter(dataCenter);
            }
            return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(nodeAndPort[0], Integer.parseInt(nodeAndPort[1])))
                .withAuthCredentials(username, password)
                .withKeyspace(keyspace)
                .withLocalDatacenter(dataCenter);
        }).collect(Collectors.toList());
        return cqlSessionBuilderList.get(ThreadLocalRandom.current().nextInt(cqlSessionBuilderList.size()));
    }

    public static SimpleStatement createSimpleStatement(String cql, ConsistencyLevel consistencyLevel) {
        return SimpleStatement.builder(cql).setConsistencyLevel(consistencyLevel).build();
    }

    public static ColumnDefinitions getTableSchema(CqlSession session, String table) {
        try {
            return session.execute(String.format("select * from %s limit 1", table))
                .getColumnDefinitions();
        } catch (Exception e) {
            throw new CassandraConnectorException(CommonErrorCode.TABLE_SCHEMA_GET_FAILED,
                    "Cannot get table schema from cassandra", e);
        }
    }
}
