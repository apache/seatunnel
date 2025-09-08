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

package org.apache.seatunnel.connectors.seatunnel.cassandra.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraParameters;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions.ASYNC_WRITE;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions.BATCH_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions.CONSISTENCY_LEVEL;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions.DATACENTER;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions.FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions.HOST;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions.KEYSPACE;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions.USERNAME;

@AutoService(Factory.class)
public class CassandraSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Cassandra";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, KEYSPACE, TABLE)
                .bundled(USERNAME, PASSWORD)
                .optional(
                        DATACENTER, CONSISTENCY_LEVEL, FIELDS, BATCH_SIZE, BATCH_TYPE, ASYNC_WRITE)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        CassandraParameters cassandraParameters = new CassandraParameters();
        cassandraParameters.buildWithConfig(context.getOptions());
        return () ->
                new CassandraSink(
                        cassandraParameters, context.getCatalogTable(), context.getOptions());
    }
}
