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

package org.apache.seatunnel.connectors.seatunnel.redis.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class RedisSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Redis";
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new RedisSink(context.getOptions(), catalogTable);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        RedisBaseOptions.HOST,
                        RedisBaseOptions.PORT,
                        RedisBaseOptions.KEY,
                        RedisBaseOptions.DATA_TYPE)
                .optional(
                        RedisBaseOptions.MODE,
                        RedisBaseOptions.AUTH,
                        RedisBaseOptions.USER,
                        RedisBaseOptions.KEY_PATTERN,
                        RedisBaseOptions.FORMAT,
                        RedisSinkOptions.EXPIRE,
                        RedisSinkOptions.SUPPORT_CUSTOM_KEY,
                        RedisSinkOptions.VALUE_FIELD,
                        RedisSinkOptions.HASH_KEY_FIELD,
                        RedisSinkOptions.HASH_VALUE_FIELD,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .conditional(
                        RedisBaseOptions.MODE,
                        RedisBaseOptions.RedisMode.CLUSTER,
                        RedisBaseOptions.NODES)
                .build();
    }
}
