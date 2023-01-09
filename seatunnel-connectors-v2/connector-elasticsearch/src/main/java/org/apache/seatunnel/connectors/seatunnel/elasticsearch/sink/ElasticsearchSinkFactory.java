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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.EsClusterConnectionConfig.HOSTS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.EsClusterConnectionConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.EsClusterConnectionConfig.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.INDEX;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.INDEX_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.KEY_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.MAX_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.MAX_RETRY_COUNT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.PRIMARY_KEYS;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class ElasticsearchSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Elasticsearch";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
            .required(HOSTS, INDEX)
            .optional(INDEX_TYPE,
                PRIMARY_KEYS,
                KEY_DELIMITER,
                USERNAME,
                PASSWORD,
                MAX_RETRY_COUNT,
                MAX_BATCH_SIZE)
            .build();
    }
}
