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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.DRIVER;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.FETCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PARTITION_COLUMN;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PARTITION_LOWER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PARTITION_NUM;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PARTITION_UPPER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.QUERY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.USER;

@AutoService(Factory.class)
public class JdbcSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Jdbc";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, DRIVER, QUERY)
                .optional(
                        USER,
                        PASSWORD,
                        CONNECTION_CHECK_TIMEOUT_SEC,
                        FETCH_SIZE,
                        PARTITION_COLUMN,
                        PARTITION_UPPER_BOUND,
                        PARTITION_LOWER_BOUND,
                        PARTITION_NUM)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return JdbcSource.class;
    }
}
