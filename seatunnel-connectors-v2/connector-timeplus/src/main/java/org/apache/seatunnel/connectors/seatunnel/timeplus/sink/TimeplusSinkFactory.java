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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.BULK_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.PRIMARY_KEY;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SPLIT_MODE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SUPPORT_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.TIMEPLUS_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.USERNAME;

@AutoService(Factory.class)
// TODO: this factory is not really used yet
public class TimeplusSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Timeplus";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, DATABASE, TABLE)
                .optional(
                        TIMEPLUS_CONFIG,
                        BULK_SIZE,
                        SPLIT_MODE,
                        SHARDING_KEY,
                        PRIMARY_KEY,
                        SUPPORT_UPSERT,
                        ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE)
                .bundled(USERNAME, PASSWORD)
                .build();
    }
}
