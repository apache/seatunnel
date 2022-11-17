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

package org.apache.seatunnel.connectors.seatunnel.datahub.sink;

import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubConfig.ACCESS_ID;
import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubConfig.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubConfig.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubConfig.PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubConfig.RETRY_TIMES;
import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubConfig.TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubConfig.TOPIC;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class DataHubSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "DataHub";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(ENDPOINT, ACCESS_ID, ACCESS_KEY, PROJECT, TOPIC, TIMEOUT, RETRY_TIMES).build();
    }
}
