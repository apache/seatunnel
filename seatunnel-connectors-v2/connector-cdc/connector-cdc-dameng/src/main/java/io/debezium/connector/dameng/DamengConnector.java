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

package io.debezium.connector.dameng;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;

import java.util.List;
import java.util.Map;

public class DamengConnector extends RelationalBaseSourceConnector {

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(DamengConnectorConfig.ALL_FIELDS);
    }

    @Override
    public void start(Map<String, String> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends Task> taskClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConfigDef config() {
        return DamengConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return Module.version();
    }
}
