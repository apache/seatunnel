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

package org.apache.seatunnel.config.sql;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ConfigAdapter;

import com.google.auto.service.AutoService;

import java.nio.file.Path;
import java.util.Map;

import static org.apache.seatunnel.config.sql.utils.Constant.SQL_FILE_EXT;

@AutoService(ConfigAdapter.class)
public class SqlConfigAdapter implements ConfigAdapter {
    @Override
    public String[] extensionIdentifiers() {
        return new String[] {SQL_FILE_EXT};
    }

    @Override
    public Map<String, Object> loadConfig(Path configFilePath) {
        Config config = SqlConfigBuilder.of(configFilePath);
        return config.root().unwrapped();
    }
}
