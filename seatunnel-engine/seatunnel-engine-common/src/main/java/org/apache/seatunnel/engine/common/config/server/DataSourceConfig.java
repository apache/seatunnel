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

package org.apache.seatunnel.engine.common.config.server;

import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for DataSource Center which manages external metadata providers.
 *
 * <p>This config contains only common properties (enabled, kind) and a properties map for
 * provider-specific settings. Provider implementations should extract their own configuration from
 * the properties map.
 */
@Data
public class DataSourceConfig implements Serializable {

    /** Whether to enable DataSource Center. */
    private boolean enabled = DataSourceOptions.ENABLED.defaultValue();

    /**
     * The kind of DataSource provider to use. Supported values: "gravitino", "datahub", "atlas",
     * etc.
     */
    private String kind = DataSourceOptions.KIND.defaultValue();

    /**
     * Provider-specific properties. Each provider (e.g., Gravitino) should extract its own
     * configuration from this map.
     */
    private Map<String, String> properties = new HashMap<>();
}
