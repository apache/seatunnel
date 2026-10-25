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

package org.apache.seatunnel.connectors.seatunnel.linear.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import java.util.HashMap;

public class LinearSourceParameter extends HttpParameter {
    /**
     * Overrides buildWithConfig to accept an explicit apiKey parameter. Linear's GraphQL API
     * requires the API key to be passed specifically as an Authorization header, so this method
     * ensures the key is properly extracted and configured
     */
    public void buildWithConfig(ReadonlyConfig pluginConfig, String apiKey) {
        super.buildWithConfig(pluginConfig);
        if (this.headers == null) {
            this.headers = new HashMap<>();
        }
        this.headers.put("Authorization", apiKey);
    }
}
