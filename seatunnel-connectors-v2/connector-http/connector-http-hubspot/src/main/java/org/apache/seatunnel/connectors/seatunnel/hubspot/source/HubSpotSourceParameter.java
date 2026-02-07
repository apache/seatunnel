/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hubspot.source; // FIX: Updated to match your file path

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import java.lang.reflect.Field;

public class HubSpotSourceParameter extends HttpParameter {
    // FIX 1: Change default to valid JsonPath
    public static final String DEFAULT_CONTENT_FIELD = "$.results";

    @Override
    public void buildWithConfig(ReadonlyConfig pluginConfig) {
        super.buildWithConfig(pluginConfig);

        // FIX 2: Support 'url' override for testing
        if (pluginConfig.getOptional(HttpCommonOptions.URL).isPresent()) {
            this.setUrl(pluginConfig.get(HttpCommonOptions.URL));
        }

        // FIX 3: Apply default content_field using Reflection (Safe Fallback)
        try {
            Field contentFieldVar = HttpParameter.class.getDeclaredField("contentField");
            contentFieldVar.setAccessible(true);

            Object currentValue = contentFieldVar.get(this);

            if (currentValue == null) {
                contentFieldVar.set(this, DEFAULT_CONTENT_FIELD);
            }
        } catch (Exception e) {
            // Safe to ignore if reflection fails
        }
    }
}