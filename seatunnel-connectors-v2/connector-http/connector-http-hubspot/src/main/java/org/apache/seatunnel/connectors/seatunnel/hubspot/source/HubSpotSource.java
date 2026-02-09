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

package org.apache.seatunnel.connectors.seatunnel.hubspot.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HubSpotSource extends HttpSource {

    public HubSpotSource(ReadonlyConfig config) {
        super(config);

        // 1. Configure the parameter (URL overrides, etc)
        HubSpotSourceParameter.configure(this.httpParameter, config);

        // 2. CRITICAL FIX: The parent HttpSource uses 'this.contentField' to create the reader.
        // If it wasn't found in the config, we MUST set it here, or the reader crashes.
        if (this.contentField == null) {
            this.contentField = HubSpotSourceParameter.DEFAULT_CONTENT_FIELD;
        }
    }

    @Override
    public String getPluginName() {
        return "HubSpot";
    }
}
