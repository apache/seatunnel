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

package org.apache.seatunnel.connectors.seatunnel.pulsar.config;

import org.apache.pulsar.shade.com.google.common.base.Preconditions;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

// TODO: more field

public class PulsarClientConfig extends BasePulsarConfig {
    private static final long serialVersionUID = 1L;

    private final String serviceUrl;

    private PulsarClientConfig(String authPluginClassName, String authParams, String serviceUrl) {
        super(authPluginClassName, authParams);
        this.serviceUrl = serviceUrl;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        /**
         * Name of the authentication plugin.
         */
        private String authPluginClassName = "";
        /**
         * Parameters for the authentication plugin.
         */
        private String authParams = "";
        /**
         * Service URL provider for Pulsar service.
         */
        private String serviceUrl;

        private Builder() {
        }

        public Builder authPluginClassName(String authPluginClassName) {
            this.authPluginClassName = authPluginClassName;
            return this;
        }

        public Builder authParams(String authParams) {
            this.authParams = authParams;
            return this;
        }

        public Builder serviceUrl(String serviceUrl) {
            this.serviceUrl = serviceUrl;
            return this;
        }

        public PulsarClientConfig build() {
            Preconditions.checkArgument(StringUtils.isNotBlank(serviceUrl), "Pulsar service URL is required.");
            return new PulsarClientConfig(authPluginClassName, authParams, serviceUrl);
        }
    }
}
