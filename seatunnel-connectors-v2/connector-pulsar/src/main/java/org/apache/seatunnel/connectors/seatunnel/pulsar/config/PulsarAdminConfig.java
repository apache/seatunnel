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

public class PulsarAdminConfig extends BasePulsarConfig {
    private static final long serialVersionUID = 1L;
    private final String adminUrl;

    private PulsarAdminConfig(String authPluginClassName, String authParams, String adminUrl) {
        super(authPluginClassName, authParams);
        this.adminUrl = adminUrl;
    }

    public String getAdminUrl() {
        return adminUrl;
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
        private String adminUrl;

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

        public Builder adminUrl(String adminUrl) {
            this.adminUrl = adminUrl;
            return this;
        }

        public PulsarAdminConfig build() {
            Preconditions.checkArgument(StringUtils.isNotBlank(adminUrl), "Pulsar admin URL is required.");
            return new PulsarAdminConfig(authPluginClassName, authParams, adminUrl);
        }
    }
}
