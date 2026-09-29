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

package org.apache.seatunnel.connectors.seatunnel.salesforce.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class SalesforceParameters implements Serializable {

    private String clientId;
    private String clientSecret;
    private String username;
    private String password;
    private String securityToken;
    private String instanceUrl;
    private String apiVersion;
    private int requestTimeoutMs;
    private long pollIntervalMs;
    private long jobCompletionTimeoutMs;

    public void buildWithConfig(ReadonlyConfig config) {
        this.clientId = config.get(SalesforceSourceOptions.CLIENT_ID);
        this.clientSecret = config.get(SalesforceSourceOptions.CLIENT_SECRET);
        this.username = config.get(SalesforceSourceOptions.USERNAME);
        this.password = config.get(SalesforceSourceOptions.PASSWORD);
        this.securityToken = config.get(SalesforceSourceOptions.SECURITY_TOKEN);
        this.instanceUrl = config.get(SalesforceSourceOptions.INSTANCE_URL);
        this.apiVersion = config.get(SalesforceSourceOptions.API_VERSION);
        this.requestTimeoutMs = config.get(SalesforceSourceOptions.REQUEST_TIMEOUT_MS);
        this.pollIntervalMs = config.get(SalesforceSourceOptions.POLL_INTERVAL_MS);
        this.jobCompletionTimeoutMs = config.get(SalesforceSourceOptions.JOB_COMPLETION_TIMEOUT_MS);
    }
}
