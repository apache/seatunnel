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

package org.apache.seatunnel.connectors.seatunnel.jira.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;
import org.apache.seatunnel.connectors.seatunnel.jira.source.config.JiraSourceConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class JiraSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Jira";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
            .required(JiraSourceConfig.URL)
            .required(JiraSourceConfig.EMAIL)
            .required(JiraSourceConfig.API_TOKEN)
            .optional(JiraSourceConfig.METHOD)
            .optional(JiraSourceConfig.HEADERS)
            .optional(JiraSourceConfig.PARAMS)
            .optional(JiraSourceConfig.FORMAT)
            .conditional(HttpConfig.METHOD, HttpRequestMethod.POST, JiraSourceConfig.BODY)
            .conditional(HttpConfig.FORMAT, HttpConfig.ResponseFormat.JSON, SeaTunnelSchema.SCHEMA)
            .optional(JiraSourceConfig.POLL_INTERVAL_MILLS)
            .optional(JiraSourceConfig.RETRY)
            .optional(JiraSourceConfig.RETRY_BACKOFF_MAX_MS)
            .optional(JiraSourceConfig.RETRY_BACKOFF_MULTIPLIER_MS)
            .build();
    }
}
