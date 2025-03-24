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

package org.apache.seatunnel.connectors.seatunnel.prometheus.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;
import org.apache.seatunnel.connectors.seatunnel.prometheus.Exception.PrometheusConnectorException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;

import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.INSTANT_QUERY_URL;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.RANGE_QUERY_URL;

public class PrometheusSourceParameter extends HttpParameter {
    public static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";

    public void buildWithConfig(ReadonlyConfig pluginConfig) {
        super.buildWithConfig(pluginConfig);
        String query = pluginConfig.get(PrometheusSourceOptions.QUERY);
        PrometheusQueryType queryType = pluginConfig.get(PrometheusSourceOptions.QUERY_TYPE);
        this.params = this.getParams() == null ? new HashMap<>() : this.getParams();
        params.put(PrometheusSourceOptions.QUERY.key(), query);
        this.setMethod(HttpRequestMethod.GET);
        if (pluginConfig.getOptional(PrometheusSourceOptions.TIMEOUT).isPresent()) {
            params.put(
                    PrometheusSourceOptions.TIMEOUT.key(),
                    String.valueOf(pluginConfig.get(PrometheusSourceOptions.TIMEOUT)));
        }
        if (PrometheusQueryType.Range.equals(queryType)) {
            this.setUrl(this.getUrl() + RANGE_QUERY_URL);
            params.put(
                    PrometheusSourceOptions.START.key(),
                    checkTimeParam(pluginConfig.get(PrometheusSourceOptions.START)));
            params.put(
                    PrometheusSourceOptions.END.key(),
                    checkTimeParam(pluginConfig.get(PrometheusSourceOptions.END)));
            params.put(
                    PrometheusSourceOptions.STEP.key(),
                    pluginConfig.get(PrometheusSourceOptions.STEP));
        } else {
            this.setUrl(this.getUrl() + INSTANT_QUERY_URL);
            if (pluginConfig.getOptional(PrometheusSourceOptions.TIME).isPresent()) {
                params.put(
                        PrometheusSourceOptions.TIME.key(),
                        String.valueOf(pluginConfig.get(PrometheusSourceOptions.TIME)));
            }
        }
        this.setParams(params);
    }

    private String checkTimeParam(String time) {
        if (CURRENT_TIMESTAMP.equals(time)) {
            ZonedDateTime now = ZonedDateTime.now();
            return now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        if (isValidISO8601(time)) {
            return time;
        }
        throw new PrometheusConnectorException(
                CommonErrorCode.UNSUPPORTED_DATA_TYPE, "unsupported time type");
    }

    private boolean isValidISO8601(String dateTimeString) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
            ZonedDateTime.parse(dateTimeString, formatter);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
