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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;
import org.apache.seatunnel.connectors.seatunnel.prometheus.Exception.PrometheusConnectorException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;

import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.INSTANT_QUERY_URL;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.InstantQueryConfig.TIME;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.QUERY;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.QUERY_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.RANGE_QUERY;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.RANGE_QUERY_URL;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.RangeConfig.END;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.RangeConfig.START;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.RangeConfig.STEP;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.TIMEOUT;

public class PrometheusSourceParameter extends HttpParameter {
    public static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";

    public void buildWithConfig(Config pluginConfig) {
        super.buildWithConfig(pluginConfig);

        String query = pluginConfig.getString(QUERY.key());

        String queryType =
                pluginConfig.hasPath(QUERY_TYPE.key())
                        ? pluginConfig.getString(QUERY_TYPE.key())
                        : QUERY_TYPE.defaultValue();

        this.params = this.getParams() == null ? new HashMap<>() : this.getParams();

        params.put(PrometheusSourceConfig.QUERY.key(), query);

        this.setMethod(HttpRequestMethod.GET);

        if (pluginConfig.hasPath(TIMEOUT.key())) {
            params.put(TIMEOUT.key(), pluginConfig.getString(TIMEOUT.key()));
        }

        if (RANGE_QUERY.equals(queryType)) {
            this.setUrl(this.getUrl() + RANGE_QUERY_URL);
            params.put(START.key(), checkTimeParam(pluginConfig.getString(START.key())));
            params.put(END.key(), checkTimeParam(pluginConfig.getString(END.key())));
            params.put(STEP.key(), pluginConfig.getString(STEP.key()));

        } else {
            this.setUrl(this.getUrl() + INSTANT_QUERY_URL);
            if (pluginConfig.hasPath(TIME.key())) {
                String time = pluginConfig.getString(TIME.key());
                params.put(TIME.key(), time);
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
