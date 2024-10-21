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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;

/**
 * SourceConfig is the configuration for the PrometheusSource.
 *
 * <p>please see the following link for more details:
 * https://prometheus.io/docs/prometheus/latest/querying/api/
 */
public class PrometheusSourceConfig extends HttpConfig {

    public static final String INSTANT_QUERY_URL = "/api/v1/query";

    public static final String RANGE_QUERY = "Range";

    public static final String INSTANT_QUERY = "Instant";

    public static final String RANGE_QUERY_URL = "/api/v1/query_range";

    public static final Option<String> QUERY =
            Options.key("query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prometheus expression query string");

    public static final Option<String> QUERY_TYPE =
            Options.key("query_type")
                    .stringType()
                    .defaultValue("Instant")
                    .withDescription("Prometheus expression query string");

    public static final Option<Long> TIMEOUT =
            Options.key("timeout")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Evaluation timeout");

    public static class RangeConfig {

        public static final Option<String> START =
                Options.key("start")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("Start timestamp, inclusive.");

        public static final Option<String> END =
                Options.key("end")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("End timestamp, inclusive.");

        public static final Option<String> STEP =
                Options.key("step")
                        .stringType()
                        .noDefaultValue()
                        .withDescription(
                                " Query resolution step width in duration format or float number of seconds.");
    }

    public static class InstantQueryConfig {
        public static final Option<Long> TIME =
                Options.key("time")
                        .longType()
                        .noDefaultValue()
                        .withDescription("Evaluation timestamp,unix_timestamp");
    }
}
