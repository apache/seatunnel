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

package org.apache.seatunnel.connectors.seatunnel.splunk.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Configuration options of the Splunk HTTP Event Collector (HEC) sink. */
public class SplunkSinkOptions {

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Splunk HTTP Event Collector address. When the address does not already "
                                    + "contain a collector path (for example https://splunk-host:8088), "
                                    + "/services/collector/event is appended. An address that already "
                                    + "contains /services/collector is used verbatim, so pass the full "
                                    + "endpoint including the /event suffix: a bare "
                                    + "https://host:8088/services/collector targets the raw ingestion "
                                    + "endpoint, which does not accept the JSON event envelopes this "
                                    + "sink sends.");

    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "HTTP Event Collector token, sent as the 'Authorization: Splunk <token>' header.");

    public static final Option<String> INDEX =
            Options.key("index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Target Splunk index. When unset, the index configured on the HEC token is used.");

    public static final Option<String> SOURCE =
            Options.key("source")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Value written to the Splunk 'source' event metadata field. "
                                    + "When unset, the source configured on the HEC token is used.");

    public static final Option<String> SOURCE_TYPE =
            Options.key("sourcetype")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Value written to the Splunk 'sourcetype' event metadata field. "
                                    + "When unset, the sourcetype configured on the HEC token is used.");

    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Static value written to the Splunk 'host' event metadata field. "
                                    + "Ignored when 'host_field' is set.");

    public static final Option<String> HOST_FIELD =
            Options.key("host_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of an upstream field whose value populates the Splunk 'host' event "
                                    + "metadata field. Takes precedence over the static 'host' option.");

    public static final Option<String> TIME_FIELD =
            Options.key("time_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of an upstream field whose value populates the Splunk 'time' event "
                                    + "metadata field. Must be of type TIMESTAMP (interpreted as UTC), "
                                    + "TIMESTAMP_TZ (its own offset is used), or BIGINT (interpreted as "
                                    + "epoch milliseconds). When unset, Splunk stamps events with their "
                                    + "ingest time.");

    public static final Option<Integer> MAX_BATCH_SIZE =
            Options.key("max_batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Maximum number of events sent in one HTTP Event Collector request.");

    public static final Option<Integer> MAX_RETRY_COUNT =
            Options.key("max_retry_count")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Maximum number of attempts for one batch request. Only transport failures "
                                    + "and retryable HTTP responses (429 and 5xx) are retried.");

    public static final Option<Integer> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms")
                    .intType()
                    .defaultValue(200)
                    .withDescription(
                            "Base backoff in milliseconds between two attempts of the same batch. "
                                    + "The backoff doubles on each subsequent attempt and is capped "
                                    + "at 20 seconds.");

    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Timeout in milliseconds for establishing a connection to the collector.");

    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription(
                            "Timeout in milliseconds waiting for collector response data between packets.");

    public static final Option<Boolean> TLS_VERIFY_CERTIFICATE =
            Options.key("tls_verify_certificate")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to verify the collector TLS certificate. Set to false only for "
                                    + "Splunk deployments still using the default self-signed certificate.");

    public static final Option<Boolean> TLS_VERIFY_HOSTNAME =
            Options.key("tls_verify_hostname")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to verify the collector TLS certificate hostname.");

    private SplunkSinkOptions() {}
}
