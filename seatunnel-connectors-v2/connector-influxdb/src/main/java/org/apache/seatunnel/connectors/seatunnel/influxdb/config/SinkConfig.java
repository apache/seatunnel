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

package org.apache.seatunnel.connectors.seatunnel.influxdb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Setter
@Getter
@ToString
public class SinkConfig extends InfluxDBConfig {
    public SinkConfig(ReadonlyConfig config) {
        super(config);
        loadConfig(config);
    }

    public static final Option<String> KEY_TIME =
            Options.key("key_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server key time");

    public static final Option<List<String>> KEY_TAGS =
            Options.key("key_tags")
                    .listType()
                    .noDefaultValue()
                    .withDescription("the influxdb server key tags");

    public static final Option<String> KEY_MEASUREMENT =
            Options.key("measurement")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server measurement");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("batch size of the influxdb client");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .noDefaultValue()
                    .withDescription("max retries of the influxdb client");

    public static final Option<Integer> WRITE_TIMEOUT =
            Options.key("write_timeout")
                    .intType()
                    .defaultValue(5)
                    .withDescription("the influxdb client write data timeout");

    public static final Option<Integer> RETRY_BACKOFF_MULTIPLIER_MS =
            Options.key("retry_backoff_multiplier_ms")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the influxdb client retry backoff multiplier ms");

    public static final Option<Integer> MAX_RETRY_BACKOFF_MS =
            Options.key("max_retry_backoff_ms")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the influxdb client max retry backoff ms");

    public static final Option<String> RETENTION_POLICY =
            Options.key("rp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb client retention policy");

    private static final TimePrecision DEFAULT_TIME_PRECISION = TimePrecision.NS;

    private String rp;
    private String measurement;
    private int writeTimeout = WRITE_TIMEOUT.defaultValue();
    private String keyTime;
    private List<String> keyTags;
    private int batchSize = BATCH_SIZE.defaultValue();
    private int maxRetries;
    private int retryBackoffMultiplierMs;
    private int maxRetryBackoffMs;
    private TimePrecision precision = DEFAULT_TIME_PRECISION;

    public void loadConfig(ReadonlyConfig config) {
        if (config.getOptional(KEY_TIME).isPresent()) {
            setKeyTime(config.get(KEY_TIME));
        }

        if (config.getOptional(KEY_TAGS).isPresent()) {
            setKeyTags(config.get(KEY_TAGS));
        }

        if (config.getOptional(MAX_RETRIES).isPresent()) {
            setMaxRetries(config.get(MAX_RETRIES));
        }

        if (config.getOptional(RETRY_BACKOFF_MULTIPLIER_MS).isPresent()) {
            setRetryBackoffMultiplierMs(config.get(RETRY_BACKOFF_MULTIPLIER_MS));
        }

        if (config.getOptional(MAX_RETRY_BACKOFF_MS).isPresent()) {
            setMaxRetryBackoffMs(config.get(MAX_RETRY_BACKOFF_MS));
        }

        if (config.getOptional(WRITE_TIMEOUT).isPresent()) {
            setWriteTimeout(config.get(WRITE_TIMEOUT));
        }

        if (config.getOptional(RETENTION_POLICY).isPresent()) {
            setRp(config.get(RETENTION_POLICY));
        }

        if (config.getOptional(EPOCH).isPresent()) {
            setPrecision(TimePrecision.getPrecision(config.get(EPOCH)));
        }

        setMeasurement(config.get(KEY_MEASUREMENT));
    }
}
