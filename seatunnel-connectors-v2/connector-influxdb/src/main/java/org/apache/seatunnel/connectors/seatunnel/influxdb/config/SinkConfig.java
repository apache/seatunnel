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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Setter
@Getter
@ToString
@SuppressWarnings("checkstyle:MagicNumber")
public class SinkConfig extends InfluxDBConfig {
    public SinkConfig(Config config) {
        super(config);
    }

    public static final Option<String> KEY_TIME = Options.key("key_time")
        .stringType()
        .noDefaultValue()
        .withDescription("the influxdb server key time");

    public static final Option<List<String>> KEY_TAGS = Options.key("key_tags")
        .listType()
        .noDefaultValue()
        .withDescription("the influxdb server key tags");

    public static final Option<String> KEY_MEASUREMENT = Options.key("measurement")
        .stringType()
        .noDefaultValue()
        .withDescription("the influxdb server measurement");

    public static final Option<Integer> BATCH_SIZE = Options.key("batch_size")
        .intType()
        .defaultValue(1024)
        .withDescription("batch size of the influxdb client");

    public static final Option<Integer> BATCH_INTERVAL_MS = Options.key("batch_interval_ms")
        .intType()
        .noDefaultValue()
        .withDescription("batch interval ms of the influxdb client");

    public static final Option<Integer> MAX_RETRIES = Options.key("max_retries")
        .intType()
        .noDefaultValue()
        .withDescription("max retries of the influxdb client");

    public static final Option<Integer> WRITE_TIMEOUT = Options.key("write_timeout")
        .intType()
        .defaultValue(5)
        .withDescription("the influxdb client write data timeout");

    public static final Option<Integer> RETRY_BACKOFF_MULTIPLIER_MS = Options.key("retry_backoff_multiplier_ms")
        .intType()
        .noDefaultValue()
        .withDescription("the influxdb client retry backoff multiplier ms");

    public static final Option<Integer> MAX_RETRY_BACKOFF_MS = Options.key("max_retry_backoff_ms")
        .intType()
        .noDefaultValue()
        .withDescription("the influxdb client max retry backoff ms");

    public static final Option<String> RETENTION_POLICY = Options.key("rp")
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
    private Integer batchIntervalMs;
    private int maxRetries;
    private int retryBackoffMultiplierMs;
    private int maxRetryBackoffMs;
    private TimePrecision precision = DEFAULT_TIME_PRECISION;

    public static SinkConfig loadConfig(Config config) {
        SinkConfig sinkConfig = new SinkConfig(config);

        if (config.hasPath(KEY_TIME.key())) {
            sinkConfig.setKeyTime(config.getString(KEY_TIME.key()));
        }
        if (config.hasPath(KEY_TAGS.key())) {
            sinkConfig.setKeyTags(config.getStringList(KEY_TAGS.key()));
        }
        if (config.hasPath(BATCH_INTERVAL_MS.key())) {
            sinkConfig.setBatchIntervalMs(config.getInt(BATCH_INTERVAL_MS.key()));
        }
        if (config.hasPath(MAX_RETRIES.key())) {
            sinkConfig.setMaxRetries(config.getInt(MAX_RETRIES.key()));
        }
        if (config.hasPath(RETRY_BACKOFF_MULTIPLIER_MS.key())) {
            sinkConfig.setRetryBackoffMultiplierMs(config.getInt(RETRY_BACKOFF_MULTIPLIER_MS.key()));
        }
        if (config.hasPath(MAX_RETRY_BACKOFF_MS.key())) {
            sinkConfig.setMaxRetryBackoffMs(config.getInt(MAX_RETRY_BACKOFF_MS.key()));
        }
        if (config.hasPath(WRITE_TIMEOUT.key())) {
            sinkConfig.setWriteTimeout(config.getInt(WRITE_TIMEOUT.key()));
        }
        if (config.hasPath(RETENTION_POLICY.key())) {
            sinkConfig.setRp(config.getString(RETENTION_POLICY.key()));
        }
        if (config.hasPath(EPOCH.key())) {
            sinkConfig.setPrecision(TimePrecision.getPrecision(config.getString(EPOCH.key())));
        }
        sinkConfig.setMeasurement(config.getString(KEY_MEASUREMENT.key()));
        return sinkConfig;
    }

}
