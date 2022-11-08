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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Setter
@Getter
@ToString
public class SinkConfig extends InfluxDBConfig{
    public SinkConfig(Config config) {
        super(config);
    }

    private static final String KEY_TIME = "key_time";
    private static final String KEY_TAGS = "key_tags";
    public static final String KEY_MEASUREMENT = "measurement";

    private static final String BATCH_SIZE = "batch_size";
    private static final String BATCH_INTERVAL_MS = "batch_interval_ms";
    private static final String MAX_RETRIES = "max_retries";
    private static final String WRITE_TIMEOUT = "write_timeout";
    private static final String RETRY_BACKOFF_MULTIPLIER_MS = "retry_backoff_multiplier_ms";
    private static final String MAX_RETRY_BACKOFF_MS = "max_retry_backoff_ms";
    private static final String RETENTION_POLICY = "rp";
    private static final int DEFAULT_BATCH_SIZE = 1024;
    private static final int DEFAULT_WRITE_TIMEOUT = 5;
    private static final TimePrecision DEFAULT_TIME_PRECISION = TimePrecision.NS;

    private String rp;
    private String measurement;
    private int writeTimeout = DEFAULT_WRITE_TIMEOUT;
    private String keyTime;
    private List<String> keyTags;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private Integer batchIntervalMs;
    private int maxRetries;
    private int retryBackoffMultiplierMs;
    private int maxRetryBackoffMs;
    private TimePrecision precision = DEFAULT_TIME_PRECISION;

    public static SinkConfig loadConfig(Config config) {
        SinkConfig sinkConfig = new SinkConfig(config);

        if (config.hasPath(KEY_TIME)) {
            sinkConfig.setKeyTime(config.getString(KEY_TIME));
        }
        if (config.hasPath(KEY_TAGS)) {
            sinkConfig.setKeyTags(config.getStringList(KEY_TAGS));
        }
        if (config.hasPath(BATCH_INTERVAL_MS)) {
            sinkConfig.setBatchIntervalMs(config.getInt(BATCH_INTERVAL_MS));
        }
        if (config.hasPath(MAX_RETRIES)) {
            sinkConfig.setMaxRetries(config.getInt(MAX_RETRIES));
        }
        if (config.hasPath(RETRY_BACKOFF_MULTIPLIER_MS)) {
            sinkConfig.setRetryBackoffMultiplierMs(config.getInt(RETRY_BACKOFF_MULTIPLIER_MS));
        }
        if (config.hasPath(MAX_RETRY_BACKOFF_MS)) {
            sinkConfig.setMaxRetryBackoffMs(config.getInt(MAX_RETRY_BACKOFF_MS));
        }
        if (config.hasPath(WRITE_TIMEOUT)) {
            sinkConfig.setWriteTimeout(config.getInt(WRITE_TIMEOUT));
        }
        if (config.hasPath(RETENTION_POLICY)) {
            sinkConfig.setRp(config.getString(RETENTION_POLICY));
        }
        if (config.hasPath(EPOCH)) {
            sinkConfig.setPrecision(TimePrecision.getPrecision(config.getString(EPOCH)));
        }
        sinkConfig.setMeasurement(config.getString(KEY_MEASUREMENT));
        return sinkConfig;
    }

}
