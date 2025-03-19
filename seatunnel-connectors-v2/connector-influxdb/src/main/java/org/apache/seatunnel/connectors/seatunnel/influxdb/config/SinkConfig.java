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

    private static final TimePrecision DEFAULT_TIME_PRECISION = TimePrecision.NS;

    private String rp;
    private String measurement;
    private int writeTimeout;
    private String keyTime;
    private List<String> keyTags;
    private int batchSize;
    private int maxRetries;
    private int retryBackoffMultiplierMs;
    private int maxRetryBackoffMs;
    private TimePrecision precision = DEFAULT_TIME_PRECISION;

    public void loadConfig(ReadonlyConfig config) {
        setKeyTime(config.get(InfluxDBSinkOptions.KEY_TIME));
        setKeyTags(config.get(InfluxDBSinkOptions.KEY_TAGS));
        setBatchSize(config.get(InfluxDBSinkOptions.BATCH_SIZE));
        if (config.getOptional(InfluxDBSinkOptions.MAX_RETRIES).isPresent()) {
            setMaxRetries(config.get(InfluxDBSinkOptions.MAX_RETRIES));
        }
        if (config.getOptional(InfluxDBSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS).isPresent()) {
            setRetryBackoffMultiplierMs(
                    config.get(InfluxDBSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS));
        }
        if (config.getOptional(InfluxDBSinkOptions.MAX_RETRY_BACKOFF_MS).isPresent()) {
            setMaxRetryBackoffMs(config.get(InfluxDBSinkOptions.MAX_RETRY_BACKOFF_MS));
        }
        setWriteTimeout(config.get(InfluxDBSinkOptions.WRITE_TIMEOUT));
        setRp(config.get(InfluxDBSinkOptions.RETENTION_POLICY));
        setPrecision(TimePrecision.getPrecision(config.get(InfluxDBSinkOptions.EPOCH)));
        setMeasurement(config.get(InfluxDBSinkOptions.KEY_MEASUREMENT));
    }
}
