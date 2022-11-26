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

package org.apache.seatunnel.connectors.seatunnel.influxdb.client;

import org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorException;

import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.impl.InfluxDBImpl;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class InfluxDBClient {
    public static InfluxDB getInfluxDB(InfluxDBConfig config) throws ConnectException {
        OkHttpClient.Builder clientBuilder =
            new OkHttpClient.Builder()
                .connectTimeout(config.getConnectTimeOut(), TimeUnit.MILLISECONDS)
                .readTimeout(config.getQueryTimeOut(), TimeUnit.SECONDS);
        InfluxDB.ResponseFormat format = InfluxDB.ResponseFormat.valueOf(config.getFormat());
        clientBuilder.addInterceptor(
            new Interceptor() {
                @Override
                public Response intercept(Chain chain) throws IOException {
                    Request request = chain.request();
                    HttpUrl httpUrl =
                        request.url()
                            .newBuilder()
                            //set epoch
                            .addQueryParameter("epoch", config.getEpoch())
                            .build();
                    Request build = request.newBuilder().url(httpUrl).build();
                    return chain.proceed(build);
                }
            });
        InfluxDB influxdb =
            new InfluxDBImpl(
                config.getUrl(),
                StringUtils.isEmpty(config.getUsername()) ? StringUtils.EMPTY : config.getUsername(),
                StringUtils.isEmpty(config.getPassword()) ? StringUtils.EMPTY : config.getPassword(),
                clientBuilder,
                format);
        String version = influxdb.version();
        if (!influxdb.ping().isGood()) {
            throw new InfluxdbConnectorException(InfluxdbConnectorErrorCode.CONNECT_FAILED,
                String.format(
                    "Connect influxdb failed, the url is: {%s}",
                    config.getUrl()
                )
            );
        }
        log.info("connect influxdb successful. sever version :{}.", version);
        return influxdb;
    }

    public static void setWriteProperty(InfluxDB influxdb, SinkConfig sinkConfig) {
        String rp = sinkConfig.getRp();
        if (!StringUtils.isEmpty(rp)) {
            influxdb.setRetentionPolicy(rp);
        }
    }

    public static InfluxDB getWriteClient(SinkConfig sinkConfig) throws ConnectException {
        InfluxDB influxdb = getInfluxDB(sinkConfig);
        influxdb.setDatabase(sinkConfig.getDatabase());
        setWriteProperty(getInfluxDB(sinkConfig), sinkConfig);
        return influxdb;
    }
}
