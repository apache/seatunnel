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

package org.apache.seatunnel.trace.collector;

import org.apache.seatunnel.trace.collector.config.TraceCollectorConfig;
import org.apache.seatunnel.trace.collector.db.TraceRepository;
import org.apache.seatunnel.trace.collector.db.clickhouse.ClickHouseTraceRepository;
import org.apache.seatunnel.trace.collector.db.mysql.MySqlTraceRepository;
import org.apache.seatunnel.trace.collector.db.postgres.PostgresTraceRepository;
import org.apache.seatunnel.trace.collector.http.TraceHttpServer;
import org.apache.seatunnel.trace.collector.metrics.TraceCollectorMetrics;

import io.prometheus.client.hotspot.DefaultExports;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class TraceCollectorMain {

    public static void main(String[] args) throws Exception {
        TraceCollectorConfig config = TraceCollectorConfig.load();
        log.info(
                "Starting trace collector with dbType={}, port={}",
                config.getDbType(),
                config.getServerPort());

        DefaultExports.initialize();
        TraceCollectorMetrics metrics = new TraceCollectorMetrics();

        TraceRepository repository;
        if ("clickhouse".equalsIgnoreCase(config.getDbType())) {
            repository = new ClickHouseTraceRepository(config, metrics);
        } else if ("mysql".equalsIgnoreCase(config.getDbType())) {
            repository = new MySqlTraceRepository(config, metrics);
        } else {
            repository = new PostgresTraceRepository(config, metrics);
        }
        repository.init();

        TraceHttpServer server = new TraceHttpServer(config, repository, metrics);
        server.start();

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    try {
                                        server.close();
                                        repository.close();
                                    } catch (IOException e) {
                                        log.warn("Failed to shutdown trace collector cleanly", e);
                                    }
                                },
                                "trace-collector-shutdown"));
    }
}
