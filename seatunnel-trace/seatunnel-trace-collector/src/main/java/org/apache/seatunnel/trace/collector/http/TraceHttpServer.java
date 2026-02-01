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

package org.apache.seatunnel.trace.collector.http;

import org.apache.seatunnel.trace.collector.config.TraceCollectorConfig;
import org.apache.seatunnel.trace.collector.db.TraceRepository;
import org.apache.seatunnel.trace.collector.enrich.TaskMappingCache;
import org.apache.seatunnel.trace.collector.metrics.TraceCollectorMetrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class TraceHttpServer implements Closeable {

    public static final ObjectMapper MAPPER = new ObjectMapper();

    private final TraceCollectorConfig config;
    private final TraceRepository repository;
    private final TraceCollectorMetrics metrics;
    private final HttpServer server;
    private final ExecutorService executor;
    private final TaskMappingCache mappingCache;

    public TraceHttpServer(
            TraceCollectorConfig config, TraceRepository repository, TraceCollectorMetrics metrics)
            throws IOException {
        this.config = config;
        this.repository = repository;
        this.metrics = metrics;
        this.mappingCache = new TaskMappingCache(config);
        this.server = HttpServer.create(new InetSocketAddress(config.getServerPort()), 0);
        this.executor =
                Executors.newFixedThreadPool(
                        Math.max(4, Runtime.getRuntime().availableProcessors()));
        this.server.setExecutor(executor);

        TraceAuth auth = new TraceAuth(config.getAuthToken());
        this.server.createContext("/healthz", new HealthHandler(metrics));
        this.server.createContext("/metrics", new MetricsHandler(metrics));
        EventsIngestHandler ingestHandler =
                new EventsIngestHandler(config, repository, metrics, auth, mappingCache);
        this.server.createContext("/api/v1/seatunnel/events", ingestHandler);
        // Compatibility endpoint, keep old default URL working.
        this.server.createContext("/ingest", ingestHandler);
        this.server.createContext(
                "/api/v1/traces", new TracesQueryHandler(repository, metrics, auth));
        this.server.createContext(
                "/api/v1/traces/", new TraceDetailHandler(repository, metrics, auth));
    }

    public void start() {
        server.start();
        log.info("Trace collector HTTP server started on port {}", config.getServerPort());
    }

    @Override
    public void close() {
        server.stop(1);
        executor.shutdownNow();
    }
}
