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

package org.apache.seatunnel.trace.collector.enrich;

import org.apache.seatunnel.trace.collector.config.TraceCollectorConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskMappingCacheTest {

    @Test
    void testFetchErrorDoesNotOverwriteOldMapping() throws Exception {
        AtomicInteger call = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/hazelcast/rest/maps/trace/task-mapping/1",
                exchange -> {
                    int n = call.incrementAndGet();
                    if (n == 1) {
                        byte[] body =
                                "{\"items\":[{\"taskId\":10,\"worker\":\"w\",\"taskGroupName\":\"g\",\"taskClass\":\"c\"}]}"
                                        .getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, body.length);
                        exchange.getResponseBody().write(body);
                        exchange.close();
                        return;
                    }
                    exchange.sendResponseHeaders(500, -1);
                    exchange.close();
                });
        server.start();

        int port = server.getAddress().getPort();
        Path temp = Files.createTempFile("trace-collector-config", ".properties");
        Files.write(
                temp,
                ("db.type=postgres\n"
                                + "db.jdbcUrl=jdbc:postgresql://127.0.0.1:5432/seatunnel\n"
                                + "db.username=u\n"
                                + "db.password=p\n"
                                + "engine.taskMappingUrlTemplate=http://127.0.0.1:"
                                + port
                                + "/hazelcast/rest/maps/trace/task-mapping/{jobId}\n"
                                + "engine.taskMappingCacheTtlMs=0\n")
                        .getBytes(StandardCharsets.UTF_8));

        String old = System.getProperty("trace.collector.config");
        try {
            System.setProperty("trace.collector.config", temp.toString());
            TraceCollectorConfig cfg = TraceCollectorConfig.load();
            TaskMappingCache cache = new TaskMappingCache(cfg);

            Map<Long, TaskMappingCache.Mapping> first = cache.getMapping("1");
            Assertions.assertEquals(1, first.size());
            Assertions.assertEquals("w", first.get(10L).getWorker());

            Thread.sleep(2);
            Map<Long, TaskMappingCache.Mapping> second = cache.getMapping("1");
            Assertions.assertEquals(1, second.size());
            Assertions.assertEquals("w", second.get(10L).getWorker());
        } finally {
            if (old == null) {
                System.clearProperty("trace.collector.config");
            } else {
                System.setProperty("trace.collector.config", old);
            }
            Files.deleteIfExists(temp);
            server.stop(0);
        }
    }
}
