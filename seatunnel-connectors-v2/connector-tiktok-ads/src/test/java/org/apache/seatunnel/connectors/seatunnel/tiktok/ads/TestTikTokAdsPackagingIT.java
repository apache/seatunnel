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

package org.apache.seatunnel.connectors.seatunnel.tiktok.ads;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarFile;
import java.util.stream.Stream;

import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsReportTest.bytes;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsReportTest.options;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsReportTest.page;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsReportTest.row;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestTikTokAdsPackagingIT {
    @Test
    void packagedFactoryAndRelocatedHttpExecuteRealLocalRequest() throws Exception {
        Path artifact;
        try (Stream<Path> files = Files.list(Paths.get("target"))) {
            artifact =
                    files.filter(
                                    path ->
                                            path.getFileName()
                                                    .toString()
                                                    .matches("connector-tiktok-ads-.*[.]jar"))
                            .filter(path -> !path.getFileName().toString().contains("sources"))
                            .filter(path -> !path.getFileName().toString().contains("javadoc"))
                            .findFirst()
                            .orElseThrow(() -> new IOException("packaged artifact missing"));
        }
        try (JarFile jar = new JarFile(artifact.toFile())) {
            assertNotNull(
                    jar.getEntry(
                            "META-INF/services/org.apache.seatunnel.api.table.factory.Factory"));
            assertNotNull(jar.getEntry("META-INF/NOTICE"));
        }
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger requests = new AtomicInteger();
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange -> {
                    assertEquals(
                            "mock-token", exchange.getRequestHeaders().getFirst("Access-Token"));
                    requests.incrementAndGet();
                    byte[] body = bytes(page(1, 1, row("100")));
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                    exchange.close();
                });
        server.start();
        String prefix = "org.apache.seatunnel.connectors.seatunnel.tiktok.ads.";
        try (URLClassLoader loader =
                new URLClassLoader(
                        new URL[] {artifact.toUri().toURL()}, getClass().getClassLoader()) {
                    @Override
                    protected Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        synchronized (getClassLoadingLock(name)) {
                            if (!name.startsWith(prefix)) {
                                return super.loadClass(name, resolve);
                            }
                            Class<?> type = findLoadedClass(name);
                            if (type == null) {
                                type = findClass(name);
                            }
                            if (resolve) {
                                resolveClass(type);
                            }
                            return type;
                        }
                    }
                }) {
            assertSame(
                    loader,
                    loader.loadClass(prefix + "shaded.http.impl.client.HttpClients")
                            .getClassLoader());
            Object factory =
                    loader.loadClass(prefix + "TikTokAdsSourceFactory")
                            .getConstructor()
                            .newInstance();
            assertEquals(
                    "TikTokAds", factory.getClass().getMethod("factoryIdentifier").invoke(factory));
            Map<String, Object> config = options();
            config.put("token", "mock-token");
            config.put("mock_url", "http://127.0.0.1:" + server.getAddress().getPort());
            SeaTunnelSource source =
                    (SeaTunnelSource)
                            loader.loadClass(prefix + "TikTokAdsSource")
                                    .getConstructor(ReadonlyConfig.class)
                                    .newInstance(ReadonlyConfig.fromMap(config));
            SourceReader.Context context = mock(SourceReader.Context.class);
            SourceReader reader = source.createReader(context);
            Collector<SeaTunnelRow> output = mock(Collector.class);
            when(output.getCheckpointLock()).thenReturn(new Object());
            List<SeaTunnelRow> rows = new ArrayList<>();
            doAnswer(
                            call -> {
                                rows.add(call.getArgument(0));
                                return null;
                            })
                    .when(output)
                    .collect(any(SeaTunnelRow.class));
            try {
                reader.open();
                reader.pollNext(output);
                assertEquals(1, rows.size());
                assertEquals("100", rows.get(0).getField(2));
                assertTrue(rows.get(0).getField(1) instanceof java.math.BigDecimal);
                verify(context).signalNoMoreElement();
            } finally {
                reader.close();
            }
            assertEquals(1, requests.get());
        } finally {
            server.stop(0);
        }
    }
}
