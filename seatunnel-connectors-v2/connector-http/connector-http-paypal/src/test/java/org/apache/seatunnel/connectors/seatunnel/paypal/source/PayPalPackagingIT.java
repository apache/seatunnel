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

package org.apache.seatunnel.connectors.seatunnel.paypal.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.Factory;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PayPalPackagingIT {
    @Test
    void packagedFactoryAndOAuthTransportLoadWithoutConnectorParentFallback() throws Exception {
        File[] files =
                new File("target")
                        .listFiles(
                                (dir, name) ->
                                        name.startsWith("connector-http-paypal-")
                                                && name.endsWith(".jar")
                                                && !name.contains("sources")
                                                && !name.contains("tests"));
        assertNotNull(files);
        assertEquals(1, files.length);
        try (JarFile jar = new JarFile(files[0])) {
            assertNotNull(
                    jar.getJarEntry(
                            "META-INF/services/org.apache.seatunnel.api.table.factory.Factory"));
            assertNotNull(jar.getJarEntry("org/apache/http/impl/client/HttpClients.class"));
        }
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    requests.incrementAndGet();
                    String response;
                    if (exchange.getRequestURI().getPath().equals("/v1/oauth2/token")) {
                        assertTrue(
                                exchange.getRequestHeaders()
                                        .getFirst("Authorization")
                                        .startsWith("Basic "));
                        response =
                                "{\"access_token\":\"packaged-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
                    } else {
                        assertEquals(
                                "Bearer packaged-token",
                                exchange.getRequestHeaders().getFirst("Authorization"));
                        response = PayPalResponseTest.page(1, 0, "");
                    }
                    byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, bytes.length);
                    exchange.getResponseBody().write(bytes);
                    exchange.close();
                });
        server.start();
        try (URLClassLoader loader =
                new URLClassLoader(
                        new URL[] {files[0].toURI().toURL()}, getClass().getClassLoader()) {
                    @Override
                    protected synchronized Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (name.startsWith("org.apache.seatunnel.connectors.seatunnel.paypal.")
                                || name.startsWith("org.apache.http.")) {
                            Class<?> result = findLoadedClass(name);
                            if (result == null) {
                                result = findClass(name);
                            }
                            if (resolve) {
                                resolveClass(result);
                            }
                            return result;
                        }
                        return super.loadClass(name, resolve);
                    }
                }) {
            String prefix = "org.apache.seatunnel.connectors.seatunnel.paypal.source.";
            Factory factory =
                    (Factory)
                            loader.loadClass(prefix + "PayPalSourceFactory")
                                    .getConstructor()
                                    .newInstance();
            assertEquals("PayPal", factory.factoryIdentifier());
            assertEquals(loader, factory.getClass().getClassLoader());
            Map<String, Object> options = PayPalResponseTest.options();
            options.put("mock_mode", true);
            options.put("api_base_url", "http://127.0.0.1:" + server.getAddress().getPort());
            ReadonlyConfig readonly = ReadonlyConfig.fromMap(options);
            assertNotNull(
                    loader.loadClass(prefix + "PayPalSource")
                            .getConstructor(ReadonlyConfig.class)
                            .newInstance(readonly));
            Class<?> configClass = loader.loadClass(prefix + "PayPalConfig");
            Constructor<?> configConstructor =
                    configClass.getDeclaredConstructor(ReadonlyConfig.class);
            configConstructor.setAccessible(true);
            Object config = configConstructor.newInstance(readonly);
            Class<?> clientClass = loader.loadClass(prefix + "PayPalClient");
            Constructor<?> clientConstructor = clientClass.getDeclaredConstructor(configClass);
            clientConstructor.setAccessible(true);
            try (AutoCloseable client = (AutoCloseable) clientConstructor.newInstance(config)) {
                Method page = clientClass.getDeclaredMethod("page", int.class);
                page.setAccessible(true);
                assertNotNull(page.invoke(client, 1));
                assertEquals(2, requests.get());
            }
        } finally {
            server.stop(0);
        }
    }
}
