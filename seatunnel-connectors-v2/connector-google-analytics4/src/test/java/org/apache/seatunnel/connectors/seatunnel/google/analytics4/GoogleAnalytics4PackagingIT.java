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

package org.apache.seatunnel.connectors.seatunnel.google.analytics4;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4ReportTest.JSON;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4ReportTest.bytes;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4ReportTest.options;

/**
 * Runs after shade, without permitting parent-classloader fallback for connector/private classes.
 */
class GoogleAnalytics4PackagingIT {
    @Test
    void shadedArtifactLoadsFactoryOAuthAndBoundedHttp() throws Exception {
        Path artifact = Paths.get(System.getProperty("ga4.jar"));
        String own = "org.apache.seatunnel.connectors.seatunnel.google.analytics4.";
        String shaded = "org.apache.seatunnel.shade.google.analytics4.";
        AtomicReference<Throwable> failure = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/token",
                exchange -> {
                    try {
                        String request =
                                new String(
                                        GoogleAnalytics4HttpTransport.readBounded(
                                                exchange.getRequestBody(), 65536),
                                        StandardCharsets.UTF_8);
                        Assertions.assertTrue(request.contains("assertion="));
                        byte[] body =
                                "{\"access_token\":\"packaged-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}"
                                        .getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        exchange.sendResponseHeaders(200, body.length);
                        exchange.getResponseBody().write(body);
                    } catch (Throwable e) {
                        failure.set(e);
                    } finally {
                        exchange.close();
                    }
                });
        server.createContext(
                "/report",
                exchange -> {
                    try {
                        Assertions.assertEquals(
                                "Bearer packaged-token",
                                exchange.getRequestHeaders().getFirst("Authorization"));
                        byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, body.length);
                        exchange.getResponseBody().write(body);
                    } catch (Throwable e) {
                        failure.set(e);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        String origin = "http://127.0.0.1:" + server.getAddress().getPort();
        try (URLClassLoader loader =
                new URLClassLoader(
                        new URL[] {artifact.toUri().toURL()}, getClass().getClassLoader()) {
                    @Override
                    protected Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (!name.startsWith(own) && !name.startsWith(shaded)) {
                            return super.loadClass(name, resolve);
                        }
                        synchronized (getClassLoadingLock(name)) {
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
            Class<?> credentialType =
                    loader.loadClass(shaded + "com.google.auth.oauth2.ServiceAccountCredentials");
            Assertions.assertEquals(
                    artifact.toUri().toURL(),
                    credentialType.getProtectionDomain().getCodeSource().getLocation());
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            KeyPair key = generator.generateKeyPair();
            ObjectNode json =
                    JSON.createObjectNode()
                            .put("type", "service_account")
                            .put("client_id", "123")
                            .put("client_email", "fixture@example.iam.gserviceaccount.com")
                            .put("private_key_id", "fixture-key")
                            .put("project_id", "fixture")
                            .put(
                                    "private_key",
                                    "-----BEGIN PRIVATE KEY-----\n"
                                            + Base64.getEncoder()
                                                    .encodeToString(key.getPrivate().getEncoded())
                                            + "\n-----END PRIVATE KEY-----\n")
                            .put("token_uri", origin + "/token");
            Object credential =
                    credentialType
                            .getMethod("fromStream", java.io.InputStream.class)
                            .invoke(null, new ByteArrayInputStream(bytes(json)));
            credential =
                    credentialType
                            .getMethod("createScoped", java.util.Collection.class)
                            .invoke(
                                    credential,
                                    Collections.singleton(
                                            "https://www.googleapis.com/auth/analytics.readonly"));
            credentialType.getMethod("refresh").invoke(credential);
            Object token = credentialType.getMethod("getAccessToken").invoke(credential);
            Assertions.assertEquals(
                    "packaged-token", token.getClass().getMethod("getTokenValue").invoke(token));

            Map<String, Object> options = options();
            options.put("emulator_url", origin);
            TableSourceFactory factory =
                    (TableSourceFactory)
                            loader.loadClass(own + "GoogleAnalytics4SourceFactory")
                                    .getConstructor()
                                    .newInstance();
            Assertions.assertEquals("GoogleAnalytics4", factory.factoryIdentifier());
            Assertions.assertEquals(
                    "GoogleAnalytics4",
                    factory.createSource(
                                    new TableSourceFactoryContext(
                                            ReadonlyConfig.fromMap(options), loader))
                            .createSource()
                            .getPluginName());

            Class<?> configType = loader.loadClass(own + "GoogleAnalytics4Config");
            Constructor<?> configConstructor =
                    configType.getDeclaredConstructor(ReadonlyConfig.class);
            configConstructor.setAccessible(true);
            Object config = configConstructor.newInstance(ReadonlyConfig.fromMap(options));
            Class<?> transportType = loader.loadClass(own + "GoogleAnalytics4HttpTransport");
            Constructor<?> transportConstructor = transportType.getDeclaredConstructor(configType);
            transportConstructor.setAccessible(true);
            try (Closeable transport = (Closeable) transportConstructor.newInstance(config)) {
                Method report =
                        transportType.getDeclaredMethod(
                                "report", String.class, byte[].class, String.class);
                report.setAccessible(true);
                Object response =
                        report.invoke(
                                transport,
                                origin + "/report",
                                "{}".getBytes(StandardCharsets.UTF_8),
                                "packaged-token");
                java.lang.reflect.Field status = response.getClass().getDeclaredField("status");
                status.setAccessible(true);
                Assertions.assertEquals(200, status.getInt(response));
            }
        } finally {
            server.stop(0);
        }
        if (failure.get() != null) {
            throw new AssertionError("Packaged fixture failed", failure.get());
        }
    }
}
