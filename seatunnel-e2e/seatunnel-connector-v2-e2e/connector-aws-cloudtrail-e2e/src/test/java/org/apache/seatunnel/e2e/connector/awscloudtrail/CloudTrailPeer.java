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

package org.apache.seatunnel.e2e.connector.awscloudtrail;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

/**
 * Emulates the regional CloudTrail HTTPS lookup operation outside the Zeta engine process. It
 * independently verifies the SigV4 signature and preserves the request sequence for assertions.
 */
public final class CloudTrailPeer {
    static final String EAST = "cloudtrail.us-east-1.amazonaws.com";
    static final String CHINA = "cloudtrail.cn-north-1.amazonaws.com.cn";
    static final String UNTRUSTED = "cloudtrail.eu-west-1.amazonaws.com";
    static final String WRONG_HOST = "cloudtrail.us-west-2.amazonaws.com";
    static final String GOV = "cloudtrail.us-gov-west-1.amazonaws.com";
    static final String ACCESS = "AKIATEST000000000001";
    static final String OTHER_ACCESS = "AKIATEST000000000002";
    static final String SECRET = "Synthetic-lookup-signing-secret-0";
    static final String TOKEN = "synthetic-session-mark-1456+/=";
    static final String PASSWORD = "cloudtrail-fixture-password";
    static final String PRIVATE = "never-log-fixture-private-data";
    private static final String CONTENT = "application/x-amz-json-1.1";
    private static final String TARGET =
            "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents";
    private static final DateTimeFormatter STAMP =
            DateTimeFormatter.ofPattern("uuuuMMdd'T'HHmmss'Z'");
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path base;
    private final Map<String, Integer> indices = new HashMap<>();

    private CloudTrailPeer(Path base) {
        this.base = base;
    }

    public static void main(String[] args) throws Exception {
        Path directory = Path.of(args[0]);
        CloudTrailPeer fixture = new CloudTrailPeer(directory);
        ExecutorService executor = Executors.newCachedThreadPool();
        HttpsServer trusted = fixture.server("trusted", "127.0.0.1", executor);
        HttpsServer untrusted = fixture.server("untrusted", "127.0.0.2", executor);
        try {
            Files.createFile(directory.resolve("ready"));
            while (!Files.exists(directory.resolve("stop"))) {
                Thread.sleep(50);
            }
        } finally {
            trusted.stop(0);
            untrusted.stop(0);
            executor.shutdownNow();
        }
    }

    private HttpsServer server(String identity, String host, ExecutorService executor)
            throws Exception {
        KeyStore key = KeyStore.getInstance("PKCS12");
        char[] password = PASSWORD.toCharArray();
        try (InputStream source = Files.newInputStream(base.resolve(identity + ".p12"))) {
            key.load(source, password);
        }
        KeyManagerFactory managers =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        managers.init(key, password);
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(managers.getKeyManagers(), null, null);
        HttpsServer server = HttpsServer.create(new InetSocketAddress(host, 443), 32);
        server.setHttpsConfigurator(new HttpsConfigurator(tls));
        server.createContext("/", this::reply);
        server.setExecutor(executor);
        server.start();
        return server;
    }

    private void reply(HttpExchange exchange) throws IOException {
        String name = Files.readString(base.resolve("active")).trim();
        try {
            JsonNode step = next(name, exchange);
            int status = step.path("status").asInt();
            String mode = step.path("mode").asText();
            String errorType = step.path("errorType").asText();
            String location = step.path("location").asText();
            String retry = step.path("retryAfter").asText();
            if (!errorType.isEmpty()) {
                exchange.getResponseHeaders().set("x-amzn-errortype", errorType);
            }
            if (!location.isEmpty()) {
                exchange.getResponseHeaders().set("Location", location);
            }
            if (!retry.isEmpty()) {
                exchange.getResponseHeaders().set("Retry-After", retry);
            }
            exchange.getResponseHeaders().set("Content-Type", CONTENT);
            if ("hold".equals(mode)) {
                while (!Files.exists(base.resolve(name + ".release"))) {
                    Thread.sleep(30);
                }
            }
            if ("trickle".equals(mode)) {
                trickle(name, exchange);
                return;
            }
            byte[] body = step.path("response").asText().getBytes(StandardCharsets.UTF_8);
            if ("gzip".equals(mode)) {
                ByteArrayOutputStream encoded = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(encoded)) {
                    gzip.write(body);
                }
                body = encoded.toByteArray();
                exchange.getResponseHeaders().set("Content-Encoding", "gzip");
            }
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length != 0) {
                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(body);
                }
            }
        } catch (IllegalArgumentException e) {
            Files.writeString(base.resolve(name + ".invalid"), e.getMessage());
            exchange.sendResponseHeaders(422, -1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            exchange.close();
        }
    }

    private synchronized JsonNode next(String name, HttpExchange exchange) throws IOException {
        int index = indices.merge(name, 1, Integer::sum) - 1;
        JsonNode program = JSON.readTree(Files.readAllBytes(base.resolve(name + ".program")));
        JsonNode step = program.path(index);
        require(!step.isMissingNode(), "Extra CloudTrail request");
        Headers headers = exchange.getRequestHeaders();
        String host = headers.getFirst("Host");
        require(
                host.equals(step.path("host").asText())
                        && "POST".equals(exchange.getRequestMethod())
                        && "/".equals(exchange.getRequestURI().getRawPath())
                        && exchange.getRequestURI().getRawQuery() == null,
                "Unexpected regional request path");
        require(
                CONTENT.equals(headers.getFirst("Content-Type"))
                        && TARGET.equals(headers.getFirst("X-Amz-Target")),
                "Unexpected RPC header");
        byte[] body = exchange.getRequestBody().readNBytes(65537);
        require(body.length < 65537, "Unexpected lookup request size");
        JsonNode parsed = JSON.readTree(body);
        ObjectNode expected = step.path("request").deepCopy();
        if (step.path("implicitWindow").asBoolean()) {
            expected.set("StartTime", parsed.path("StartTime"));
            expected.set("EndTime", parsed.path("EndTime"));
        }
        require(parsed.equals(expected), "Lookup parameters changed");
        require(
                parsed.path("StartTime").isIntegralNumber()
                        && parsed.path("EndTime").isIntegralNumber(),
                "Date bounds must be integers");
        verifySignature(headers, body, step);
        ObjectNode result =
                JSON.createObjectNode()
                        .put("host", host)
                        .put("nanoTime", System.nanoTime())
                        .put("temporaryCredentials", step.path("temporaryCredentials").asBoolean());
        result.set("request", parsed);
        Files.writeString(
                base.resolve(name + ".requests"),
                result + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
        return step;
    }

    private static void verifySignature(Headers headers, byte[] data, JsonNode step) {
        boolean temporary = step.path("temporaryCredentials").asBoolean();
        String provided = headers.getFirst("X-Amz-Security-Token");
        require(
                temporary ? TOKEN.equals(provided) : provided == null,
                "Unexpected temporary credential header");
        String region = step.path("region").asText();
        String dateTime = headers.getFirst("X-Amz-Date");
        require(
                dateTime != null && dateTime.matches("[0-9]{8}T[0-9]{6}Z"),
                "Invalid signature date");
        Instant timestamp = LocalDateTime.parse(dateTime, STAMP).toInstant(ZoneOffset.UTC);
        require(
                Math.abs(Instant.now().getEpochSecond() - timestamp.getEpochSecond()) <= 300,
                "Stale signature");
        String actual = headers.getFirst("Authorization");
        require(actual != null, "Missing Authorization");
        byte[] expected =
                expected(headers, data, region, step.path("access").asText(), temporary)
                        .getBytes(StandardCharsets.US_ASCII);
        require(
                MessageDigest.isEqual(expected, actual.getBytes(StandardCharsets.US_ASCII)),
                "Signature does not bind exact request bytes and AWS scope");
    }

    private static String expected(
            Headers headers, byte[] data, String region, String key, boolean temporary) {
        String dateTime = headers.getFirst("X-Amz-Date");
        String date = dateTime.split("T", 2)[0];
        String scope = String.join("/", date, region, "cloudtrail", "aws4_request");
        List<String> required =
                new ArrayList<>(
                        List.of("content-type", "host", "x-amz-content-sha256", "x-amz-date"));
        if (temporary) {
            required.add("x-amz-security-token");
        }
        required.add("x-amz-target");
        String digest = hex(hash(data));
        require(
                digest.equals(headers.getFirst("X-Amz-Content-Sha256")),
                "Payload hash does not match packet bytes");
        StringBuilder normalized = new StringBuilder();
        for (String header : required) {
            List<String> value = headers.get(header);
            require(value != null && value.size() == 1, "Missing or repeated signed header");
            normalized.append(header).append(':').append(value.get(0).strip()).append('\n');
        }
        String signedFields = String.join(";", required);
        String request =
                String.join("\n", "POST", "/", "", normalized.toString(), signedFields, digest);
        String algorithm = "AWS4-HMAC-SHA256";
        String signedData =
                String.join("\n", algorithm, dateTime, scope, hex(hash(bytes(request))));
        byte[] signing = bytes("AWS4" + SECRET);
        for (String level : scope.split("/")) {
            signing = mac(signing, bytes(level));
        }
        return algorithm
                + " Credential="
                + key
                + "/"
                + scope
                + ", SignedHeaders="
                + signedFields
                + ", Signature="
                + hex(mac(signing, bytes(signedData)));
    }

    private void trickle(String name, HttpExchange exchange)
            throws IOException, InterruptedException {
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream stream = exchange.getResponseBody()) {
            stream.write('{');
            for (int i = 0; i < 400; i++) {
                stream.write(' ');
                stream.flush();
                Thread.sleep(60);
            }
        } catch (IOException closed) {
            Files.writeString(base.resolve(name + ".disconnect"), "closed");
        }
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] hash(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (GeneralSecurityException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static byte[] mac(byte[] key, byte[] input) {
        try {
            Mac crypto = Mac.getInstance("HmacSHA256");
            crypto.init(new SecretKeySpec(key, crypto.getAlgorithm()));
            return crypto.doFinal(input);
        } catch (GeneralSecurityException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder text = new StringBuilder();
        for (byte value : bytes) {
            text.append(String.format("%02x", Byte.toUnsignedInt(value)));
        }
        return text.toString();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
