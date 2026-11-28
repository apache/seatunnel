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

package org.apache.seatunnel.e2e.connector.astra;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

/**
 * Implements an independent HTTPS Astra datastore and synthetic embedding providers. Each case
 * records API requests and persists the actual mutated documents outside the engine.
 */
public final class AstraPeer {
    static final String DATABASE = "test-db.us-east-2.apps.astra.datastax.com";
    static final String OPENAI = "api.openai.com";
    static final String AZURE = "test-deployment.openai.azure.com";
    static final String COHERE = "api.cohere.com";
    static final String COMPATIBLE = "compatible.astra-fixture.invalid";
    static final String PASSWORD = "astra-fixture-password";
    static final String DB_TOKEN = "fixture-db-token";
    static final String EMBED_TOKEN = "fixture-embed-token";
    static final String PRIVATE = "synthetic-private-provider-data";
    static final int TRUSTED = 18443;
    static final int UNTRUSTED = 18444;
    static final String KEYSPACE = "/api/json/v1/fixture";
    static final String COLLECTION = KEYSPACE + "/documents";
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path base;
    private final Map<String, Case> scenarios = new HashMap<>();

    private AstraPeer(Path base) {
        this.base = base;
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        AstraPeer peer = new AstraPeer(root);
        ExecutorService executor = Executors.newCachedThreadPool();
        HttpsServer trusted = peer.listen("trusted", TRUSTED, executor);
        HttpsServer untrusted = peer.listen("untrusted", UNTRUSTED, executor);
        try {
            Files.createFile(root.resolve("ready"));
            while (!Files.exists(root.resolve("stop"))) {
                Thread.sleep(50);
            }
        } finally {
            trusted.stop(0);
            untrusted.stop(0);
            executor.shutdownNow();
        }
    }

    private HttpsServer listen(String identity, int port, ExecutorService executor)
            throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        char[] password = PASSWORD.toCharArray();
        try (InputStream input = Files.newInputStream(base.resolve(identity + ".p12"))) {
            store.load(input, password);
        }
        KeyManagerFactory keys =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, password);
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(keys.getKeyManagers(), null, null);
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", port), 32);
        server.setHttpsConfigurator(new HttpsConfigurator(ssl));
        server.createContext("/", this::respond);
        server.setExecutor(executor);
        server.start();
        return server;
    }

    private synchronized void respond(HttpExchange exchange) throws IOException {
        String name = Files.readString(base.resolve("active")).trim();
        try {
            Case active = scenarios.computeIfAbsent(name, this::load);
            String host = exchange.getRequestHeaders().getFirst("Host").replaceFirst(":\\d+$", "");
            String path = exchange.getRequestURI().getRawPath();
            byte[] request = exchange.getRequestBody().readNBytes(8388609);
            require(
                    request.length <= 8388608
                            && "POST".equals(exchange.getRequestMethod())
                            && exchange.getRequestURI().getRawQuery() == null,
                    "Unexpected method or request shape");
            JsonNode body = JSON.readTree(request);
            if (DATABASE.equals(host)) {
                require(
                        DB_TOKEN.equals(exchange.getRequestHeaders().getFirst("Token"))
                                && exchange.getRequestHeaders().getFirst("api-key") == null
                                && exchange.getRequestHeaders().getFirst("Authorization") == null,
                        "Wrong Astra header");
                require(
                        body.isObject() && body.size() == 1,
                        "Expected a single Data API operation");
                String command = body.fieldNames().next();
                boolean collection =
                        "findCollections".equals(command) || "createCollection".equals(command);
                require((collection ? KEYSPACE : COLLECTION).equals(path), "Wrong Data API path");
                report(active, host, path, command, body);
                int count = active.calls.merge(command, 1, Integer::sum);
                switch (command) {
                    case "findCollections":
                        findCollections(exchange, active, body.path(command));
                        return;
                    case "createCollection":
                        create(exchange, active, body.path(command));
                        return;
                    case "insertMany":
                        insert(exchange, active, body.path(command), count);
                        return;
                    case "deleteMany":
                        delete(exchange, active, body.path(command));
                        return;
                    case "findOne":
                        findOne(exchange, active, body.path(command));
                        return;
                    default:
                        throw new IllegalArgumentException(
                                "Unexpected Data API operation " + command);
                }
            }
            embed(exchange, active, host, path, body);
        } catch (IllegalArgumentException e) {
            Files.writeString(base.resolve(name + ".invalid"), e.getMessage());
            send(exchange, 422, error("FIXTURE_CONTRACT_VIOLATION"));
        } finally {
            exchange.close();
        }
    }

    private Case load(String name) {
        try {
            JsonNode json = JSON.readTree(Files.readAllBytes(base.resolve(name + ".input")));
            Case active = new Case(name, json);
            persist(active);
            return active;
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read fixture case", e);
        }
    }

    private void findCollections(HttpExchange exchange, Case active, JsonNode query)
            throws IOException {
        require(query.path("options").path("explain").asBoolean(), "Detailed lookup required");
        if ("redirect".equals(active.fault)) {
            exchange.getResponseHeaders()
                    .set(
                            "Location",
                            "https://"
                                    + COMPATIBLE
                                    + ":"
                                    + TRUSTED
                                    + "/unexpected?secret="
                                    + PRIVATE);
            send(exchange, 302, error("FIXTURE_REDIRECT"));
            return;
        }
        if ("read-error".equals(active.fault)) {
            send(exchange, 200, error("AUTHORIZATION_FAILED"));
            return;
        }
        if ("slow".equals(active.fault)) {
            slow(exchange, active);
            return;
        }
        if ("oversized-response".equals(active.fault)) {
            send(exchange, 200, JSON.createObjectNode().put("padding", "a".repeat(4000)));
            return;
        }
        ObjectNode response = JSON.createObjectNode();
        ArrayNode listing = response.putObject("status").putArray("collections");
        if (active.exists) {
            listing.addObject()
                    .put("name", "documents")
                    .putObject("options")
                    .putObject("vector")
                    .put("dimension", active.dimensions)
                    .put("metric", active.metric);
        }
        send(exchange, 200, response);
    }

    private static void create(HttpExchange exchange, Case active, JsonNode query)
            throws IOException {
        JsonNode vector = query.path("options").path("vector");
        require(
                !active.exists
                        && "documents".equals(query.path("name").asText())
                        && active.dimensions == vector.path("dimension").asInt()
                        && "cosine".equals(vector.path("metric").asText())
                        && vector.size() == 2,
                "Wrong collection options");
        active.exists = true;
        ObjectNode result = JSON.createObjectNode();
        result.putObject("status").put("ok", 1);
        send(exchange, 200, result);
    }

    private void insert(HttpExchange exchange, Case active, JsonNode command, int call)
            throws IOException {
        JsonNode documents = command.path("documents");
        require(
                active.exists
                        && documents.isArray()
                        && documents.size() > 0
                        && documents.size() <= 20
                        && !command.path("options").path("ordered").asBoolean(true),
                "Wrong insertMany payload");
        if ("throttled".equals(active.fault) || "throttle-once".equals(active.fault) && call == 1) {
            exchange.getResponseHeaders().set("Retry-After", "0");
            send(exchange, 429, error("TOO_MANY_REQUESTS"));
            return;
        }
        ObjectNode result = JSON.createObjectNode();
        ArrayNode ids = result.putObject("status").putArray("insertedIds");
        if ("false-ack".equals(active.fault)) {
            ids.add("not-a-requested-id");
            send(exchange, 200, result);
            return;
        }
        ArrayNode errors = JSON.createArrayNode();
        for (JsonNode doc : documents) {
            require(
                    doc.isObject()
                            && !doc.has("$vectorize")
                            && doc.path("$vector").size() == active.dimensions
                            && doc.path("_id").isTextual()
                            && doc.path("_st_scope").isTextual()
                            && doc.path("_st_payload_hash").isTextual(),
                    "Wrong persisted document structure");
            String id = doc.get("_id").asText();
            if (call == 1 && active.fault.startsWith("duplicate-")) {
                ObjectNode stored = doc.deepCopy();
                if ("duplicate-conflict".equals(active.fault)) {
                    stored.put("_st_payload_hash", "different-value");
                }
                active.documents.put(id, stored);
            }
            if (active.documents.putIfAbsent(id, doc) == null) {
                ids.add(id);
            } else {
                errors.addObject()
                        .put("errorCode", "DOCUMENT_ALREADY_EXISTS")
                        .put("message", PRIVATE);
            }
            if ("partial".equals(active.fault) || "unknown".equals(active.fault)) {
                errors.addObject()
                        .put("errorCode", "SHRED_DOC_LIMIT_REACHED")
                        .put("message", PRIVATE);
                break;
            }
        }
        persist(active);
        if ("unknown".equals(active.fault)) {
            return;
        }
        if (!errors.isEmpty()) {
            result.set("errors", errors);
        }
        send(exchange, 200, result);
    }

    private void delete(HttpExchange exchange, Case active, JsonNode command) throws IOException {
        JsonNode filter = command.path("filter");
        require(
                filter.isObject()
                        && filter.path("_st_scope").isTextual()
                        && (filter.size() == 1
                                || filter.size() == 2 && filter.path("_st_record_id").isTextual()),
                "Unfiltered or undesired deletion attempted");
        ObjectNode result = JSON.createObjectNode();
        ObjectNode status = result.putObject("status");
        if ("delete-stall".equals(active.fault)) {
            status.put("deletedCount", 0).put("moreData", true);
            send(exchange, 200, result);
            return;
        }
        String scope = filter.path("_st_scope").asText();
        String record = filter.has("_st_record_id") ? filter.path("_st_record_id").asText() : null;
        Predicate<JsonNode> matches =
                row ->
                        scope.equals(row.path("_st_scope").asText())
                                && (record == null
                                        || record.equals(row.path("_st_record_id").asText()));
        List<String> found = new ArrayList<>();
        active.documents.forEach(
                (id, row) -> {
                    if (matches.test(row)) {
                        found.add(id);
                    }
                });
        int count = Math.min(found.size(), 20);
        found.subList(0, count).forEach(active.documents::remove);
        status.put("deletedCount", count);
        if (found.size() > count) {
            status.put("moreData", true);
        }
        persist(active);
        send(exchange, 200, result);
    }

    private static void findOne(HttpExchange exchange, Case active, JsonNode command)
            throws IOException {
        String id = command.path("filter").path("_id").asText();
        require(
                !id.isEmpty() && command.path("projection").path("_st_payload_hash").asInt() == 1,
                "Wrong readback filter");
        ObjectNode result = JSON.createObjectNode();
        ObjectNode data = result.putObject("data");
        JsonNode document = active.documents.get(id);
        if (document == null) {
            data.putNull("document");
        } else {
            data.putObject("document")
                    .put("_id", id)
                    .set("_st_payload_hash", document.path("_st_payload_hash"));
        }
        send(exchange, 200, result);
    }

    private void embed(HttpExchange exchange, Case active, String host, String path, JsonNode body)
            throws IOException {
        boolean azure = AZURE.equals(host);
        boolean cohere = COHERE.equals(host);
        boolean compatible = COMPATIBLE.equals(host);
        require(
                azure || cohere || compatible || OPENAI.equals(host),
                "Unexpected provider hostname");
        String expected =
                azure
                        ? "/openai/v1/embeddings"
                        : cohere
                                ? "/v2/embed"
                                : compatible ? "/private/v1/embeddings" : "/v1/embeddings";
        require(
                expected.equals(path) && exchange.getRequestHeaders().getFirst("Token") == null,
                "Wrong embedding path or leaked Astra token");
        String bearer = exchange.getRequestHeaders().getFirst("Authorization");
        String key = exchange.getRequestHeaders().getFirst("api-key");
        boolean anonymous = compatible && "anonymous".equals(active.fault);
        require(
                azure
                        ? EMBED_TOKEN.equals(key) && bearer == null
                        : key == null
                                && (anonymous
                                        ? bearer == null
                                        : ("Bearer " + EMBED_TOKEN).equals(bearer)),
                "Wrong embedding credentials");
        String model =
                azure
                        ? "fixture-deployment"
                        : cohere
                                ? "embed-english-v3.0"
                                : compatible
                                        ? "fixture-openai-compatible"
                                        : "text-embedding-ada-002";
        require(model.equals(body.path("model").asText()), "Wrong embedding model");
        JsonNode texts = body.path(cohere ? "texts" : "input");
        require(texts.isArray() && texts.size() > 0 && texts.size() <= 16, "Wrong text batch");
        if (cohere) {
            require(
                    "search_document".equals(body.path("input_type").asText())
                            && "NONE".equals(body.path("truncate").asText())
                            && "float".equals(body.path("embedding_types").path(0).asText()),
                    "Wrong Cohere request");
        }
        for (JsonNode text : texts) {
            require(text.isTextual() && !text.asText().isBlank(), "Empty text");
        }
        report(active, host, path, "embedding", body);
        int call = active.calls.merge("embedding", 1, Integer::sum);
        if ("embed-throttle".equals(active.fault) && call == 1) {
            exchange.getResponseHeaders().set("Retry-After", "0");
            send(exchange, 429, error("TOO_MANY_REQUESTS"));
            return;
        }
        ObjectNode result = JSON.createObjectNode();
        ArrayNode vectors =
                cohere ? result.putObject("embeddings").putArray("float") : result.putArray("data");
        for (int i = texts.size() - 1; i >= 0; i--) {
            if (cohere) {
                vectors.add(embedding(texts.get(texts.size() - 1 - i).asText(), active.dimensions));
            } else {
                vectors.addObject()
                        .put("index", "embed-bad-index".equals(active.fault) ? 0 : i)
                        .set("embedding", embedding(texts.get(i).asText(), active.dimensions));
            }
        }
        send(exchange, 200, result);
    }

    private static ArrayNode embedding(String text, int dimensions) {
        ArrayNode vector = JSON.createArrayNode();
        vector.add(text.endsWith("alpha") ? 0.5 : -0.5);
        for (int i = 1; i < dimensions; i++) {
            vector.add(i == dimensions - 1 ? 0.25 : 0.0);
        }
        return vector;
    }

    private void report(Case active, String host, String path, String type, JsonNode body)
            throws IOException {
        ObjectNode record =
                JSON.createObjectNode().put("host", host).put("path", path).put("operation", type);
        record.set("body", body);
        Files.writeString(
                base.resolve(active.name + ".requests"),
                record + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    private void persist(Case active) throws IOException {
        ArrayNode array = JSON.createArrayNode();
        active.documents.values().forEach(array::add);
        Path temporary = base.resolve(active.name + ".next");
        Files.writeString(temporary, array.toString());
        Files.move(
                temporary,
                base.resolve(active.name + ".documents"),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
    }

    private void slow(HttpExchange exchange, Case active) throws IOException {
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream stream = exchange.getResponseBody()) {
            stream.write('{');
            for (int i = 0; i < 400; i++) {
                stream.write(' ');
                stream.flush();
                Thread.sleep(60);
            }
        } catch (IOException closed) {
            Files.writeString(base.resolve(active.name + ".disconnect"), "connection closed");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void send(HttpExchange exchange, int code, ObjectNode content)
            throws IOException {
        byte[] bytes = content.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    private static ObjectNode error(String code) {
        ObjectNode result = JSON.createObjectNode();
        result.putArray("errors").addObject().put("errorCode", code).put("message", PRIVATE);
        return result;
    }

    private static void require(boolean test, String message) {
        if (!test) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Isolates each test's initial database settings, fault policy and independent stored
     * documents. Counters belong to this case and survive engine job restarts during the test.
     */
    private static final class Case {
        final String name;
        final int dimensions;
        final String metric;
        final String fault;
        final Map<String, JsonNode> documents = new LinkedHashMap<>();
        final Map<String, Integer> calls = new HashMap<>();
        boolean exists;

        Case(String name, JsonNode input) {
            this.name = name;
            this.dimensions = input.path("dimensions").asInt();
            this.exists = input.path("exists").asBoolean(true);
            this.metric = input.path("metric").asText("cosine");
            this.fault = input.path("fault").asText("");
            input.path("documents").forEach(doc -> documents.put(doc.path("_id").asText(), doc));
        }
    }
}
