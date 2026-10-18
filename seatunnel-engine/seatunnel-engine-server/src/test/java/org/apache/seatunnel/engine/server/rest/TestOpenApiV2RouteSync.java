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

package org.apache.seatunnel.engine.server.rest;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.server.diagnostic.WorkerResourceDiagnostic;
import org.apache.seatunnel.engine.server.diagnostic.WorkerResourceSnapshot;
import org.apache.seatunnel.engine.server.resourcemanager.resource.OverviewInfo;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.gson.Gson;
import com.hazelcast.internal.util.JsonUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestOpenApiV2RouteSync {

    private static final Path SOURCE = Path.of("src/main/java/org/apache/seatunnel/engine/server");
    private static final Path SPEC = Path.of("../../docs/en/engines/zeta/openapi-v2.yaml");
    private static final Pattern CONSTANT =
            Pattern.compile("public static final String (REST_URL_[A-Z_]+)\\s*=\\s*\"([^\"]+)\"");
    private static final Pattern REGISTRATION =
            Pattern.compile(
                    "context\\.addServlet\\(\\s*(\\w+)\\s*,\\s*convertUrlToPath\\((REST_URL_[A-Z_]+)\\)\\s*\\);",
                    Pattern.DOTALL);
    private static final Pattern HOLDER =
            Pattern.compile(
                    "ServletHolder\\s+(\\w+)\\s*=\\s*new ServletHolder\\(new (\\w+)\\(",
                    Pattern.DOTALL);
    private static final Map<String, String> COVERED_ROUTES =
            Map.of(
                    "REST_URL_OVERVIEW", "OverviewServlet",
                    "REST_URL_WORKER_RESOURCES", "WorkerResourceServlet");
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void selectedGetRoutesStayRegisteredAndDocumented() throws IOException {
        JsonNode spec = new YAMLMapper().readTree(SPEC.toFile());
        assertEquals("3.1.0", spec.path("openapi").asText());
        assertEquals(Set.of("/overview", "/resource/workers"), fieldNames(spec.path("paths")));

        String constants = read(SOURCE.resolve("rest/RestConstant.java"));
        String registrations = read(SOURCE.resolve("JettyService.java"));
        Map<String, String> pathsByConstant = new HashMap<>();
        Matcher constantMatcher = CONSTANT.matcher(constants);
        while (constantMatcher.find()) {
            pathsByConstant.put(constantMatcher.group(1), constantMatcher.group(2));
        }
        Map<String, String> classesByHolder = new HashMap<>();
        Matcher holderMatcher = HOLDER.matcher(registrations);
        while (holderMatcher.find()) {
            classesByHolder.put(holderMatcher.group(1), holderMatcher.group(2));
        }
        Map<String, String> registeredClasses = new HashMap<>();
        Matcher registrationMatcher = REGISTRATION.matcher(registrations);
        while (registrationMatcher.find()) {
            if (COVERED_ROUTES.containsKey(registrationMatcher.group(2))) {
                registeredClasses.put(
                        registrationMatcher.group(2),
                        classesByHolder.get(registrationMatcher.group(1)));
            }
        }
        assertEquals(COVERED_ROUTES, registeredClasses);
        for (Map.Entry<String, String> route : COVERED_ROUTES.entrySet()) {
            String path = pathsByConstant.get(route.getKey());
            JsonNode operation = spec.path("paths").path(path);
            assertEquals(Set.of("get"), fieldNames(operation), path);
            assertTrue(operation.path("get").path("responses").has("200"), path);
            assertTrue(
                    read(SOURCE.resolve("rest/servlet/" + route.getValue() + ".java"))
                            .contains("void doGet("),
                    route.getValue() + " no longer handles GET");
        }
        assertEquals(
                Set.of("basicAuth"), fieldNames(spec.path("components").path("securitySchemes")));
        assertTrue(spec.path("security").toString().contains("{}"));
    }

    @Test
    void responseSchemasMatchCurrentSerializedFields() throws IOException {
        JsonNode spec = new YAMLMapper().readTree(SPEC.toFile());
        JsonNode schemas = spec.path("components").path("schemas");
        OverviewInfo overview = new OverviewInfo();
        overview.setProjectVersion("3.0.1");
        overview.setGitCommitAbbrev("abc123");
        overview.setTotalSlot(2);
        overview.setWorkers(1);
        JsonNode overviewBody =
                JSON.readTree(
                        JsonUtil.toJsonObject(JsonUtils.toMap(JsonUtils.toJsonString(overview)))
                                .toString());
        JsonNode overviewSchema = schemas.path("Overview");
        assertEquals(fieldNames(overviewBody), fieldNames(overviewSchema.path("properties")));
        assertEquals(fieldNames(overviewBody), arrayValues(overviewSchema.path("required")));
        assertEquals("string", schemas.path("DecimalCountString").path("type").asText());
        for (String field : fieldNames(overviewBody)) {
            assertTrue(overviewBody.path(field).isTextual(), field + " must be a JSON string");
            JsonNode property = overviewSchema.path("properties").path(field);
            if (property.has("$ref")) {
                assertEquals(
                        "#/components/schemas/DecimalCountString", property.path("$ref").asText());
            } else {
                assertEquals("string", property.path("type").asText());
            }
        }

        WorkerResourceDiagnostic worker = new WorkerResourceDiagnostic();
        worker.setAddress("127.0.0.1:5801");
        worker.setTags(Map.of("region", "west"));
        worker.setTotalSlots(2);
        worker.setFreeSlots(1);
        worker.setUsedSlots(1);
        worker.setDynamicSlot(true);
        worker.setTotalCpuCores(4);
        worker.setAvailableCpuCores(2);
        worker.setTotalHeapMemoryBytes(1024L);
        worker.setAvailableHeapMemoryBytes(512L);
        worker.setCpuUsage(0.25);
        worker.setMemUsage(0.5);
        worker.setRunningJobIds(Collections.singletonList(42L));
        JsonNode snapshotBody =
                JSON.readTree(
                        new Gson()
                                .toJson(
                                        new WorkerResourceSnapshot(
                                                true, 1234L, Collections.singletonList(worker))));
        JsonNode snapshotSchema = schemas.path("WorkerResourceSnapshot");
        assertEquals(fieldNames(snapshotBody), fieldNames(snapshotSchema.path("properties")));
        assertEquals(fieldNames(snapshotBody), arrayValues(snapshotSchema.path("required")));
        for (String field : fieldNames(snapshotBody)) {
            assertType(
                    snapshotBody.path(field), snapshotSchema.path("properties").path(field), field);
        }
        JsonNode workerBody = snapshotBody.path("workers").get(0);
        JsonNode workerSchema = schemas.path("WorkerResourceDiagnostic");
        assertEquals(fieldNames(workerBody), fieldNames(workerSchema.path("properties")));
        assertTrue(fieldNames(workerBody).containsAll(arrayValues(workerSchema.path("required"))));
        for (String field : fieldNames(workerBody)) {
            assertType(workerBody.path(field), workerSchema.path("properties").path(field), field);
        }
        assertTrue(workerBody.path("tags").path("region").isTextual());
        assertTrue(workerBody.path("runningJobIds").get(0).isIntegralNumber());
    }

    private static void assertType(JsonNode value, JsonNode schema, String field) {
        switch (schema.path("type").asText()) {
            case "string":
                assertTrue(value.isTextual(), field);
                break;
            case "integer":
                assertTrue(value.isIntegralNumber(), field);
                break;
            case "number":
                assertTrue(value.isNumber(), field);
                break;
            case "boolean":
                assertTrue(value.isBoolean(), field);
                break;
            case "object":
                assertTrue(value.isObject(), field);
                break;
            case "array":
                assertTrue(value.isArray(), field);
                break;
            default:
                throw new AssertionError("Unexpected schema type for " + field);
        }
    }

    private static Set<String> fieldNames(JsonNode node) {
        Set<String> names = new HashSet<>();
        Iterator<String> fields = node.fieldNames();
        fields.forEachRemaining(names::add);
        return names;
    }

    private static Set<String> arrayValues(JsonNode node) {
        Set<String> values = new HashSet<>();
        node.forEach(value -> values.add(value.asText()));
        return values;
    }

    private static String read(Path path) throws IOException {
        return Files.readString(path, StandardCharsets.UTF_8);
    }
}
