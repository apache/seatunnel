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

package org.apache.seatunnel.engine.core.job;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.constants.PluginType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.json.JsonObject;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pins the null-safety contract of {@link JobDAGInfo#toJsonObject()}: a partially populated DAG
 * info (no-arg constructor, null edge vertex ids, null vertex type or table paths) must render as
 * empty structures or JSON null instead of throwing a {@link NullPointerException} on the REST
 * job-detail path.
 */
public class JobDAGInfoTest {

    /** Every field left null by the no-arg constructor must still serialize. */
    @Test
    public void testToJsonObjectWithAllFieldsNull() {
        JsonObject json = new JobDAGInfo().toJsonObject();

        Assertions.assertTrue(json.get("jobId").isNull());
        Assertions.assertEquals(0, json.get("pipelineEdges").asObject().size());
        Assertions.assertEquals(0, json.get("envOptions").asObject().size());
        Assertions.assertEquals(0, json.get("vertexInfoMap").asArray().size());
    }

    /** An edge whose vertex ids are null renders JSON null ids instead of failing. */
    @Test
    public void testToJsonObjectWithNullEdgeVertexIds() {
        Map<Integer, List<Edge>> edges = new HashMap<>();
        edges.put(1, Arrays.asList(new Edge(null, 2L), null));
        JobDAGInfo info = new JobDAGInfo(7L, null, edges, null, null, null);

        JsonObject json = info.toJsonObject();

        Assertions.assertEquals("7", json.get("jobId").asString());
        JsonObject edge = json.get("pipelineEdges").asObject().get("1").asArray().get(0).asObject();
        Assertions.assertTrue(edge.get("inputVertexId").isNull());
        Assertions.assertEquals("2", edge.get("targetVertexId").asString());
        Assertions.assertEquals(1, json.get("pipelineEdges").asObject().get("1").asArray().size());
    }

    /** Vertex infos with a null type, null table paths or a null entry must not fail. */
    @Test
    public void testToJsonObjectWithPartialVertexInfo() {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(1L, new VertexInfo(1L, null, "FakeSource", null));
        vertices.put(2L, null);
        vertices.put(
                3L,
                new VertexInfo(
                        3L,
                        PluginType.SINK,
                        "Console",
                        Collections.singletonList(TablePath.of("db", "tbl"))));
        JobDAGInfo info = new JobDAGInfo(1L, Collections.emptyMap(), null, vertices, null, null);

        JsonObject json = info.toJsonObject();

        Assertions.assertEquals(2, json.get("vertexInfoMap").asArray().size());
        JsonObject first = json.get("vertexInfoMap").asArray().get(0).asObject();
        Assertions.assertTrue(first.get("type").isNull());
        Assertions.assertEquals(0, first.get("tablePaths").asArray().size());
        JsonObject third = json.get("vertexInfoMap").asArray().get(1).asObject();
        Assertions.assertEquals("sink", third.get("type").asString());
        Assertions.assertEquals(
                TablePath.of("db", "tbl").toString(),
                third.get("tablePaths").asArray().get(0).asString());
    }
}
