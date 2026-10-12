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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.client;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Shard;
import org.apache.hugegraph.structure.graph.Vertex;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface HugeGraphOperations {

    Set<String> getVertexLabelPropertiesOrNull(String label);

    Set<String> getEdgeLabelPropertiesOrNull(String label);

    /** Lists the names of all vertex labels defined in the graph schema. */
    List<String> listVertexLabels();

    /** Lists the names of all edge labels defined in the graph schema. */
    List<String> listEdgeLabels();

    DataType getPropertyDataType(String propertyName);

    Cardinality getPropertyCardinality(String propertyName);

    /**
     * Lists one page of vertices of {@code label}. When {@code filter} is non-empty its entries are
     * applied server-side as property-equality conditions; null/empty means no filtering.
     */
    PageResult<Vertex> listVertices(
            String label, Map<String, Object> filter, String page, int limit);

    /**
     * Lists one page of edges of {@code label}. See {@link #listVertices} for the filter contract.
     */
    PageResult<Edge> listEdges(String label, Map<String, Object> filter, String page, int limit);

    /**
     * Splits the vertex keyspace into shards of approximately {@code splitSize} bytes each, for
     * parallel scanning. Requires a backend that supports scan (RocksDB / HBase / Cassandra); the
     * memory backend does not.
     */
    List<Shard> vertexShards(long splitSize);

    /** Splits the edge keyspace into shards. See {@link #vertexShards}. */
    List<Shard> edgeShards(long splitSize);

    /**
     * Scans one page of vertices within {@code shard}. Unlike {@link #listVertices}, the scan is by
     * key range and returns vertices of ALL labels in the range, so the caller must filter by label
     * client-side; server-side property filters are not supported here.
     */
    PageResult<Vertex> scanVertices(Shard shard, String page, int limit);

    /** Scans one page of edges within {@code shard}. See {@link #scanVertices}. */
    PageResult<Edge> scanEdges(Shard shard, String page, int limit);

    void close();
}
