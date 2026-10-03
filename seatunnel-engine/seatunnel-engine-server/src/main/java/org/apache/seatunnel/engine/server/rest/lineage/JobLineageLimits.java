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

package org.apache.seatunnel.engine.server.rest.lineage;

import lombok.Getter;

/** Structural, string and byte bounds applied when projecting a job lineage graph. */
@Getter
public final class JobLineageLimits {

    public static final JobLineageLimits DEFAULT =
            new JobLineageLimits(10_000, 50_000, 50_000, 4 * 1024, 8 * 1024 * 1024);

    /** Maximum number of vertices in the graph. */
    private final int maxNodes;

    /** Maximum number of edges across all pipelines, counted before deduplication. */
    private final int maxEdges;

    /**
     * Maximum number of reported table path entries across all vertices, counted as reported:
     * before null or default entries are dropped, before deduplication, and including entries on
     * transforms, which are not written.
     */
    private final int maxTablePaths;

    /** Maximum UTF-8 length of one display name or table path. */
    private final int maxStringBytes;

    /** Maximum UTF-8 length of the serialized response. */
    private final int maxResponseBytes;

    public JobLineageLimits(
            int maxNodes,
            int maxEdges,
            int maxTablePaths,
            int maxStringBytes,
            int maxResponseBytes) {
        if (maxNodes <= 0
                || maxEdges <= 0
                || maxTablePaths <= 0
                || maxStringBytes <= 0
                || maxResponseBytes <= 0) {
            throw new IllegalArgumentException("Job lineage limits must be positive");
        }
        this.maxNodes = maxNodes;
        this.maxEdges = maxEdges;
        this.maxTablePaths = maxTablePaths;
        this.maxStringBytes = maxStringBytes;
        this.maxResponseBytes = maxResponseBytes;
    }
}
