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

package org.apache.seatunnel.engine.checkpoint.storage.savepoint;

import io.protostuff.Tag;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Commit metadata of one savepoint bundle ({@code _metadata.ser}).
 *
 * <p>This DTO is part of the storage-facing public contract and must not import engine runtime
 * classes. Field numbers are frozen by {@link Tag}: only append new fields with new numbers.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SavepointMeta {

    /** Savepoint bundle format version ({@link SavepointStorageConstants#FORMAT_VERSION}). */
    @Tag(1)
    private int formatVersion;

    /** Globally unique savepoint id (not the per-pipeline checkpoint id). */
    @Tag(2)
    private String savepointId;

    /** Logical job id; the restore key. */
    @Tag(3)
    private String jobId;

    /** Engine producer version; diagnostics only, never a compatibility decision. */
    @Tag(4)
    private String producerVersion;

    @Tag(5)
    private long triggerTimestamp;

    /** Manifest entries, one per pipeline, sorted by pipeline id. */
    @Tag(6)
    private List<SavepointManifestEntry> pipelines;

    /** SHA-256 over the canonical manifest representation. */
    @Tag(7)
    private String manifestChecksum;
}
