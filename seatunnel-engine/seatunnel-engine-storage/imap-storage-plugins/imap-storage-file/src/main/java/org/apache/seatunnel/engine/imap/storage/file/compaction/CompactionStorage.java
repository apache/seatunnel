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

package org.apache.seatunnel.engine.imap.storage.file.compaction;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.wal.reader.IFileReader.RecordConsumer;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;

/** Storage-specific publication and reclamation, separate from input selection and reduction. */
public interface CompactionStorage {
    /** Streams a completed input without deserializing its user key/value payloads. */
    void read(Path input, RecordConsumer<IMapFileData> consumer) throws IOException;

    /**
     * Publishes a complete immutable snapshot before returning its path. On failure the original
     * inputs must remain authoritative. Implementations must also supply a compatible replay
     * protocol.
     */
    Path publish(CompactionPlan plan, Collection<IMapFileData> mutations) throws IOException;

    /**
     * Deletes one replaced input after its replacement has been published. Missing inputs are
     * harmless.
     */
    void delete(Path input) throws IOException;
}
