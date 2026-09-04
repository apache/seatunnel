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

/** Manifest entry of one pipeline payload inside a savepoint bundle. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SavepointManifestEntry {

    @Tag(1)
    private int pipelineId;

    @Tag(2)
    private long checkpointId;

    /** File name relative to the savepoint directory. */
    @Tag(3)
    private String payloadFile;

    @Tag(4)
    private long payloadLength;

    /** SHA-256 hex of the payload bytes. */
    @Tag(5)
    private String payloadChecksum;

    /** Payload format, e.g. {@link SavepointStorageConstants#PAYLOAD_FORMAT_V1}. */
    @Tag(6)
    private String payloadFormat;
}
