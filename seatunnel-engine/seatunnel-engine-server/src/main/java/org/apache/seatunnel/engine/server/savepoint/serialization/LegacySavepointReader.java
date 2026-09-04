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

package org.apache.seatunnel.engine.server.savepoint.serialization;

import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;

/**
 * Best-effort reader for legacy checkpoints/savepoints written by engine versions that stored the
 * runtime {@link CompletedCheckpoint} bytes directly (format {@code legacy-v0}, no version marker).
 *
 * <p>The legacy format has no bundle manifest and no version contract: reading it relies on the
 * current runtime class layout, so it is <b>best-effort and terminating</b>. The committed {@code
 * savepoint-wire/legacy-v0} fixtures pin the expected wire layout; a future runtime model change
 * must keep this reader (and those fixtures) working or declare the legacy format retired.
 */
public final class LegacySavepointReader {

    private static final ProtoStuffSerializer SERIALIZER = new ProtoStuffSerializer();

    private LegacySavepointReader() {}

    /** Decodes legacy-v0 bytes into the current wire DTO. */
    public static WireSavepoint read(byte[] data) {
        CompletedCheckpoint checkpoint = SERIALIZER.deserialize(data, CompletedCheckpoint.class);
        return SavepointWireCodec.fromCompletedCheckpoint(checkpoint);
    }
}
