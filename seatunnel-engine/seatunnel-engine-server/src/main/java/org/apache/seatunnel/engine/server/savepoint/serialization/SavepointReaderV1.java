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

import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointManifestEntry;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointMeta;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;

import java.util.HashMap;
import java.util.Map;

/**
 * Reader for format version 1 ({@code engine-wire-v1} payloads).
 *
 * <p>After decoding, the payload's job/pipeline/checkpoint ids are cross-checked against the bundle
 * metadata so a tampered or misplaced payload fails explicitly instead of restoring with the wrong
 * ids.
 */
public class SavepointReaderV1 implements SavepointReader {

    @Override
    public Map<Integer, CompletedCheckpoint> read(
            SavepointMeta meta, Map<Integer, byte[]> pipelinePayloads) {
        Map<Integer, SavepointManifestEntry> manifestByPipeline = new HashMap<>();
        if (meta.getPipelines() != null) {
            for (SavepointManifestEntry entry : meta.getPipelines()) {
                manifestByPipeline.put(entry.getPipelineId(), entry);
            }
        }
        Map<Integer, CompletedCheckpoint> checkpoints = new HashMap<>();
        pipelinePayloads.forEach(
                (pipelineId, payload) -> {
                    WireSavepoint wire = SavepointWireCodec.decode(payload);
                    CompletedCheckpoint checkpoint = SavepointWireCodec.toCompletedCheckpoint(wire);
                    SavepointManifestEntry entry = manifestByPipeline.get(pipelineId);
                    if (entry == null) {
                        throw new SavepointIncompatibleException(
                                meta.getSavepointId(),
                                meta.getFormatVersion(),
                                "payload for pipeline "
                                        + pipelineId
                                        + " is not declared in the manifest");
                    }
                    if (!String.valueOf(wire.getJobId()).equals(meta.getJobId())
                            || wire.getPipelineId() != entry.getPipelineId()
                            || wire.getCheckpointId() != entry.getCheckpointId()) {
                        throw new SavepointIncompatibleException(
                                meta.getSavepointId(),
                                meta.getFormatVersion(),
                                "payload id mismatch for pipeline "
                                        + pipelineId
                                        + ": wire job "
                                        + wire.getJobId()
                                        + "/pipeline "
                                        + wire.getPipelineId()
                                        + "/checkpoint "
                                        + wire.getCheckpointId()
                                        + " vs manifest job "
                                        + meta.getJobId()
                                        + "/pipeline "
                                        + entry.getPipelineId()
                                        + "/checkpoint "
                                        + entry.getCheckpointId());
                    }
                    checkpoints.put(pipelineId, checkpoint);
                });
        return checkpoints;
    }
}
