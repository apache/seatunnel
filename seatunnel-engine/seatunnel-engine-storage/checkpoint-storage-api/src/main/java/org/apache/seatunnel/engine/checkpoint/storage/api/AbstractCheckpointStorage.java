/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.api;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.checkpoint.storage.common.Serializer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractCheckpointStorage implements CheckpointStorage {

    /**
     * serializer,default is protostuff,if necessary, consider other serialization methods, temporarily hard-coding
     */
    private final Serializer serializer = new ProtoStuffSerializer();

    /**
     * storage root directory
     */
    private static final String CHECKPOINT_DEFAULT_FILE_DIR = "/tmp/seatunnel/checkpoint/";

    public static final String FILE_NAME_SPLIT = "-";

    public static final int FILE_NAME_PIPELINE_ID_INDEX = 1;

    public static final int FILE_SORT_ID_INDEX = 0;

    public static final String FILE_FORMAT = "txt";

    /**
     * record order
     */
    private final AtomicLong counter = new AtomicLong(0);

    public String getStorageParentDirectory() {
        return CHECKPOINT_DEFAULT_FILE_DIR;
    }

    public String getCheckPointName(PipelineState state) {
        return counter.incrementAndGet() + FILE_NAME_SPLIT + state.getPipelineId() + FILE_NAME_SPLIT + state.getCheckpointId() + "." + FILE_FORMAT;
    }

    public byte[] serializeCheckPointData(PipelineState state) throws IOException {
        return serializer.serialize(state);
    }

    public PipelineState deserializeCheckPointData(byte[] data) throws IOException {
        return serializer.deserialize(data, PipelineState.class);
    }
}
