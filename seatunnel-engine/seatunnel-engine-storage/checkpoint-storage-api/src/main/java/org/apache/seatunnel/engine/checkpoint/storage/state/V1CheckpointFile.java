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

package org.apache.seatunnel.engine.checkpoint.storage.state;

import lombok.Value;

import java.io.Serializable;

/**
 * A v1 checkpoint file ({@code <jobId>/<writeTimestamp>-<random>-<pipelineId>-<checkpointId>.ser}),
 * identified from its name only, without reading it.
 */
@Value
public class V1CheckpointFile implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Path relative to the storage namespace. */
    String location;

    String jobId;

    String pipelineId;

    long checkpointId;

    /** The write time encoded in the file name, which orders v1 checkpoints. */
    long writeTimestamp;
}
