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

package org.apache.seatunnel.engine.server.common.statestore.cleanup;

import org.apache.seatunnel.engine.server.common.statestore.IterableStateStore;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.master.cleanup.PipelineCleanupRecord;

/**
 * Store for pending pipeline cleanup records.
 *
 * <p>This store keeps cleanup work items that may need to survive active-master switches so the
 * next coordinator can continue cleanup attempts.
 *
 * <p>It extends {@link IterableStateStore} because the coordinator needs to iterate over all
 * pending cleanup records, while also exposing conditional update operations used by the current
 * cleanup flow.
 */
public interface PendingPipelineCleanupStore
        extends IterableStateStore<PipelineLocation, PipelineCleanupRecord> {

    /**
     * Replaces the current record only if it is equal to the expected record.
     *
     * @param pipelineLocation pipeline identifier
     * @param expected expected current record
     * @param updated replacement record
     * @return {@code true} if the record was replaced, {@code false} otherwise
     */
    boolean replace(
            PipelineLocation pipelineLocation,
            PipelineCleanupRecord expected,
            PipelineCleanupRecord updated);

    /**
     * Removes the current record only if it is equal to the expected record.
     *
     * @param pipelineLocation pipeline identifier
     * @param expected expected current record
     * @return {@code true} if the record was removed, {@code false} otherwise
     */
    boolean remove(PipelineLocation pipelineLocation, PipelineCleanupRecord expected);
}
