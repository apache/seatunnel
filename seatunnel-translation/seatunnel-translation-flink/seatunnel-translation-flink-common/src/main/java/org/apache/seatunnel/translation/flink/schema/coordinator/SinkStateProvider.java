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

package org.apache.seatunnel.translation.flink.schema.coordinator;

import org.apache.seatunnel.api.table.catalog.TableIdentifier;

/**
 * Interface for sink subtasks to provide their schema processing state This allows the coordinator
 * to query the actual processing state during recovery
 */
public interface SinkStateProvider {
    /**
     * Get the last processed epoch for a specific table
     *
     * @param tableId the table identifier
     * @return the last processed epoch, or null if never processed
     */
    Long getLastProcessedEpoch(TableIdentifier tableId);
}
