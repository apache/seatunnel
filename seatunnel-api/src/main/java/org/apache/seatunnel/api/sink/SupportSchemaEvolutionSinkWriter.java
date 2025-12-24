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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.table.coordinator.SchemaCoordinator;
import org.apache.seatunnel.api.table.schema.event.FlushEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;

import java.io.IOException;

public interface SupportSchemaEvolutionSinkWriter {

    /**
     * apply schema change to third party data receiver.
     *
     * @param event
     * @throws IOException
     */
    void applySchemaChange(SchemaChangeEvent event) throws IOException;

    /**
     * handle FlushEvent propagated from upstream
     *
     * @param event
     * @throws IOException
     */
    default void handleFlushEvent(FlushEvent event) throws IOException {
        flushData();
        sendFlushSuccessful(event);
    }

    /**
     * send success event to coordinator upon successful flash
     *
     * @param event
     * @throws IOException
     */
    default void sendFlushSuccessful(FlushEvent event) throws IOException {
        SchemaCoordinator coordinator = getSchemaCoordinator();
        if (coordinator == null && event != null && event.getJobId() != null) {
            coordinator = SchemaCoordinator.getOrCreateInstance(event.getJobId());
        }

        if (coordinator != null) {
            coordinator.notifyFlushSuccessful(event.getJobId(), event.tableIdentifier());
        }
    }

    /**
     * Get the schema coordinator instance for reporting flush completion
     *
     * @return the schema coordinator instance, or null if not available
     */
    default SchemaCoordinator getSchemaCoordinator() {
        return null;
    }

    /**
     * flush data to other system
     *
     * @throws IOException
     */
    default void flushData() throws IOException {}
}
