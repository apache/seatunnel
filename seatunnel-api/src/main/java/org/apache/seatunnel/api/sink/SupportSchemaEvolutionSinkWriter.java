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

import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;

import java.io.IOException;
import java.util.Optional;

public interface SupportSchemaEvolutionSinkWriter {

    /**
     * apply schema change to third party data receiver.
     *
     * @param event
     * @throws IOException
     */
    void applySchemaChange(SchemaChangeEvent event) throws IOException;

    /**
     * Returns a stable identifier of the physical sink table this writer commits to. Multi-table
     * sinks that resolve a sink-table template per upstream table can end up with several writers
     * sharing one physical destination. When that happens, a schema change applied through one
     * sub-writer mutates the external table immediately while sibling sub-writers keep writing with
     * their stale in-memory schema unless the coordinator can fan the change out to all of them.
     *
     * <p>Writers that can share one physical destination should expose that resolved identifier
     * here. The default implementation returns {@link Optional#empty()} so connectors that do not
     * need shared-sink coordination keep the legacy source-only routing.
     */
    default Optional<String> getPhysicalSinkTableIdentifier() {
        return Optional.empty();
    }
}
