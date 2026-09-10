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

import org.apache.seatunnel.api.annotation.Experimental;
import org.apache.seatunnel.api.table.catalog.TablePath;

import java.io.IOException;

/**
 * Sink capability for engine-coordinated external schema application.
 *
 * <p>Writers created by an implementation must also implement {@link
 * SupportSchemaRefreshSinkWriter}.
 */
@Experimental
public interface SupportCoordinatedSchemaEvolutionSink extends SupportSchemaEvolutionSink {

    /**
     * Returns whether this sink instance can use engine-coordinated schema evolution.
     *
     * <p>Composite sinks override this method when coordinated support depends on their children.
     */
    default boolean supportsCoordinatedSchemaEvolution() {
        return true;
    }

    /**
     * Creates an applier for a resolved physical sink table.
     *
     * <p>The engine creates and invokes only the applier selected for the coordinated schema
     * change. Other sink writers refresh their local state through {@link
     * SupportSchemaRefreshSinkWriter} without executing the external DDL.
     */
    SchemaChangeApplier createSchemaChangeApplier(TablePath sinkTablePath) throws IOException;
}
