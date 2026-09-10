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
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;

import java.io.IOException;

/** Applies schema changes to an external sink system without managing writer-local state. */
@Experimental
public interface SchemaChangeApplier extends AutoCloseable {

    /**
     * Applies or reconciles one external schema change.
     *
     * <p>The same event can be delivered again after recovery. Implementations must treat an
     * already-applied compatible change as success and fail when the external schema conflicts with
     * the requested change.
     *
     * <p>Table, column, and other identifiers carried by the event must be treated as untrusted
     * input. Implementations must validate and quote every event-derived identifier through the
     * sink system's dialect or identifier API and must not concatenate raw identifiers into DDL.
     */
    void apply(SchemaChangeEvent event) throws IOException;

    @Override
    default void close() throws IOException {}
}
