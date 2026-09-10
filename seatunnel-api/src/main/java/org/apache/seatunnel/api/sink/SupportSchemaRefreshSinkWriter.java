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
import org.apache.seatunnel.api.table.catalog.CatalogTable;

import java.io.IOException;

/** Writer capability for reconciling local state with an authoritative evolved sink schema. */
@Experimental
public interface SupportSchemaRefreshSinkWriter {

    /**
     * Rebuilds all schema-dependent writer-local state from the complete evolved schema.
     *
     * <p>The engine drains old-schema records and applies the external schema change before this
     * method is called. Implementations must not execute external DDL from this method. Repeating a
     * refresh with the same schema must be safe.
     */
    void refreshSchema(CatalogTable evolvedSchema) throws IOException;
}
