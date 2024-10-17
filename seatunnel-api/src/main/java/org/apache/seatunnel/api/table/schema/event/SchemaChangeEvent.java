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

package org.apache.seatunnel.api.table.schema.event;

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;

/** Represents a structural change to a table schema. */
public interface SchemaChangeEvent extends Event {

    /**
     * Path of the change table object
     *
     * @return
     */
    default TablePath tablePath() {
        return tableIdentifier().toTablePath();
    }

    /**
     * Path of the change table object
     *
     * @return
     */
    TableIdentifier tableIdentifier();

    /**
     * Get the table struct after the change
     *
     * @return
     */
    CatalogTable getChangeAfter();

    /**
     * Set the table struct after the change
     *
     * @param table
     */
    void setChangeAfter(CatalogTable table);
}
