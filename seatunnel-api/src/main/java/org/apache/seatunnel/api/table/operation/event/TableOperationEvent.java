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

package org.apache.seatunnel.api.table.operation.event;

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.operation.TableOperationType;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;

/**
 * A table-level operation that is not a structural {@link SchemaChangeEvent}.
 *
 * <p>{@code TRUNCATE TABLE} belongs here: the table object and schema stay in place, only the data
 * is removed.
 */
public interface TableOperationEvent extends Event {

    TableIdentifier tableIdentifier();

    default TablePath tablePath() {
        return tableIdentifier().toTablePath();
    }

    TableOperationType operationType();

    String getStatement();

    void setStatement(String statement);
}
