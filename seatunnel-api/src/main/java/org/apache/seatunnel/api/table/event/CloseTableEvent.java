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

package org.apache.seatunnel.api.table.event;

import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Signals that a bounded source has finished producing records for a table, so downstream sink
 * writers can release table-scoped resources early.
 */
@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class CloseTableEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The downstream table whose resources can be reclaimed once all upstream signals arrive. */
    private final TablePath tablePath;

    /** The upstream subtask that emitted this event on its own output channel. */
    private final Integer sourceSubtaskId;

    /** Number of distinct upstream subtasks that must emit this event before closing the table. */
    private final Integer expectedSourceEventCount;

    public CloseTableEvent(TablePath tablePath) {
        this(tablePath, null, null);
    }

    public String tableId() {
        return tablePath == null ? null : tablePath.getFullName();
    }
}
