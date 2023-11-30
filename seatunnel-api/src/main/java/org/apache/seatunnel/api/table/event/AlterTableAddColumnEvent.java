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

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class AlterTableAddColumnEvent extends AlterTableColumnEvent {
    private final Column column;
    private final boolean first;
    private final String afterColumn;

    public AlterTableAddColumnEvent(
            TablePath tablePath, Column column, boolean first, String afterColumn) {
        super(tablePath);
        this.column = column;
        this.first = first;
        this.afterColumn = afterColumn;
    }

    public static AlterTableAddColumnEvent addFirst(TablePath tablePath, Column column) {
        return new AlterTableAddColumnEvent(tablePath, column, true, null);
    }

    public static AlterTableAddColumnEvent add(TablePath tablePath, Column column) {
        return new AlterTableAddColumnEvent(tablePath, column, false, null);
    }

    public static AlterTableAddColumnEvent addAfter(
            TablePath tablePath, Column column, String afterColumn) {
        return new AlterTableAddColumnEvent(tablePath, column, false, afterColumn);
    }
}
