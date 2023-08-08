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
public class AlterTableChangeColumnEvent extends AlterTableAddColumnEvent {
    private final String oldColumn;

    public AlterTableChangeColumnEvent(
            TablePath tablePath,
            String oldColumn,
            Column column,
            boolean first,
            String afterColumn) {
        super(tablePath, column, first, afterColumn);
        this.oldColumn = oldColumn;
    }

    public static AlterTableChangeColumnEvent changeFirst(
            TablePath tablePath, String oldColumn, Column column) {
        return new AlterTableChangeColumnEvent(tablePath, oldColumn, column, true, null);
    }

    public static AlterTableChangeColumnEvent change(
            TablePath tablePath, String oldColumn, Column column) {
        return new AlterTableChangeColumnEvent(tablePath, oldColumn, column, false, null);
    }

    public static AlterTableChangeColumnEvent changeAfter(
            TablePath tablePath, String oldColumn, Column column, String afterColumn) {
        return new AlterTableChangeColumnEvent(tablePath, oldColumn, column, false, afterColumn);
    }
}
