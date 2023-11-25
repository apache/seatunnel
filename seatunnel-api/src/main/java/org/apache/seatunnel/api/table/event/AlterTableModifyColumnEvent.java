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
public class AlterTableModifyColumnEvent extends AlterTableAddColumnEvent {
    public AlterTableModifyColumnEvent(
            TablePath tablePath, Column column, boolean first, String afterColumn) {
        super(tablePath, column, first, afterColumn);
    }

    public static AlterTableModifyColumnEvent modifyFirst(TablePath tablePath, Column column) {
        return new AlterTableModifyColumnEvent(tablePath, column, true, null);
    }

    public static AlterTableModifyColumnEvent modify(TablePath tablePath, Column column) {
        return new AlterTableModifyColumnEvent(tablePath, column, false, null);
    }

    public static AlterTableModifyColumnEvent modifyAfter(
            TablePath tablePath, Column column, String afterColumn) {
        return new AlterTableModifyColumnEvent(tablePath, column, false, afterColumn);
    }
}
