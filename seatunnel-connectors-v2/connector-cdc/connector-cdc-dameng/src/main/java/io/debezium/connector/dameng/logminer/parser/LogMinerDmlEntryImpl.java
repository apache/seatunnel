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

package io.debezium.connector.dameng.logminer.parser;

import io.debezium.connector.dameng.logminer.Operation;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@ToString
@Accessors(chain = true)
@EqualsAndHashCode(of = {"operation", "newValues", "oldValues"})
@RequiredArgsConstructor
public class LogMinerDmlEntryImpl implements LogMinerDmlEntry {
    @NonNull private final Operation operation;
    private final Object[] newValues;
    private final Object[] oldValues;
    @Setter private String objectOwner;
    @Setter private String objectName;

    public static LogMinerDmlEntry forInsert(Object[] newColumnValues) {
        return new LogMinerDmlEntryImpl(Operation.INSERT, newColumnValues, null);
    }

    public static LogMinerDmlEntry forUpdate(Object[] newColumnValues, Object[] oldColumnValues) {
        return new LogMinerDmlEntryImpl(Operation.UPDATE, newColumnValues, oldColumnValues);
    }

    public static LogMinerDmlEntry forDelete(Object[] oldColumnValues) {
        return new LogMinerDmlEntryImpl(Operation.DELETE, null, oldColumnValues);
    }
}
