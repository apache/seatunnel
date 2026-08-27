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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.schema;

import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.iceberg.types.Type;

import java.util.Collection;
import java.util.Map;

public class SchemaChangeWrapper {
    private final Map<String, SchemaAddColumn> addColumns = Maps.newHashMap();
    private final Map<String, SchemaDeleteColumn> deleteColumns = Maps.newHashMap();
    private final Map<String, SchemaModifyColumn> modifyColumns = Maps.newHashMap();
    private final Map<String, SchemaChangeColumn> changeColumns = Maps.newHashMap();

    public Collection<SchemaAddColumn> addColumns() {
        return addColumns.values();
    }

    public Collection<SchemaModifyColumn> modifyColumns() {
        return modifyColumns.values();
    }

    public Collection<SchemaDeleteColumn> deleteColumns() {
        return deleteColumns.values();
    }

    public Collection<SchemaChangeColumn> changeColumns() {
        return changeColumns.values();
    }

    public boolean empty() {
        return addColumns.isEmpty()
                && modifyColumns.isEmpty()
                && deleteColumns.isEmpty()
                && changeColumns.isEmpty();
    }

    public void addColumn(String parentName, String name, Type type) {
        SchemaAddColumn addCol = new SchemaAddColumn(parentName, name, type);
        addColumns.put(addCol.key(), addCol);
    }

    public void modifyColumn(String name, Type.PrimitiveType type) {
        modifyColumns.put(name, new SchemaModifyColumn(name, type));
    }

    public void deleteColumn(String name) {
        deleteColumns.put(name, new SchemaDeleteColumn(name));
    }

    public void changeColumn(String oldName, String newName) {
        changeColumns.put(newName, new SchemaChangeColumn(oldName, newName));
    }
}
