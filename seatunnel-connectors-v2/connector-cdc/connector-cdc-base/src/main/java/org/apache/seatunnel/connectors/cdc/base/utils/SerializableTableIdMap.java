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

package org.apache.seatunnel.connectors.cdc.base.utils;

import io.debezium.relational.TableId;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A map whose Java serialization does not depend on the runtime Debezium TableId being
 * serializable.
 *
 * @param <V> serializable value type
 */
public final class SerializableTableIdMap<V extends Serializable> extends AbstractMap<TableId, V>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient Map<TableId, V> entries = new HashMap<>();

    @Override
    public V get(Object key) {
        return entries.get(key);
    }

    @Override
    public V put(TableId tableId, V value) {
        return entries.put(tableId, value);
    }

    @Override
    public Set<Entry<TableId, V>> entrySet() {
        return entries.entrySet();
    }

    private void writeObject(ObjectOutputStream output) throws IOException {
        synchronized (this) {
            output.defaultWriteObject();
            output.writeInt(entries.size());
            for (Entry<TableId, V> entry : entries.entrySet()) {
                TableId tableId = entry.getKey();
                // A dotted identifier cannot distinguish catalog-only from schema-only names.
                output.writeObject(tableId.catalog());
                output.writeObject(tableId.schema());
                output.writeObject(tableId.table());
                // TableId equality is based on identifier(), which a connector can customize.
                output.writeObject(tableId.identifier());
                output.writeObject(entry.getValue());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        entries = new HashMap<>();
        int entryCount = input.readInt();
        for (int index = 0; index < entryCount; index++) {
            String catalog = (String) input.readObject();
            String schema = (String) input.readObject();
            String table = (String) input.readObject();
            String identifier = (String) input.readObject();
            entries.put(
                    new TableId(catalog, schema, table, ignored -> identifier),
                    (V) input.readObject());
        }
    }
}
