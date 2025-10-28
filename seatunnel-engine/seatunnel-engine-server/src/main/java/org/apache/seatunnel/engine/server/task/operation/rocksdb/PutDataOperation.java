/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.server.task.operation.rocksdb;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.operation.TracingOperation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PutDataOperation<K, V> extends TracingOperation implements IdentifiedDataSerializable {
    private String stateName;
    private Map<K, V> map;

    public PutDataOperation() {}

    public PutDataOperation(String stateName, Map<K, V> map) {
        this.stateName = stateName;
        this.map = map;
    }

    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer seaTunnelServer = getService();
        seaTunnelServer.getRocksDBService().putData(stateName, map);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(stateName);
        out.writeInt(map == null ? 0 : map.size());
        if (map != null) {
            for (Map.Entry<K, V> e : map.entrySet()) {
                out.writeObject(e.getKey());
                out.writeObject(e.getValue());
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.stateName = in.readString();
        int size = in.readInt();
        this.map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            K key = in.readObject();
            V value = in.readObject();
            this.map.put(key, value);
        }
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.PUT_DATA_OPERATION;
    }
}
