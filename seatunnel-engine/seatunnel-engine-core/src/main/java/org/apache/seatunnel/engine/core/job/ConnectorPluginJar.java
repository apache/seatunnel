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

package org.apache.seatunnel.engine.core.job;

import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.io.InvalidObjectException;

public class ConnectorPluginJar extends ConnectorJar {

    public ConnectorPluginJar() {
        super();
    }

    protected ConnectorPluginJar(byte[] data, String fileName) {
        super(ConnectorJarType.CONNECTOR_PLUGIN_JAR, data, fileName);
    }

    protected ConnectorPluginJar(byte[] connectorJarID, byte[] data, String fileName) {
        super(connectorJarID, ConnectorJarType.CONNECTOR_PLUGIN_JAR, data, fileName);
    }

    @Override
    public int getFactoryId() {
        return JobDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JobDataSerializerHook.CONNECTOR_PLUGIN_JAR;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByteArray(connectorJarID);
        out.writeInt(ConnectorJarType.CONNECTOR_PLUGIN_JAR.ordinal());
        out.writeByteArray(data);
        out.writeString(fileName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.connectorJarID = in.readByteArray();
        int ordinal = in.readInt();
        ConnectorJarType[] values = ConnectorJarType.values();
        if (ordinal >= 0 && ordinal < values.length) {
            // Obtain the corresponding enumeration constant based on the ordinal
            this.type = values[ordinal];
        } else {
            throw new InvalidObjectException("Invalid ordinal for ConnectorJarType");
        }
        this.data = in.readByteArray();
        this.fileName = in.readString();
    }
}
