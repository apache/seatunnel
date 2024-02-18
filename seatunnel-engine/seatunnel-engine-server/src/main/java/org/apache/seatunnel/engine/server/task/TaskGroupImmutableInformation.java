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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.net.URL;
import java.util.Set;

@lombok.Data
@AllArgsConstructor
public class TaskGroupImmutableInformation implements IdentifiedDataSerializable {
    // Each deployment generates a new executionId
    private long executionId;

    private Data group;

    private Set<URL> jars;

    // Set<URL> pluginJarsUrls is a collection of paths stored on the engine for all connector Jar
    // packages and third-party Jar packages that the connector relies on.
    // All storage paths come from the unique identifier obtained after uploading the Jar package
    // through the client.
    // Set<ConnectorJarIdentifier> represents the set of the unique identifier of a Jar package
    // file,
    // which contains more information about the Jar package file, including the name of the
    // connector plugin using the current Jar, the type of the current Jar package, and so on.
    // TODO: Only use Set<ConnectorJarIdentifier>to save more information about the Jar package,
    // including the storage path of the Jar package on the server.
    private Set<ConnectorJarIdentifier> connectorJarIdentifiers;

    public TaskGroupImmutableInformation() {}

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.TASK_GROUP_INFO_TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(executionId);
        out.writeObject(jars);
        out.writeObject(connectorJarIdentifiers);
        IOUtil.writeData(out, group);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        executionId = in.readLong();
        jars = in.readObject();
        connectorJarIdentifiers = in.readObject();
        group = IOUtil.readData(in);
    }
}
