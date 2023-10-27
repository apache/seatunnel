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

import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.NonNull;

import java.io.IOException;
import java.net.URL;
import java.util.List;

public class JobImmutableInformation implements IdentifiedDataSerializable {
    private long jobId;

    private String jobName;

    private boolean isStartWithSavePoint;

    private long createTime;

    private Data logicalDag;

    private JobConfig jobConfig;

    private List<URL> pluginJarsUrls;

    // List<URL> pluginJarsUrls is a collection of paths stored on the engine for all connector Jar
    // packages and third-party Jar packages that the connector relies on.
    // All storage paths come from the unique identifier obtained after uploading the Jar package
    // through the client.
    // List<ConnectorJarIdentifier> represents the set of the unique identifier of a Jar package
    // file,
    // which contains more information about the Jar package file, including the name of the
    // connector plugin using the current Jar, the type of the current Jar package, and so on.
    // TODO: Only use List<ConnectorJarIdentifier> to save more information about the Jar package,
    // including the storage path of the Jar package on the server.
    private List<ConnectorJarIdentifier> connectorJarIdentifiers;

    public JobImmutableInformation() {}

    public JobImmutableInformation(
            long jobId,
            String jobName,
            boolean isStartWithSavePoint,
            @NonNull Data logicalDag,
            @NonNull JobConfig jobConfig,
            @NonNull List<URL> pluginJarsUrls,
            @NonNull List<ConnectorJarIdentifier> connectorJarIdentifiers) {
        this.createTime = System.currentTimeMillis();
        this.jobId = jobId;
        this.jobName = jobName;
        this.isStartWithSavePoint = isStartWithSavePoint;
        this.logicalDag = logicalDag;
        this.jobConfig = jobConfig;
        this.pluginJarsUrls = pluginJarsUrls;
        this.connectorJarIdentifiers = connectorJarIdentifiers;
    }

    public JobImmutableInformation(
            long jobId,
            String jobName,
            @NonNull Data logicalDag,
            @NonNull JobConfig jobConfig,
            @NonNull List<URL> pluginJarsUrls,
            @NonNull List<ConnectorJarIdentifier> connectorJarIdentifiers) {
        this(jobId, jobName, false, logicalDag, jobConfig, pluginJarsUrls, connectorJarIdentifiers);
    }

    public long getJobId() {
        return jobId;
    }

    public boolean isStartWithSavePoint() {
        return isStartWithSavePoint;
    }

    public long getCreateTime() {
        return createTime;
    }

    public String getJobName() {
        return jobName;
    }

    public Data getLogicalDag() {
        return logicalDag;
    }

    public JobConfig getJobConfig() {
        return jobConfig;
    }

    public List<URL> getPluginJarsUrls() {
        return pluginJarsUrls;
    }

    public List<ConnectorJarIdentifier> getPluginJarIdentifiers() {
        return connectorJarIdentifiers;
    }

    @Override
    public int getFactoryId() {
        return JobDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JobDataSerializerHook.JOB_IMMUTABLE_INFORMATION;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeString(jobName);
        out.writeBoolean(isStartWithSavePoint);
        out.writeLong(createTime);
        IOUtil.writeData(out, logicalDag);
        out.writeObject(jobConfig);
        out.writeObject(pluginJarsUrls);
        out.writeObject(connectorJarIdentifiers);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        jobName = in.readString();
        isStartWithSavePoint = in.readBoolean();
        createTime = in.readLong();
        logicalDag = IOUtil.readData(in);
        jobConfig = in.readObject();
        pluginJarsUrls = in.readObject();
        connectorJarIdentifiers = in.readObject();
    }
}
