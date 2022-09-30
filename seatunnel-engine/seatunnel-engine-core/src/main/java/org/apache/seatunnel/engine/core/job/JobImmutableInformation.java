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

    private long createTime;

    private Data logicalDag;

    private JobConfig jobConfig;

    private List<URL> pluginJarsUrls;

    public JobImmutableInformation() {
    }

    public JobImmutableInformation(long jobId, @NonNull Data logicalDag, @NonNull JobConfig jobConfig, @NonNull List<URL> pluginJarsUrls) {
        this.createTime = System.currentTimeMillis();
        this.jobId = jobId;
        this.logicalDag = logicalDag;
        this.jobConfig = jobConfig;
        this.pluginJarsUrls = pluginJarsUrls;
    }

    public long getJobId() {
        return jobId;
    }

    public long getCreateTime() {
        return createTime;
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
        out.writeLong(createTime);
        IOUtil.writeData(out, logicalDag);
        out.writeObject(jobConfig);
        out.writeObject(pluginJarsUrls);

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        createTime = in.readLong();
        logicalDag = IOUtil.readData(in);
        jobConfig = in.readObject();
        pluginJarsUrls = in.readObject();
    }
}
