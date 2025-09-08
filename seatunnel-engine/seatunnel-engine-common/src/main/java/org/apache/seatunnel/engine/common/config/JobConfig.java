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

package org.apache.seatunnel.engine.common.config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.engine.common.serializeable.ConfigDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.Data;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Data
public class JobConfig implements IdentifiedDataSerializable {
    private String name = EnvCommonOptions.JOB_NAME.defaultValue();
    private JobContext jobContext;

    private Map<String, Object> envOptions = new HashMap<>();

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.JOB_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeObject(jobContext);
        out.writeObject(envOptions);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.name = in.readString();
        this.jobContext = in.readObject();
        this.envOptions = in.readObject();
    }
}
