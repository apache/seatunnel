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

package org.apache.seatunnel.engine.common.serializeable;

import org.apache.seatunnel.engine.common.config.JobConfig;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class ConfigDataSerializerHook implements DataSerializerHook {
    /**
     * Serialization ID of the {@link org.apache.seatunnel.engine.common.config.JobConfig} class.
     */
    public static final int JOB_CONFIG = 0;

    public static final int FACTORY_ID =
            FactoryIdHelper.getFactoryId(
                    SeaTunnelFactoryIdConstant.SEATUNNEL_CONFIG_DATA_SERIALIZER_FACTORY,
                    SeaTunnelFactoryIdConstant.SEATUNNEL_CONFIG_DATA_SERIALIZER_FACTORY_ID);

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new Factory();
    }

    private static class Factory implements DataSerializableFactory {

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case JOB_CONFIG:
                    return new JobConfig();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
