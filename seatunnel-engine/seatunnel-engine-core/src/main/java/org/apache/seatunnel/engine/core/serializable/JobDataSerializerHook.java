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

package org.apache.seatunnel.engine.core.serializable;

import org.apache.seatunnel.engine.common.serializeable.SeaTunnelFactoryIdConstant;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.CommonPluginJar;
import org.apache.seatunnel.engine.core.job.ConnectorPluginJar;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.RefCount;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

/**
 * A Java Service Provider hook for Hazelcast's Identified Data Serializable mechanism. This is
 * private API. All about the Job's data serializable define in this class.
 */
@PrivateApi
public final class JobDataSerializerHook implements DataSerializerHook {

    /** Serialization ID of the {@link LogicalDag} class. */
    public static final int LOGICAL_DAG = 0;

    /** Serialization ID of the {@link LogicalVertex} class. */
    public static final int LOGICAL_VERTEX = 1;

    /** Serialization ID of the {@link LogicalEdge} class. */
    public static final int LOGICAL_EDGE = 2;

    /**
     * Serialization ID of the {@link org.apache.seatunnel.engine.core.job.JobImmutableInformation}
     * class.
     */
    public static final int JOB_IMMUTABLE_INFORMATION = 3;

    public static final int JOB_INFO = 4;

    public static final int COMMON_PLUGIN_JAR = 5;

    public static final int CONNECTOR_PLUGIN_JAR = 6;

    public static final int CONNECTOR_JAR_REF_COUNT = 7;

    public static final int FACTORY_ID =
            FactoryIdHelper.getFactoryId(
                    SeaTunnelFactoryIdConstant.SEATUNNEL_JOB_DATA_SERIALIZER_FACTORY,
                    SeaTunnelFactoryIdConstant.SEATUNNEL_JOB_DATA_SERIALIZER_FACTORY_ID);

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
                case LOGICAL_DAG:
                    return new LogicalDag();
                case LOGICAL_VERTEX:
                    return new LogicalVertex();
                case LOGICAL_EDGE:
                    return new LogicalEdge();
                case JOB_IMMUTABLE_INFORMATION:
                    return new JobImmutableInformation();
                case JOB_INFO:
                    return new JobInfo();
                case COMMON_PLUGIN_JAR:
                    return new CommonPluginJar();
                case CONNECTOR_PLUGIN_JAR:
                    return new ConnectorPluginJar();
                case CONNECTOR_JAR_REF_COUNT:
                    return new RefCount();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
