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

/**
 * Constants used for Hazelcast's {@link com.hazelcast.nio.serialization.IdentifiedDataSerializable}
 * mechanism.
 */
public final class SeaTunnelFactoryIdConstant {
    /**
     * Name of the system property that specifies SeaTunnelEngine's data serialization factory ID.
     */
    public static final String SEATUNNEL_OPERATION_DATA_SERIALIZER_FACTORY =
        "hazelcast.serialization.ds.seatunnel.engine.operation";
    /**
     * Default ID of SeaTunnelEngine's data serialization factory.
     */
    public static final int SEATUNNEL_OPERATION_DATA_SERIALIZER_FACTORY_ID = -30001;

    public static final String SEATUNNEL_JOB_DATA_SERIALIZER_FACTORY =
        "hazelcast.serialization.ds.seatunnel.engine.job";
    public static final int SEATUNNEL_JOB_DATA_SERIALIZER_FACTORY_ID = -30002;

    public static final String SEATUNNEL_CONFIG_DATA_SERIALIZER_FACTORY =
        "hazelcast.serialization.ds.seatunnel.engine.config";
    public static final int SEATUNNEL_CONFIG_DATA_SERIALIZER_FACTORY_ID = -30003;

    public static final String SEATUNNEL_TASK_DATA_SERIALIZER_FACTORY =
            "hazelcast.serialization.ds.seatunnel.engine.task";
    public static final int SEATUNNEL_TASK_DATA_SERIALIZER_FACTORY_ID = -30004;

    public static final String SEATUNNEL_RESOURCE_DATA_SERIALIZER_FACTORY =
            "hazelcast.serialization.ds.seatunnel.engine.resource";
    public static final int SEATUNNEL_RESOURCE_DATA_SERIALIZER_FACTORY_ID = -30005;

    public static final String SEATUNNEL_CHECKPOINT_DATA_SERIALIZER_FACTORY =
        "hazelcast.serialization.ds.seatunnel.engine.checkpoint";
    public static final int SEATUNNEL_CHECKPOINT_DATA_SERIALIZER_FACTORY_ID = -30006;
}
