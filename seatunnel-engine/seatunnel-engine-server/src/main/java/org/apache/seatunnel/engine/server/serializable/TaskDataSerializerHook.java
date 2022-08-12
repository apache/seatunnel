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

package org.apache.seatunnel.engine.server.serializable;

import org.apache.seatunnel.engine.common.serializeable.SeaTunnelFactoryIdConstant;
import org.apache.seatunnel.engine.server.task.TaskGroupInfo;
import org.apache.seatunnel.engine.server.task.operation.AssignSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.RegisterOperation;
import org.apache.seatunnel.engine.server.task.operation.RequestSplitOperation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class TaskDataSerializerHook implements DataSerializerHook {

    public static final int SOURCE_REGISTER_TYPE = 1;

    public static final int REQUEST_SPLIT_TYPE = 2;

    public static final int ASSIGN_SPLIT_TYPE = 3;

    public static final int TASK_GROUP_INFO_TYPE = 4;

    public static final int SOURCE_UNREGISTER_TYPE = 5;

    public static final int SINK_UNREGISTER_TYPE = 6;

    public static final int SINK_REGISTER_TYPE = 7;

    public static final int TASK_LOCATION_TYPE = 8;

    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(
            SeaTunnelFactoryIdConstant.SEATUNNEL_TASK_DATA_SERIALIZER_FACTORY,
            SeaTunnelFactoryIdConstant.SEATUNNEL_TASK_DATA_SERIALIZER_FACTORY_ID
    );

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
                case SOURCE_REGISTER_TYPE:
                    return new SourceRegisterOperation();
                case REQUEST_SPLIT_TYPE:
                    return new RequestSplitOperation();
                case ASSIGN_SPLIT_TYPE:
                    return new AssignSplitOperation<>();
                case TASK_GROUP_INFO_TYPE:
                    return new TaskGroupImmutableInformation();
                case SOURCE_UNREGISTER_TYPE:
                    return new SourceUnregisterOperation();
                case SINK_REGISTER_TYPE:
                    return new SinkRegisterOperation();
                case SINK_UNREGISTER_TYPE:
                    return new SinkUnregisterOperation();
                case TASK_LOCATION_TYPE:
                    return new TaskLocation();
                default:
                    return null;
            }
        }
    }
}
