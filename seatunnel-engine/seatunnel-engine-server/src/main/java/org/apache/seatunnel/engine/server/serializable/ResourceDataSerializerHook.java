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
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.GetOverviewOperation;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.ReleaseSlotOperation;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.RequestSlotOperation;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.ResetResourceOperation;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.SyncWorkerProfileOperation;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.WorkerHeartbeatOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.service.slot.SlotAndWorkerProfile;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class ResourceDataSerializerHook implements DataSerializerHook {

    public static final int WORKER_HEARTBEAT_TYPE = 1;

    public static final int REQUEST_SLOT_TYPE = 2;

    public static final int RELEASE_SLOT_TYPE = 3;

    public static final int RESET_RESOURCE_TYPE = 4;

    public static final int WORKER_PROFILE_TYPE = 5;

    public static final int SLOT_PROFILE_TYPE = 6;

    public static final int SLOT_AND_WORKER_PROFILE = 7;

    public static final int SYNC_SLOT_SERVICE_STATUS_TYPE = 8;

    public static final int REQUEST_SLOT_INFO_TYPE = 9;

    public static final int FACTORY_ID =
            FactoryIdHelper.getFactoryId(
                    SeaTunnelFactoryIdConstant.SEATUNNEL_RESOURCE_DATA_SERIALIZER_FACTORY,
                    SeaTunnelFactoryIdConstant.SEATUNNEL_RESOURCE_DATA_SERIALIZER_FACTORY_ID);

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
                case WORKER_HEARTBEAT_TYPE:
                    return new WorkerHeartbeatOperation();
                case REQUEST_SLOT_TYPE:
                    return new RequestSlotOperation();
                case RELEASE_SLOT_TYPE:
                    return new ReleaseSlotOperation();
                case RESET_RESOURCE_TYPE:
                    return new ResetResourceOperation();
                case WORKER_PROFILE_TYPE:
                    return new WorkerProfile();
                case SLOT_PROFILE_TYPE:
                    return new SlotProfile();
                case SLOT_AND_WORKER_PROFILE:
                    return new SlotAndWorkerProfile();
                case SYNC_SLOT_SERVICE_STATUS_TYPE:
                    return new SyncWorkerProfileOperation();
                case REQUEST_SLOT_INFO_TYPE:
                    return new GetOverviewOperation();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
