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
import org.apache.seatunnel.engine.server.operation.CancelJobOperation;
import org.apache.seatunnel.engine.server.operation.GetClusterHealthMetricsOperation;
import org.apache.seatunnel.engine.server.operation.GetJobDetailStatusOperation;
import org.apache.seatunnel.engine.server.operation.GetJobInfoOperation;
import org.apache.seatunnel.engine.server.operation.GetJobMetricsOperation;
import org.apache.seatunnel.engine.server.operation.GetJobStatusOperation;
import org.apache.seatunnel.engine.server.operation.GetRunningJobMetricsOperation;
import org.apache.seatunnel.engine.server.operation.PrintMessageOperation;
import org.apache.seatunnel.engine.server.operation.SavePointJobOperation;
import org.apache.seatunnel.engine.server.operation.SubmitJobOperation;
import org.apache.seatunnel.engine.server.operation.UploadConnectorJarOperation;
import org.apache.seatunnel.engine.server.operation.WaitForJobCompleteOperation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

/**
 * A Java Service Provider hook for Hazelcast's Identified Data Serializable mechanism. This is
 * private API. All about the Operation's data serializable define in this class.
 */
@PrivateApi
public final class ClientToServerOperationDataSerializerHook implements DataSerializerHook {
    public static final int PRINT_MESSAGE_OPERATOR = 0;
    public static final int SUBMIT_OPERATOR = 1;

    public static final int WAIT_FORM_JOB_COMPLETE_OPERATOR = 2;

    public static final int CANCEL_JOB_OPERATOR = 3;

    public static final int GET_JOB_STATUS_OPERATOR = 4;

    public static final int GET_JOB_METRICS_OPERATOR = 5;

    public static final int GET_JOB_STATE_OPERATION = 6;

    public static final int GET_JOB_INFO_OPERATION = 7;

    public static final int SAVEPOINT_JOB_OPERATOR = 8;

    public static final int GET_CLUSTER_HEALTH_METRICS = 9;

    public static final int GET_RUNNING_JOB_METRICS_OPERATOR = 10;

    public static final int UPLOAD_CONNECTOR_JAR_OPERATION = 11;

    public static final int FACTORY_ID =
            FactoryIdHelper.getFactoryId(
                    SeaTunnelFactoryIdConstant.SEATUNNEL_OPERATION_DATA_SERIALIZER_FACTORY,
                    SeaTunnelFactoryIdConstant.SEATUNNEL_OPERATION_DATA_SERIALIZER_FACTORY_ID);

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
                case PRINT_MESSAGE_OPERATOR:
                    return new PrintMessageOperation();
                case SUBMIT_OPERATOR:
                    return new SubmitJobOperation();
                case WAIT_FORM_JOB_COMPLETE_OPERATOR:
                    return new WaitForJobCompleteOperation();
                case CANCEL_JOB_OPERATOR:
                    return new CancelJobOperation();
                case GET_JOB_STATUS_OPERATOR:
                    return new GetJobStatusOperation();
                case GET_JOB_METRICS_OPERATOR:
                    return new GetJobMetricsOperation();
                case GET_JOB_STATE_OPERATION:
                    return new GetJobDetailStatusOperation();
                case GET_JOB_INFO_OPERATION:
                    return new GetJobInfoOperation();
                case SAVEPOINT_JOB_OPERATOR:
                    return new SavePointJobOperation();
                case GET_CLUSTER_HEALTH_METRICS:
                    return new GetClusterHealthMetricsOperation();
                case GET_RUNNING_JOB_METRICS_OPERATOR:
                    return new GetRunningJobMetricsOperation();
                case UPLOAD_CONNECTOR_JAR_OPERATION:
                    return new UploadConnectorJarOperation();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
