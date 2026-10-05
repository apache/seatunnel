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

package org.apache.seatunnel.engine.core.protocol.codec;

import org.apache.seatunnel.engine.core.job.RestoreMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.impl.protocol.ClientMessage;

class SeaTunnelGetJobCheckpointCodecTest {

    @Test
    void encodeDecodeRequest_shouldKeepLegacyJobIdApi() {
        long jobId = 123456789L;

        ClientMessage message = SeaTunnelGetJobCheckpointCodec.encodeRequest(jobId);

        Assertions.assertEquals(jobId, SeaTunnelGetJobCheckpointCodec.decodeRequest(message));
    }

    @Test
    void decodeRequestParameters_shouldDefaultLegacyRequestToSavepoint() {
        long jobId = 123456789L;

        ClientMessage message = SeaTunnelGetJobCheckpointCodec.encodeRequest(jobId);
        SeaTunnelGetJobCheckpointCodec.RequestParameters parameters =
                SeaTunnelGetJobCheckpointCodec.decodeRequestParameters(message);

        Assertions.assertEquals(jobId, parameters.jobId);
        Assertions.assertEquals(RestoreMode.SAVEPOINT.getCode(), parameters.restoreModeCode);
    }

    @Test
    void encodeDecodeRequestParameters_shouldUseStableRestoreModeCode() {
        long jobId = 123456789L;
        ClientMessage message =
                SeaTunnelGetJobCheckpointCodec.encodeRequest(
                        jobId, RestoreMode.CHECKPOINT.getCode());

        SeaTunnelGetJobCheckpointCodec.RequestParameters parameters =
                SeaTunnelGetJobCheckpointCodec.decodeRequestParameters(message);

        Assertions.assertEquals(jobId, parameters.jobId);
        Assertions.assertEquals(RestoreMode.CHECKPOINT.getCode(), parameters.restoreModeCode);
        Assertions.assertEquals(
                RestoreMode.CHECKPOINT, RestoreMode.fromCode(parameters.restoreModeCode));
    }
}
