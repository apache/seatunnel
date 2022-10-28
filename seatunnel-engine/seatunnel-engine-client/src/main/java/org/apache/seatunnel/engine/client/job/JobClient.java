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

package org.apache.seatunnel.engine.client.job;

import org.apache.seatunnel.engine.client.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;

import lombok.NonNull;

public class JobClient {
    private final SeaTunnelHazelcastClient hazelcastClient;

    public JobClient(@NonNull SeaTunnelHazelcastClient hazelcastClient) {
        this.hazelcastClient = hazelcastClient;
    }

    public long getNewJobId() {
        return hazelcastClient.getHazelcastInstance().getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
    }

    public ClientJobProxy createJobProxy(@NonNull JobImmutableInformation jobImmutableInformation) {
        return new ClientJobProxy(hazelcastClient, jobImmutableInformation);
    }
}
