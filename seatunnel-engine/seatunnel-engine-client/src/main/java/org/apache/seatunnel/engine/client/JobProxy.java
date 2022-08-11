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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.engine.common.utils.NonCompletableFuture;
import org.apache.seatunnel.engine.core.job.Job;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelSubmitJobCodec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.concurrent.ExecutionException;

public class JobProxy implements Job {
    private static final ILogger LOGGER = Logger.getLogger(JobProxy.class);
    private SeaTunnelHazelcastClient seaTunnelHazelcastClient;
    private JobImmutableInformation jobImmutableInformation;

    public JobProxy(@NonNull SeaTunnelHazelcastClient seaTunnelHazelcastClient,
                    @NonNull JobImmutableInformation jobImmutableInformation) {
        this.seaTunnelHazelcastClient = seaTunnelHazelcastClient;
        this.jobImmutableInformation = jobImmutableInformation;
    }

    @Override
    public long getJobId() {
        return jobImmutableInformation.getJobId();
    }

    @Override
    public void submitJob() throws ExecutionException, InterruptedException {
        ClientMessage request = SeaTunnelSubmitJobCodec.encodeRequest(
            seaTunnelHazelcastClient.getSerializationService().toData(jobImmutableInformation));
        NonCompletableFuture<Void> submitJobFuture =
            seaTunnelHazelcastClient.requestOnMasterAndGetCompletableFuture(request);
        submitJobFuture.get();
    }
}
