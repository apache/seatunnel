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

package org.apache.seatunnel.edge.agent.starter.config;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class AgentRuntimeConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final AgentSectionConfig agent;
    private final QueueConfig queue;
    private final AgentSchedulerConfig scheduler;
    private final RetryConfig retry;

    private AgentRuntimeConfig(
            AgentSectionConfig agent,
            QueueConfig queue,
            AgentSchedulerConfig scheduler,
            RetryConfig retry) {
        this.agent = agent;
        this.queue = queue;
        this.scheduler = scheduler;
        this.retry = retry;
    }

    public static AgentRuntimeConfig compose(
            AgentSectionConfig agent,
            QueueConfig queue,
            AgentSchedulerConfig scheduler,
            RetryConfig retry) {
        return new AgentRuntimeConfig(agent, queue, scheduler, retry);
    }

    public String getAgentId() {
        return agent.getAgentId();
    }

    public EdgeDeliveryGuarantee getDeliveryGuarantee() {
        return agent.getDeliveryGuarantee();
    }

    public String getSqlitePath() {
        return queue.getSqlitePath();
    }

    public int getMaxPollRecords() {
        return queue.getMaxPollRecords();
    }

    public int getResurrectBatchSize() {
        return queue.getResurrectBatchSize();
    }

    public long getResurrectIntervalMs() {
        return queue.getResurrectIntervalMs();
    }

    public int getCleanupBatchSize() {
        return queue.getCleanupBatchSize();
    }

    public long getAckedRetentionMs() {
        return queue.getAckedRetentionMs();
    }

    public long getIdleSleepMs() {
        return scheduler.getIdleSleepMs();
    }

    public int getBatchBulkMaxSize() {
        return scheduler.getBulkMaxSize();
    }

    public long getBatchFlushIntervalMs() {
        return scheduler.getFlushIntervalMs();
    }

    public int getRetryMaxAttempts() {
        return retry.getMaxAttempts();
    }

    public long getRetryBackoffMs() {
        return retry.getBackoffMs();
    }

    public long getRetryBackoffMaxMs() {
        return retry.getBackoffMaxMs();
    }
}
