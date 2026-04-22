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

package org.apache.seatunnel.api.signal;

public final class FlushSignal implements Signal {

    private final long jobId;
    private final long taskId;
    private final long createdTime;

    public static FlushSignal of(long jobId, long taskId) {
        return new FlushSignal(jobId, taskId, System.currentTimeMillis());
    }

    public FlushSignal(long jobId, long taskId, long createdTime) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.createdTime = createdTime;
    }

    @Override
    public long getJobId() {
        return jobId;
    }

    @Override
    public long getTaskId() {
        return taskId;
    }

    @Override
    public long getCreatedTime() {
        return createdTime;
    }
}
