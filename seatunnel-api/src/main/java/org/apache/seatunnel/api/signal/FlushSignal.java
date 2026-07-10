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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;

@Getter
@RequiredArgsConstructor
public final class FlushSignal implements Signal {

    private static final long serialVersionUID = 1L;

    private final long jobId;
    private final long taskId;
    private final long createdTime;

    public static FlushSignal of(long jobId, long taskId) {
        return new FlushSignal(jobId, taskId, System.currentTimeMillis());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FlushSignal)) {
            return false;
        }
        FlushSignal that = (FlushSignal) o;
        return jobId == that.jobId && taskId == that.taskId && createdTime == that.createdTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, taskId, createdTime);
    }

    @Override
    public String toString() {
        return "FlushSignal{"
                + "jobId="
                + jobId
                + ", taskId="
                + taskId
                + ", createdTime="
                + createdTime
                + '}';
    }
}
