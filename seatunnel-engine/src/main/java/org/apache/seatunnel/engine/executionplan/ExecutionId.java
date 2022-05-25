/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.executionplan;

import org.apache.seatunnel.engine.utils.AbstractID;

/**
 * Unique identifier for the attempt to execute a tasks. Multiple attempts happen in cases of
 * failures and recovery.
 */
public class ExecutionId implements java.io.Serializable {

    private static final long serialVersionUID = -1169683445778281344L;

    private final AbstractID executionAttemptId;

    public ExecutionId() {
        this(new AbstractID());
    }

    private ExecutionId(AbstractID id) {
        this.executionAttemptId = id;
    }

    public ExecutionId(ExecutionId toCopy) {
        this.executionAttemptId = new AbstractID(toCopy.executionAttemptId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == getClass()) {
            ExecutionId that = (ExecutionId) obj;
            return that.executionAttemptId.equals(this.executionAttemptId);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return executionAttemptId.hashCode();
    }

    @Override
    public String toString() {
        return executionAttemptId.toString();
    }
}
