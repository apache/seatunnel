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

package org.apache.seatunnel.engine.server.task.source;

/** Main lifecycle states owned exclusively by a managed Source event loop. */
public enum ManagedSourceLifecycleState {
    CREATED(1),
    RESTORING(2),
    RUNNING(3),
    DRAINING(4),
    CANCELLING(5),
    FAILED(6),
    CLOSED(7);

    private final int code;

    ManagedSourceLifecycleState(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static ManagedSourceLifecycleState fromCode(int code) {
        for (ManagedSourceLifecycleState state : values()) {
            if (state.code == code) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown managed Source lifecycle state code " + code);
    }
}
