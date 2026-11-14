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

/** Immediate result returned by an operation after bounded mailbox admission. */
public enum SourceCommandAdmissionStatus {
    ACCEPTED(1),
    DUPLICATE(2),
    RETRY_LATER(3),
    STALE_TARGET(4),
    TERMINAL_REJECTED(5),
    UNSUPPORTED_PROTOCOL(6),
    INVALID_PAYLOAD(7);

    private final int code;

    SourceCommandAdmissionStatus(int code) {
        this.code = code;
    }

    /** Returns the stable wire code for this admission result. */
    public int getCode() {
        return code;
    }

    /**
     * Resolves a stable wire code without relying on enum ordinal ordering.
     *
     * @param code encoded admission status
     * @return matching status
     */
    public static SourceCommandAdmissionStatus fromCode(int code) {
        for (SourceCommandAdmissionStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown Source command admission status code " + code);
    }
}
