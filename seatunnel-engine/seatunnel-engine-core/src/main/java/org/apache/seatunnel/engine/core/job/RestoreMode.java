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

package org.apache.seatunnel.engine.core.job;

/**
 * Restore policy for job submission.
 *
 * <p>The numeric codes are persisted and transferred across engine boundaries. They are stable
 * compatibility contracts and must not be reassigned.
 */
public enum RestoreMode {
    /** Fresh submit with no restore source. Stable code: {@code 0}. */
    NONE(0),
    /** Restore from savepoint-only checkpoint artifacts. Stable code: {@code 1}. */
    SAVEPOINT(1),
    /**
     * Restore from the latest restore-eligible checkpoint of a historical source job. Stable code:
     * {@code 2}.
     */
    CHECKPOINT(2);

    private final int code;

    RestoreMode(int code) {
        this.code = code;
    }

    /** Returns the stable persisted and wire-level code for this restore mode. */
    public int getCode() {
        return code;
    }

    /**
     * Resolves a stable persisted or wire-level code to its restore mode.
     *
     * @throws IllegalArgumentException if the code is unknown
     */
    public static RestoreMode fromCode(int code) {
        for (RestoreMode restoreMode : values()) {
            if (restoreMode.code == code) {
                return restoreMode;
            }
        }
        throw new IllegalArgumentException("Unknown restore mode code: " + code);
    }

    /** Returns whether this mode requires checkpoint or savepoint state to be restored. */
    public boolean isRestore() {
        return this != NONE;
    }
}
