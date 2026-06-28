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

public enum RestoreMode {
    NONE(0),
    SAVEPOINT(1),
    CHECKPOINT(2);

    private final int code;

    RestoreMode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static RestoreMode fromCode(int code) {
        for (RestoreMode restoreMode : values()) {
            if (restoreMode.code == code) {
                return restoreMode;
            }
        }
        throw new IllegalArgumentException("Unknown restore mode code: " + code);
    }

    public boolean isRestore() {
        return this != NONE;
    }
}
