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

package org.apache.seatunnel.engine.common.runtime.source;

import java.io.Serializable;

/** Source runtime lane selected once during physical plan generation. */
public enum ManagedSourceRuntimeMode implements Serializable {
    LEGACY(0, false, false),
    MANAGED_READER(1, true, false),
    MANAGED_COORDINATOR(2, false, true),
    MANAGED_READER_AND_COORDINATOR(3, true, true);

    private final int code;
    private final boolean managedReader;
    private final boolean managedCoordinator;

    ManagedSourceRuntimeMode(int code, boolean managedReader, boolean managedCoordinator) {
        this.code = code;
        this.managedReader = managedReader;
        this.managedCoordinator = managedCoordinator;
    }

    public int getCode() {
        return code;
    }

    public boolean hasManagedReader() {
        return managedReader;
    }

    public boolean hasManagedCoordinator() {
        return managedCoordinator;
    }

    public static ManagedSourceRuntimeMode fromCode(int code) {
        for (ManagedSourceRuntimeMode mode : values()) {
            if (mode.code == code) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown managed Source runtime mode code " + code);
    }
}
