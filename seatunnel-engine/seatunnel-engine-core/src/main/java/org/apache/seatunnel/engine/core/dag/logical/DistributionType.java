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

package org.apache.seatunnel.engine.core.dag.logical;

/** Distribution contract carried by a port-aware logical edge. */
public enum DistributionType {
    /**
     * Direct channel declaration used by the current dynamic lookup runtime.
     *
     * <p>HASH and BROADCAST routing remain reserved until the exchange protocol owns partitioned
     * routing and restore-safe channel state.
     */
    FORWARD(0);

    /** Stable serialized value independent of enum declaration order. */
    private final int wireCode;

    DistributionType(int wireCode) {
        this.wireCode = wireCode;
    }

    public int getWireCode() {
        return wireCode;
    }

    /** Resolves a stable wire code without depending on enum declaration order. */
    public static DistributionType fromWireCode(int wireCode) {
        for (DistributionType value : values()) {
            if (value.wireCode == wireCode) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown distribution wire code: " + wireCode);
    }
}
