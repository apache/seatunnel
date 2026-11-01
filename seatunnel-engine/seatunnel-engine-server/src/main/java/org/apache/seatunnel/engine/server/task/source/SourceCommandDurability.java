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

/** Ownership and failure policy attached to a managed Source command. */
public enum SourceCommandDurability {
    EPHEMERAL(1),
    RECONSTRUCTABLE(2),
    CHECKPOINT_COUPLED(3),
    TERMINAL(4);

    private final int code;

    SourceCommandDurability(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static SourceCommandDurability fromCode(int code) {
        for (SourceCommandDurability durability : values()) {
            if (durability.code == code) {
                return durability;
            }
        }
        throw new IllegalArgumentException("Unknown Source command durability code " + code);
    }
}
