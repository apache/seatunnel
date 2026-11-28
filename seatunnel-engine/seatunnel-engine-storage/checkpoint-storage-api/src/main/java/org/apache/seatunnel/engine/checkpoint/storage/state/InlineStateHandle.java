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

package org.apache.seatunnel.engine.checkpoint.storage.state;

import lombok.ToString;

import java.util.List;
import java.util.Objects;

/** A {@link StateHandle} whose state segments are embedded in the handle itself. */
@ToString(exclude = "states")
public final class InlineStateHandle implements StateHandle {

    private static final long serialVersionUID = 1L;

    private final List<byte[]> states;

    public InlineStateHandle(List<byte[]> states) {
        this.states = Objects.requireNonNull(states, "states");
    }

    /** Returns the embedded state segments; elements may be {@code null}. */
    public List<byte[]> getStates() {
        return states;
    }

    @Override
    public long getStateSize() {
        return sizeOf(states);
    }

    /** Sums the lengths of all non-null segments. */
    public static long sizeOf(List<byte[]> states) {
        long size = 0;
        for (byte[] segment : states) {
            if (segment != null) {
                size += segment.length;
            }
        }
        return size;
    }
}
