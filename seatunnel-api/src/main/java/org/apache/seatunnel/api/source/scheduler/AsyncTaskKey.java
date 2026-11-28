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

package org.apache.seatunnel.api.source.scheduler;

import org.apache.seatunnel.api.annotation.Experimental;

import java.io.Serializable;
import java.util.Objects;

/**
 * Stable key used for overlap control and lifecycle cancellation of coordinator async work.
 *
 * <p>Scheduling a timer with an existing key replaces the previous timer.
 */
@Experimental
public final class AsyncTaskKey implements Serializable {
    private static final int MAX_LENGTH = 256;

    private final String value;

    private AsyncTaskKey(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Async task key must not be blank");
        }
        if (value.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("Async task key exceeds 256 characters");
        }
        this.value = value;
    }

    public static AsyncTaskKey of(String value) {
        return new AsyncTaskKey(value);
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof AsyncTaskKey && value.equals(((AsyncTaskKey) other).getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
