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

package org.apache.seatunnel.engine.imap.storage.api;

import java.util.Objects;

/** A stored value together with the time at which its entry expires. */
public final class ExpirableValue {

    /** Expiration time of an entry that never expires. */
    public static final long NO_EXPIRATION_TIME = Long.MAX_VALUE;

    private final Object value;

    private final long expirationTime;

    public ExpirableValue(Object value) {
        this(value, NO_EXPIRATION_TIME);
    }

    /**
     * @param value stored value
     * @param expirationTime epoch milliseconds at which the entry expires, or {@link
     *     #NO_EXPIRATION_TIME}
     */
    public ExpirableValue(Object value, long expirationTime) {
        this.value = Objects.requireNonNull(value, "value");
        this.expirationTime = expirationTime;
    }

    public Object getValue() {
        return value;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExpirableValue)) {
            return false;
        }
        ExpirableValue that = (ExpirableValue) o;
        return expirationTime == that.expirationTime && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, expirationTime);
    }
}
