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

package org.apache.seatunnel.api.common.metrics;

import java.io.Serializable;
import java.util.Arrays;

public final class RawJobMetrics implements Serializable {

    private long timestamp;
    private byte[] blob;

    RawJobMetrics() {
    }

    private RawJobMetrics(long timestamp, byte[] blob) {
        this.timestamp = timestamp;
        this.blob = blob;
    }

    public static RawJobMetrics empty() {
        return of(null);
    }

    public static RawJobMetrics of(byte[] blob) {
        return new RawJobMetrics(System.currentTimeMillis(), blob);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getBlob() {
        return blob;
    }

    @Override
    public int hashCode() {
        return (int) timestamp * 31 + Arrays.hashCode(blob);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        RawJobMetrics that;
        return Arrays.equals(blob, (that = (RawJobMetrics) obj).blob)
                && this.timestamp == that.timestamp;
    }

    @Override
    public String toString() {
        return Arrays.toString(blob) + " @ " + timestamp;
    }
}
