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

package org.apache.seatunnel.connectors.seatunnel.datadog;

import org.apache.seatunnel.api.source.SourceSplit;

import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/** Versioned logical progress. A response, running request and credential are never serialized. */
final class DatadogSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private static final int VERSION = 1;
    private final int version;
    private final byte[] identity;
    final Instant end;
    Instant from;
    long position;
    String cursor;
    int page;
    int prefix;
    byte[] prefixDigest = new byte[0];
    final Set<String> seen = new HashSet<>();
    boolean complete;

    DatadogSplit(byte[] identity, Instant from, Instant end) {
        this.version = VERSION;
        this.identity = identity;
        this.from = from;
        this.end = end;
        this.complete = end != null && from.equals(end);
    }

    private DatadogSplit(DatadogSplit other, boolean includeSeen) {
        version = other.version;
        identity = other.identity.clone();
        end = other.end;
        from = other.from;
        position = other.position;
        cursor = other.cursor;
        page = other.page;
        prefix = other.prefix;
        prefixDigest = other.prefixDigest.clone();
        if (includeSeen) {
            seen.addAll(other.seen);
        }
        complete = other.complete;
    }

    boolean matches(byte[] scope) {
        return version == VERSION && MessageDigest.isEqual(identity, scope);
    }

    DatadogSplit copy() {
        return new DatadogSplit(this, true);
    }

    DatadogSplit requestView() {
        return new DatadogSplit(this, false);
    }

    Instant until(int days) {
        if (from == null) {
            return null;
        }
        Instant candidate = from.plus(Duration.ofDays(days));
        return candidate.isBefore(end) ? candidate : end;
    }

    @Override
    public String splitId() {
        return "datadog-scan";
    }
}
