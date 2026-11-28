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
package org.apache.seatunnel.connectors.seatunnel.adobecommerce;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;

/** Durable scan position and one-way digests without provider payloads or authentication. */
final class AdobeCommerceSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    static final int HISTORY_LIMIT = 128;
    private static final byte[] EMPTY = new byte[0];

    final int version = 1;
    final byte[] identity;
    final Instant end;
    final Deque<String> history = new ArrayDeque<>();
    int resource;
    int page = 1;
    int emitted;
    byte[] digest = EMPTY;

    AdobeCommerceSplit(byte[] identity, Instant end) {
        this.identity = identity.clone();
        this.end = end;
    }

    AdobeCommerceSplit copy() {
        AdobeCommerceSplit state = new AdobeCommerceSplit(identity, end);
        state.resource = resource;
        state.page = page;
        state.emitted = emitted;
        state.digest = digest.clone();
        state.history.addAll(history);
        return state;
    }

    /** Advance the durable page only after its last row has passed collection. */
    void finish(boolean more, String fingerprint) {
        emitted = 0;
        digest = EMPTY;
        if (more) {
            page++;
            if (history.size() == HISTORY_LIMIT) {
                history.removeFirst();
            }
            history.addLast(fingerprint);
        } else {
            resource++;
            page = 1;
            history.clear();
        }
    }

    @Override
    public String splitId() {
        return "adobe-commerce-scan";
    }
}
