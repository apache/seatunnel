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
package org.apache.seatunnel.connectors.seatunnel.buzzsprout;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;

/**
 * Frozen publication window and immutable output progress between Buzzsprout lists.
 *
 * <p>The checkpoint retains only read scope, the selected resource position, the raw-array
 * fingerprint and the number of emitted rows.
 */
final class BuzzsproutSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final Instant end;
    final int resourceIndex;
    final String fingerprint;
    final int published;

    BuzzsproutSplit(BuzzsproutConfig config) {
        this(config.scope, config.scanEnd(), 0, null, 0);
    }

    private BuzzsproutSplit(
            String scope, Instant end, int resourceIndex, String fingerprint, int published) {
        this.scope = scope;
        this.end = end;
        this.resourceIndex = resourceIndex;
        this.fingerprint = fingerprint;
        this.published = published;
    }

    BuzzsproutSplit partial(String fingerprint, int count) {
        return new BuzzsproutSplit(scope, end, resourceIndex, fingerprint, count);
    }

    BuzzsproutSplit next() {
        return new BuzzsproutSplit(scope, end, resourceIndex + 1, null, 0);
    }

    @Override
    public String splitId() {
        return "buzzsprout-metadata";
    }
}
