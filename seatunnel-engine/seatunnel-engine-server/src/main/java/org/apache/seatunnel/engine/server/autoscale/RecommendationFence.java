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

package org.apache.seatunnel.engine.server.autoscale;

/**
 * Fences recommendation publication by active-master epoch and generation.
 *
 * <p>Duplicate identities are idempotent; stale epochs or non-increasing generations are rejected.
 */
public final class RecommendationFence {

    private long lastMasterEpoch = Long.MIN_VALUE;
    private long lastGeneration = Long.MIN_VALUE;

    public synchronized PublicationResult tryPublish(long masterEpoch, long generation) {
        if (masterEpoch == lastMasterEpoch && generation == lastGeneration) {
            return PublicationResult.DUPLICATE;
        }
        if (masterEpoch < lastMasterEpoch) {
            return PublicationResult.REJECTED;
        }
        if (masterEpoch == lastMasterEpoch && generation <= lastGeneration) {
            return PublicationResult.REJECTED;
        }
        lastMasterEpoch = masterEpoch;
        lastGeneration = generation;
        return PublicationResult.ACCEPTED;
    }

    public synchronized void reset() {
        lastMasterEpoch = Long.MIN_VALUE;
        lastGeneration = Long.MIN_VALUE;
    }

    public enum PublicationResult {
        ACCEPTED,
        DUPLICATE,
        REJECTED
    }
}
