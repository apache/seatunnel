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
 * Tracks whether one scaling direction has remained true for its stabilization window.
 *
 * <p>It uses caller-supplied monotonic nanoseconds so wall-clock jumps cannot affect continuity.
 */
public final class StabilizationTracker {

    private final long scaleOutWindowNanos;
    private final long scaleInWindowNanos;
    private ScalingAction activeAction;
    private long activeSinceNanos;

    public StabilizationTracker(long scaleOutWindowNanos, long scaleInWindowNanos) {
        this.scaleOutWindowNanos = scaleOutWindowNanos;
        this.scaleInWindowNanos = scaleInWindowNanos;
    }

    public synchronized boolean isStabilized(ScalingAction action, long monotonicNanos) {
        if (action == ScalingAction.NO_ACTION || action == ScalingAction.SCALE_IN_BLOCKED) {
            clear();
            return true;
        }
        if (activeAction != action) {
            activeAction = action;
            activeSinceNanos = monotonicNanos;
            return windowFor(action) <= 0L;
        }
        return monotonicNanos - activeSinceNanos >= windowFor(action);
    }

    public synchronized void reset() {
        clear();
    }

    private void clear() {
        activeAction = null;
        activeSinceNanos = 0L;
    }

    private long windowFor(ScalingAction action) {
        if (action == ScalingAction.SCALE_OUT) {
            return scaleOutWindowNanos;
        }
        if (action == ScalingAction.SCALE_IN_CANDIDATE) {
            return scaleInWindowNanos;
        }
        return 0L;
    }
}
