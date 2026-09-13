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
 * <p>It uses caller-supplied monotonic milliseconds so wall-clock jumps cannot affect continuity.
 */
public final class StabilizationTracker {

    private final long scaleOutWindowMillis;
    private final long scaleInWindowMillis;
    private ScalingAction lastAction;
    private long actionFirstSeenAtMillis;
    private StabilizationState state = StabilizationState.NOT_APPLICABLE;

    public StabilizationTracker(long scaleOutWindowMillis, long scaleInWindowMillis) {
        this.scaleOutWindowMillis = scaleOutWindowMillis;
        this.scaleInWindowMillis = scaleInWindowMillis;
    }

    public synchronized StabilizationState evaluate(
            ScalingAction currentAction, long currentMonotonicMillis) {
        if (currentAction == ScalingAction.NO_ACTION
                || currentAction == ScalingAction.SCALE_IN_BLOCKED) {
            // No active scaling condition: clear the previous action and its firing state.
            clear();
            return state;
        }
        long requiredStabilizationWindowMillis =
                getRequiredStabilizationWindowMillis(currentAction);
        if (lastAction != currentAction) {
            // A new action starts a fresh stabilization window and is not firing yet.
            lastAction = currentAction;
            actionFirstSeenAtMillis = currentMonotonicMillis;
            state = StabilizationState.WAITING;
            return state;
        }
        if (state != StabilizationState.FIRING) {
            // Keep firing once the same action has satisfied its stabilization window.
            if (currentMonotonicMillis - actionFirstSeenAtMillis
                    >= requiredStabilizationWindowMillis) {
                state = StabilizationState.FIRING;
            }
        }
        return state;
    }

    public synchronized void reset() {
        clear();
    }

    private void clear() {
        lastAction = null;
        actionFirstSeenAtMillis = 0L;
        state = StabilizationState.NOT_APPLICABLE;
    }

    private long getRequiredStabilizationWindowMillis(ScalingAction action) {
        if (action == ScalingAction.SCALE_OUT) {
            return scaleOutWindowMillis;
        }
        if (action == ScalingAction.SCALE_IN_CANDIDATE) {
            return scaleInWindowMillis;
        }
        throw new IllegalArgumentException("Unsupported scaling action: " + action);
    }

    public enum StabilizationState {
        /** No scaling action requires stabilization. */
        NOT_APPLICABLE,

        /** A scaling action is being observed but has not satisfied its stabilization window. */
        WAITING,

        /** A scaling action has satisfied its stabilization window and remains active. */
        FIRING
    }
}
