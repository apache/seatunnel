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

import java.util.Objects;

/**
 * Tracks one active autoscaling direction through pending, firing, and optional recovery.
 *
 * <p>All timestamps are supplied by the caller's monotonic clock.
 */
public final class AutoscalingStateTracker {
    private final long scaleOutWindowMillis;
    private final long scaleInWindowMillis;
    private final long keepFiringMillis;

    private AutoscalingState state = AutoscalingState.NORMAL;
    private EvaluationAction stateAction;
    private long actionStartedAtMillis;
    private long recoveryStartedAtMillis;

    public AutoscalingStateTracker(
            long scaleOutWindowMillis, long scaleInWindowMillis, long keepFiringMillis) {
        if (scaleOutWindowMillis < 0 || scaleInWindowMillis < 0 || keepFiringMillis < 0) {
            throw new IllegalArgumentException("lifecycle windows must be >= 0");
        }
        this.scaleOutWindowMillis = scaleOutWindowMillis;
        this.scaleInWindowMillis = scaleInWindowMillis;
        this.keepFiringMillis = keepFiringMillis;
    }

    public synchronized AutoscalingStateTransition evaluate(
            EvaluationAction evaluationAction, long currentMonotonicMillis) {
        Objects.requireNonNull(evaluationAction, "evaluationAction");
        AutoscalingState previousState = state;
        EvaluationAction previousStateAction = stateAction;

        if (state == AutoscalingState.NORMAL) {
            if (isScalingAction(evaluationAction)) {
                beginPending(evaluationAction, currentMonotonicMillis);
            }
        } else if (state == AutoscalingState.PENDING) {
            if (evaluationAction == EvaluationAction.NO_ACTION) {
                clear();
            } else if (evaluationAction != stateAction) {
                beginPending(evaluationAction, currentMonotonicMillis);
            } else if (currentMonotonicMillis - actionStartedAtMillis
                    >= stabilizationWindowMillis(stateAction)) {
                state = AutoscalingState.FIRING;
            }
        } else if (state == AutoscalingState.FIRING) {
            if (evaluationAction == EvaluationAction.NO_ACTION) {
                if (keepFiringMillis == 0L) {
                    clear();
                } else {
                    state = AutoscalingState.RECOVERING;
                    recoveryStartedAtMillis = currentMonotonicMillis;
                }
            } else if (evaluationAction != stateAction) {
                clear();
            }
        } else if (state == AutoscalingState.RECOVERING) {
            if (evaluationAction == stateAction) {
                state = AutoscalingState.FIRING;
                recoveryStartedAtMillis = 0L;
            } else if (evaluationAction != EvaluationAction.NO_ACTION) {
                clear();
            } else if (currentMonotonicMillis - recoveryStartedAtMillis >= keepFiringMillis) {
                clear();
            }
        }
        return new AutoscalingStateTransition(
                previousState, previousStateAction, state, stateAction);
    }

    public synchronized void reset() {
        clear();
    }

    private void beginPending(EvaluationAction action, long currentMonotonicMillis) {
        stateAction = action;
        actionStartedAtMillis = currentMonotonicMillis;
        recoveryStartedAtMillis = 0L;
        state = AutoscalingState.PENDING;
    }

    private void clear() {
        state = AutoscalingState.NORMAL;
        stateAction = null;
        actionStartedAtMillis = 0L;
        recoveryStartedAtMillis = 0L;
    }

    private long stabilizationWindowMillis(EvaluationAction action) {
        return action == EvaluationAction.SCALE_OUT ? scaleOutWindowMillis : scaleInWindowMillis;
    }

    private static boolean isScalingAction(EvaluationAction action) {
        return action == EvaluationAction.SCALE_OUT || action == EvaluationAction.SCALE_IN;
    }
}
