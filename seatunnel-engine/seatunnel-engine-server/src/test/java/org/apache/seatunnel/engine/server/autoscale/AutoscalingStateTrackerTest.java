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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AutoscalingStateTrackerTest {

    @Test
    void confirmsPendingActionAfterItsStabilizationWindow() {
        AutoscalingStateTracker tracker = new AutoscalingStateTracker(100L, 200L, 0L);

        Assertions.assertEquals(
                AutoscalingState.PENDING,
                tracker.evaluate(EvaluationAction.SCALE_OUT, 0L).getCurrentState());
        Assertions.assertEquals(
                AutoscalingState.PENDING,
                tracker.evaluate(EvaluationAction.SCALE_OUT, 99L).getCurrentState());
        AutoscalingStateTransition transition = tracker.evaluate(EvaluationAction.SCALE_OUT, 100L);

        Assertions.assertEquals(AutoscalingState.FIRING, transition.getCurrentState());
        Assertions.assertEquals(EvaluationAction.SCALE_OUT, transition.getCurrentStateAction());
    }

    @Test
    void cancelsPendingActionWhenConditionDisappears() {
        AutoscalingStateTracker tracker = new AutoscalingStateTracker(100L, 200L, 0L);

        tracker.evaluate(EvaluationAction.SCALE_IN, 0L);
        AutoscalingStateTransition transition = tracker.evaluate(EvaluationAction.NO_ACTION, 1L);

        Assertions.assertEquals(AutoscalingState.PENDING, transition.getPreviousState());
        Assertions.assertEquals(AutoscalingState.NORMAL, transition.getCurrentState());
        Assertions.assertNull(transition.getCurrentStateAction());
    }

    @Test
    void resumesFiringWhenActionReturnsDuringRecovery() {
        AutoscalingStateTracker tracker = new AutoscalingStateTracker(0L, 0L, 100L);

        tracker.evaluate(EvaluationAction.SCALE_OUT, 0L);
        tracker.evaluate(EvaluationAction.SCALE_OUT, 0L);
        Assertions.assertEquals(
                AutoscalingState.RECOVERING,
                tracker.evaluate(EvaluationAction.NO_ACTION, 1L).getCurrentState());

        AutoscalingStateTransition transition = tracker.evaluate(EvaluationAction.SCALE_OUT, 2L);

        Assertions.assertEquals(AutoscalingState.FIRING, transition.getCurrentState());
        Assertions.assertEquals(EvaluationAction.SCALE_OUT, transition.getCurrentStateAction());
    }

    @Test
    void resolvesAfterRecoveryWindowElapses() {
        AutoscalingStateTracker tracker = new AutoscalingStateTracker(0L, 0L, 100L);

        tracker.evaluate(EvaluationAction.SCALE_OUT, 0L);
        tracker.evaluate(EvaluationAction.SCALE_OUT, 0L);
        tracker.evaluate(EvaluationAction.NO_ACTION, 1L);

        Assertions.assertEquals(
                AutoscalingState.RECOVERING,
                tracker.evaluate(EvaluationAction.NO_ACTION, 100L).getCurrentState());
        AutoscalingStateTransition transition = tracker.evaluate(EvaluationAction.NO_ACTION, 101L);
        Assertions.assertEquals(AutoscalingState.NORMAL, transition.getCurrentState());
        Assertions.assertEquals(EvaluationAction.SCALE_OUT, transition.getPreviousStateAction());
    }

    @Test
    void resolvesFiringDirectionBeforeTrackingTheOppositeDirection() {
        AutoscalingStateTracker tracker = new AutoscalingStateTracker(0L, 0L, 0L);

        tracker.evaluate(EvaluationAction.SCALE_OUT, 0L);
        tracker.evaluate(EvaluationAction.SCALE_OUT, 0L);
        AutoscalingStateTransition resolved = tracker.evaluate(EvaluationAction.SCALE_IN, 1L);

        Assertions.assertEquals(AutoscalingState.FIRING, resolved.getPreviousState());
        Assertions.assertEquals(EvaluationAction.SCALE_OUT, resolved.getPreviousStateAction());
        Assertions.assertEquals(AutoscalingState.NORMAL, resolved.getCurrentState());
        Assertions.assertNull(resolved.getCurrentStateAction());

        AutoscalingStateTransition pending = tracker.evaluate(EvaluationAction.SCALE_IN, 2L);
        Assertions.assertEquals(AutoscalingState.NORMAL, pending.getPreviousState());
        Assertions.assertEquals(AutoscalingState.PENDING, pending.getCurrentState());
        Assertions.assertEquals(EvaluationAction.SCALE_IN, pending.getCurrentStateAction());
    }

    @Test
    void resolvesRecoveringDirectionBeforeTrackingTheOppositeDirection() {
        AutoscalingStateTracker tracker = new AutoscalingStateTracker(0L, 0L, 100L);

        tracker.evaluate(EvaluationAction.SCALE_OUT, 0L);
        tracker.evaluate(EvaluationAction.SCALE_OUT, 0L);
        tracker.evaluate(EvaluationAction.NO_ACTION, 1L);
        AutoscalingStateTransition resolved = tracker.evaluate(EvaluationAction.SCALE_IN, 2L);

        Assertions.assertEquals(AutoscalingState.RECOVERING, resolved.getPreviousState());
        Assertions.assertEquals(EvaluationAction.SCALE_OUT, resolved.getPreviousStateAction());
        Assertions.assertEquals(AutoscalingState.NORMAL, resolved.getCurrentState());
        Assertions.assertNull(resolved.getCurrentStateAction());
    }

    @Test
    void replacesOppositePendingCandidateAndRestartsItsWindow() {
        AutoscalingStateTracker tracker = new AutoscalingStateTracker(100L, 200L, 0L);

        tracker.evaluate(EvaluationAction.SCALE_OUT, 0L);
        AutoscalingStateTransition replaced = tracker.evaluate(EvaluationAction.SCALE_IN, 50L);

        Assertions.assertEquals(AutoscalingState.PENDING, replaced.getPreviousState());
        Assertions.assertEquals(EvaluationAction.SCALE_OUT, replaced.getPreviousStateAction());
        Assertions.assertEquals(AutoscalingState.PENDING, replaced.getCurrentState());
        Assertions.assertEquals(EvaluationAction.SCALE_IN, replaced.getCurrentStateAction());
        Assertions.assertEquals(
                AutoscalingState.PENDING,
                tracker.evaluate(EvaluationAction.SCALE_IN, 249L).getCurrentState());
        Assertions.assertEquals(
                AutoscalingState.FIRING,
                tracker.evaluate(EvaluationAction.SCALE_IN, 250L).getCurrentState());
    }
}
