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

class StabilizationTrackerTest {

    @Test
    void scaleOutRequiresContinuousMonotonicWindow() {
        StabilizationTracker tracker = new StabilizationTracker(300_000L, 600_000L);

        Assertions.assertEquals(
                StabilizationTracker.StabilizationState.WAITING,
                tracker.evaluate(ScalingAction.SCALE_OUT, 10L));
        Assertions.assertEquals(
                StabilizationTracker.StabilizationState.WAITING,
                tracker.evaluate(ScalingAction.SCALE_OUT, 10L + 299_000L));
        Assertions.assertEquals(
                StabilizationTracker.StabilizationState.FIRING,
                tracker.evaluate(ScalingAction.SCALE_OUT, 10L + 300_000L));
    }

    @Test
    void differentActionResetsWindow() {
        StabilizationTracker tracker = new StabilizationTracker(300_000L, 600_000L);

        tracker.evaluate(ScalingAction.SCALE_OUT, 10L);
        Assertions.assertEquals(
                StabilizationTracker.StabilizationState.NOT_APPLICABLE,
                tracker.evaluate(ScalingAction.NO_ACTION, 10L + 200_000L));
        Assertions.assertEquals(
                StabilizationTracker.StabilizationState.WAITING,
                tracker.evaluate(ScalingAction.SCALE_OUT, 10L + 400_000L));
    }

    @Test
    void resetClearsContinuity() {
        StabilizationTracker tracker = new StabilizationTracker(300_000L, 600_000L);

        tracker.evaluate(ScalingAction.SCALE_IN_CANDIDATE, 10L);
        tracker.reset();

        Assertions.assertEquals(
                StabilizationTracker.StabilizationState.WAITING,
                tracker.evaluate(ScalingAction.SCALE_IN_CANDIDATE, 10L + 700_000L));
    }

    @Test
    void noActionIsNotApplicableAndDoesNotStartScalingWindow() {
        StabilizationTracker tracker = new StabilizationTracker(300_000L, 600_000L);

        Assertions.assertEquals(
                StabilizationTracker.StabilizationState.NOT_APPLICABLE,
                tracker.evaluate(ScalingAction.NO_ACTION, 10L));
        Assertions.assertEquals(
                StabilizationTracker.StabilizationState.WAITING,
                tracker.evaluate(ScalingAction.SCALE_OUT, 11L));
    }
}
