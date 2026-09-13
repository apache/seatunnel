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

import java.util.concurrent.TimeUnit;

class StabilizationTrackerTest {

    @Test
    void scaleOutRequiresContinuousMonotonicWindow() {
        StabilizationTracker tracker =
                new StabilizationTracker(
                        TimeUnit.SECONDS.toNanos(300), TimeUnit.SECONDS.toNanos(600));

        Assertions.assertFalse(tracker.isStabilized(ScalingAction.SCALE_OUT, 10L));
        Assertions.assertFalse(
                tracker.isStabilized(ScalingAction.SCALE_OUT, 10L + TimeUnit.SECONDS.toNanos(299)));
        Assertions.assertTrue(
                tracker.isStabilized(ScalingAction.SCALE_OUT, 10L + TimeUnit.SECONDS.toNanos(300)));
    }

    @Test
    void differentActionResetsWindow() {
        StabilizationTracker tracker =
                new StabilizationTracker(
                        TimeUnit.SECONDS.toNanos(300), TimeUnit.SECONDS.toNanos(600));

        tracker.isStabilized(ScalingAction.SCALE_OUT, 10L);
        Assertions.assertTrue(
                tracker.isStabilized(ScalingAction.NO_ACTION, 10L + TimeUnit.SECONDS.toNanos(200)));
        Assertions.assertFalse(
                tracker.isStabilized(ScalingAction.SCALE_OUT, 10L + TimeUnit.SECONDS.toNanos(400)));
    }

    @Test
    void resetClearsContinuity() {
        StabilizationTracker tracker =
                new StabilizationTracker(
                        TimeUnit.SECONDS.toNanos(300), TimeUnit.SECONDS.toNanos(600));

        tracker.isStabilized(ScalingAction.SCALE_IN_CANDIDATE, 10L);
        tracker.reset();

        Assertions.assertFalse(
                tracker.isStabilized(
                        ScalingAction.SCALE_IN_CANDIDATE, 10L + TimeUnit.SECONDS.toNanos(700)));
    }

    @Test
    void noActionIsImmediatelyStableAndDoesNotStartScalingWindow() {
        StabilizationTracker tracker =
                new StabilizationTracker(
                        TimeUnit.SECONDS.toNanos(300), TimeUnit.SECONDS.toNanos(600));

        Assertions.assertTrue(tracker.isStabilized(ScalingAction.NO_ACTION, 10L));
        Assertions.assertFalse(tracker.isStabilized(ScalingAction.SCALE_OUT, 11L));
    }
}
