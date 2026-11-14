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

import java.io.Serializable;
import java.util.Objects;

/** One policy evaluation together with the autoscaling state transition it caused. */
public final class AutoscalingEvaluationRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private final AutoscaleEvaluation evaluation;
    private final AutoscalingStateTransition stateTransition;
    private final long evaluatedAtMillis;

    public AutoscalingEvaluationRecord(
            AutoscaleEvaluation evaluation,
            AutoscalingStateTransition stateTransition,
            long evaluatedAtMillis) {
        this.evaluation = Objects.requireNonNull(evaluation, "evaluation");
        this.stateTransition = Objects.requireNonNull(stateTransition, "stateTransition");
        this.evaluatedAtMillis = evaluatedAtMillis;
    }

    public AutoscaleEvaluation getEvaluation() {
        return evaluation;
    }

    public AutoscalingStateTransition getStateTransition() {
        return stateTransition;
    }

    public long getEvaluatedAtMillis() {
        return evaluatedAtMillis;
    }
}
