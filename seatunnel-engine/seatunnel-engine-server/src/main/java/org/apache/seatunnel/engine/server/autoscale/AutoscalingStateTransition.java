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

/** Immutable description of an autoscaling state transition and its associated actions. */
public final class AutoscalingStateTransition implements Serializable {
    private static final long serialVersionUID = 1L;

    private final AutoscalingState previousState;
    private final EvaluationAction previousStateAction;
    private final AutoscalingState currentState;
    private final EvaluationAction currentStateAction;

    public AutoscalingStateTransition(
            AutoscalingState previousState,
            EvaluationAction previousStateAction,
            AutoscalingState currentState,
            EvaluationAction currentStateAction) {
        this.previousState = previousState;
        this.previousStateAction = previousStateAction;
        this.currentState = currentState;
        this.currentStateAction = currentStateAction;
    }

    public AutoscalingState getPreviousState() {
        return previousState;
    }

    public EvaluationAction getPreviousStateAction() {
        return previousStateAction;
    }

    public AutoscalingState getCurrentState() {
        return currentState;
    }

    public EvaluationAction getCurrentStateAction() {
        return currentStateAction;
    }

    public boolean startsFiring() {
        return previousState == AutoscalingState.PENDING && currentState == AutoscalingState.FIRING;
    }

    public boolean remainsFiring() {
        return previousState == AutoscalingState.FIRING && currentState == AutoscalingState.FIRING;
    }
}
