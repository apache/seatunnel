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

package org.apache.seatunnel.engine.server.checkpoint;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ActionState implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The id of the action.
     */
    private final String actionId;

    /**
     * The handles to states created by the parallel actions: action index -> action state.
     */
    private final List<ActionSubtaskState> subtaskStates;

    private ActionSubtaskState coordinatorState;

    /**
     * The parallelism of the action when it was checkpointed.
     */
    private final int parallelism;

    public ActionState(String actionId, int parallelism) {
        this.actionId = actionId;
        this.subtaskStates = Arrays.asList(new ActionSubtaskState[parallelism]);
        this.parallelism = parallelism;
    }

    public String getActionId() {
        return actionId;
    }

    public List<ActionSubtaskState> getSubtaskStates() {
        return subtaskStates;
    }

    public ActionSubtaskState getCoordinatorState() {
        return coordinatorState;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void reportState(int index, ActionSubtaskState state) {
        if (index < 0) {
            coordinatorState = state;
            return;
        }
        subtaskStates.set(index, state);
    }
}
