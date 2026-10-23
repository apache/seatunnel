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

package org.apache.seatunnel.engine.server.execution;

/** Outcome of asking {@link CooperativeWorkerBudget} to admit one worker promotion. */
public enum PromotionDecision {

    /** The promotion fits both the node budget and the budget of its job. */
    ADMITTED,

    /** The node already holds as many promoted workers as it is allowed to. */
    NODE_BUDGET_EXHAUSTED,

    /** The job already holds as many promoted workers on this node as it is allowed to. */
    JOB_BUDGET_EXHAUSTED;

    /** @return true when the promotion may be carried out */
    public boolean isAdmitted() {
        return this == ADMITTED;
    }
}
