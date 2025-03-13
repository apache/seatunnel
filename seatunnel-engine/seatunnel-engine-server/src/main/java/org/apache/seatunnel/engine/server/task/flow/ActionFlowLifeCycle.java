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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.server.checkpoint.Stateful;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;

public abstract class ActionFlowLifeCycle extends AbstractFlowLifeCycle implements Stateful {

    protected Action action;

    public ActionFlowLifeCycle(
            Action action, SeaTunnelTask runningTask, CompletableFuture<Void> completableFuture) {
        super(runningTask, completableFuture);
        this.action = action;
    }

    public Action getAction() {
        return action;
    }
}
