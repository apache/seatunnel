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

package org.apache.seatunnel.engine.core.dag.logicaldag;

import org.apache.seatunnel.engine.core.dag.actions.Action;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.List;

public class LogicalDagGenerator {
    private static final ILogger LOGGER = Logger.getLogger(LogicalDagGenerator.class);
    private List<Action> actions;

    public LogicalDagGenerator(@NonNull List<Action> actions) {
        this.actions = actions;
        if (actions.size() <= 0) {
            throw new IllegalStateException("No actions define in the job. Cannot execute.");
        }
    }

    public LogicalDag generate() {
        return null;
    }
}
