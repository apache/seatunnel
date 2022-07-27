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

package org.apache.seatunnel.engine.core.dag.actions;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractAction implements Action {
    private String name;
    private List<Action> upstreams = new ArrayList<>();
    // This is used to assign a unique ID to every Action
    protected static Integer ID_COUNTER = 0;

    private int id;

    private int parallelism = 1;

    protected AbstractAction(@NonNull String name, @NonNull List<Action> upstreams) {
        this.name = name;
        this.upstreams = upstreams;
        this.id = getNewNodeId();
    }

    protected AbstractAction(@NonNull String name) {
        this.name = name;
    }

    public static int getNewNodeId() {
        ID_COUNTER++;
        return ID_COUNTER;
    }

    @NonNull
    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(@NonNull String name) {
        this.name = name;
    }

    @NonNull
    @Override
    public List<Action> getUpstream() {
        return upstreams;
    }

    @Override
    public void addUpstream(@NonNull Action action) {
        this.upstreams.add(action);
    }

    @Override
    public int getParallelism() {
        return parallelism;
    }

    @Override
    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public int getId() {
        return id;
    }
}
