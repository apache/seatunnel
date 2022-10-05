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

package org.apache.seatunnel.engine.core.dag.internal;

import java.io.Serializable;

public class IntermediateQueue implements Serializable {

    private static final long serialVersionUID = -3049265155605303992L;

    private final long id;
    private final int parallelism;
    private final String name;

    public IntermediateQueue(long id, String name, int parallelism) {
        this.id = id;
        this.name = name;
        this.parallelism = parallelism;
    }

    public long getId() {
        return id;
    }

    public int getParallelism() {
        return parallelism;
    }

    public String getName() {
        return name;
    }
}
