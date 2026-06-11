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

import java.util.Collections;
import java.util.List;

/**
 * Extra metadata for {@link TransformChainAction}, mainly used for plan splitting and
 * observability.
 */
public class TransformChainConfig implements Config {

    private static final long serialVersionUID = 1L;

    private final List<String> transformNames;

    public TransformChainConfig(@NonNull List<String> transformNames) {
        this.transformNames = Collections.unmodifiableList(transformNames);
    }

    public List<String> getTransformNames() {
        return transformNames;
    }

    public String getStartTransformName() {
        return transformNames.isEmpty() ? null : transformNames.get(0);
    }

    public String getEndTransformName() {
        return transformNames.isEmpty() ? null : transformNames.get(transformNames.size() - 1);
    }
}
