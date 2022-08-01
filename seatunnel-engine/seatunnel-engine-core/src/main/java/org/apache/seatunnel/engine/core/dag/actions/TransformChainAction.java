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

import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import lombok.NonNull;

import java.util.List;

public class TransformChainAction extends AbstractAction {

    private static final long serialVersionUID = -340174711145367535L;
    private final List<SeaTunnelTransform> transforms;

    public TransformChainAction(int id,
                                @NonNull String name,
                                @NonNull List<Action> upstreams,
                                @NonNull List<SeaTunnelTransform> transforms) {
        super(id, name, upstreams);
        this.transforms = transforms;
    }

    public TransformChainAction(int id,
                                @NonNull String name,
                                @NonNull List<SeaTunnelTransform> transforms) {
        super(id, name);
        this.transforms = transforms;
    }

    public List<SeaTunnelTransform> getTransforms() {
        return transforms;
    }
}
