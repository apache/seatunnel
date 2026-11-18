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

package org.apache.seatunnel.resource.yarn.launch.resources;

import java.util.Objects;

/** A named YARN resource backed by the Hadoop record sent to, or received from, the cluster. */
public abstract class YarnResource<T> {
    private final String name;
    private final T internalResource;

    protected YarnResource(String name, T internalResource) {
        this.name = Objects.requireNonNull(name, "name");
        this.internalResource = Objects.requireNonNull(internalResource, "internalResource");
    }

    public final String getName() {
        return name;
    }

    /** Returns the wrapped Hadoop record itself so changes made during assembly reach the SDK. */
    public final T getInternalResource() {
        return internalResource;
    }
}
