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

package org.apache.seatunnel.resource.yarn.launch.decorators;

import org.apache.seatunnel.resource.yarn.launch.resources.YarnContainerSpec;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnResource;

import java.io.IOException;
import java.util.List;

/**
 * One feature of an AM or worker launch. Each role's factory selects the steps and their order. The
 * submitter uses their resource declarations for staging. The AM uses the worker declarations to
 * resolve the files that a NodeManager must localize.
 *
 * @param <R> kind of accompanying YARN resource this chain builds
 */
public interface YarnStepDecorator<R extends YarnResource<?>> {
    /**
     * Applies this feature to a single launch after the role's local resources have been resolved.
     *
     * @param spec container produced by the factory or the preceding step
     * @return the spec that the next step should receive
     * @throws IOException if preparing launch data such as filesystem credentials fails
     */
    YarnContainerSpec decorate(YarnContainerSpec spec) throws IOException;

    /**
     * Declares the resources owned by this feature in staging order. Callers may build the
     * declarations separately for upload and for metadata lookup. File I/O belongs to those
     * operations.
     *
     * @return ordered resources, or an empty list for a feature that only decorates the container
     */
    List<R> build();
}
