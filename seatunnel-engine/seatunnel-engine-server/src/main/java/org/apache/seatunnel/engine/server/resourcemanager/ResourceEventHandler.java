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

package org.apache.seatunnel.engine.server.resourcemanager;

import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceIDRetrievable;

/** Callbacks for resource events reported by an external resource manager. */
public interface ResourceEventHandler<WorkerType extends ResourceIDRetrievable> {

    /**
     * Reports an abnormal worker termination. Successful completion and intentional release must
     * not invoke this callback.
     *
     * @param worker terminated worker; its identity is available through getResourceID()
     * @param diagnostics platform diagnostics describing the termination
     */
    void onWorkerTerminated(WorkerType worker, String diagnostics);

    /**
     * Reports a failure that prevents the driver from continuing.
     *
     * @param exception original cause of the driver failure
     */
    void onError(Throwable exception);
}
