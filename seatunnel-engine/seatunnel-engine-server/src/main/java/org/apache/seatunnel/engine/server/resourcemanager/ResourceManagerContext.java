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

import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;

/**
 * Application-owned context supplied once when an external worker driver is initialized.
 *
 * <p>The context identifies a single application and exposes its master endpoint after the master
 * has bound its listener. Drivers may retain this context until close. Accessors return immutable
 * application metadata; failure callbacks are thread-safe and may be invoked by allocation,
 * heartbeat, or watch threads. Only the first unexpected failure is retained. Callbacks received
 * after application cleanup begins are ignored because expected worker exits are part of cleanup.
 */
public interface ResourceManagerContext {
    /** @return the platform-specific identity of the application owning these workers */
    ApplicationId getApplicationId();

    /** @return the immutable deployment and fixed worker resource specification */
    ApplicationSpecification getSpecification();

    /** @return the unique Hazelcast cluster name shared only by this application's processes */
    String getClusterName();

    /**
     * @return the reachable, bound Hazelcast master endpoint as host:port, including IPv6 brackets
     */
    String getMasterAddress();

    /**
     * Reports an unrecoverable driver or external resource-manager failure.
     *
     * <p>The runtime fails the application and releases its workers. Drivers must report the cause
     * without blocking for job cancellation or attempting to restart the master.
     *
     * @param error non-null failure that prevents this application from continuing
     */
    void onError(Throwable error);

    /**
     * Reports an unexpected exit of an allocated worker while the application is active.
     *
     * <p>The fixed-size MVP fails the application rather than requesting a replacement. A driver
     * must suppress events for workers it intentionally released; the runtime also ignores all
     * callbacks once cleanup starts.
     *
     * @param workerId platform identifier previously assigned to the worker
     * @param diagnostics platform-provided failure description, or null when unavailable
     */
    void onWorkerTerminated(String workerId, String diagnostics);
}
