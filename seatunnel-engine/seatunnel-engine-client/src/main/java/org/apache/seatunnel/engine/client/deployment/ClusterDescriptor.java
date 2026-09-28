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

package org.apache.seatunnel.engine.client.deployment;

import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;

/**
 * Deploys and manages applications through the external resource platform.
 *
 * <p>Application operations do not require a live Engine master. {@link #retrieve(Object)}
 * discovers the master connection settings without opening an Engine client. The caller owns the
 * descriptor and any clients created by the returned provider; closing either releases its own
 * connections without canceling the application.
 *
 * @param <ID> the platform's native application identity
 */
public interface ClusterDescriptor<ID> extends AutoCloseable {

    /** Deploys an application and returns its platform ID without connecting an Engine client. */
    ID deployApplication(ApplicationSpecification specification) throws Exception;

    /**
     * Discovers a running application's master and returns a factory for independently owned
     * clients.
     */
    SeatunnelClientProvider retrieve(ID id) throws Exception;

    /** Queries application state through the platform, including after the master has exited. */
    ApplicationStatus getApplicationStatus(ID id) throws Exception;

    /** Cancels the application and releases its platform-owned resources. */
    void cancelApplication(ID id) throws Exception;

    /** Releases only this descriptor's platform connections, leaving applications running. */
    @Override
    void close() throws Exception;
}
