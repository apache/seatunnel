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

package org.apache.seatunnel.engine.server.common.jar;

import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.RefCount;
import org.apache.seatunnel.engine.server.common.statestore.StateStore;

/**
 * Store for reference counts of shared connector jars.
 *
 * <p>This store tracks how many jobs currently reference a given shared connector jar. The
 * reference count is used to prevent premature deletion and to trigger cleanup when a jar is no
 * longer referenced.
 */
public interface ConnectorJarReferenceStateStore
        extends StateStore<ConnectorJarIdentifier, RefCount> {

    /**
     * Increases the reference count for the given connector jar.
     *
     * @param connectorJarIdentifier connector jar identifier
     */
    void increaseReference(ConnectorJarIdentifier connectorJarIdentifier);

    /**
     * Decreases the reference count for the given connector jar.
     *
     * @param connectorJarIdentifier connector jar identifier
     */
    void decreaseReference(ConnectorJarIdentifier connectorJarIdentifier);
}
