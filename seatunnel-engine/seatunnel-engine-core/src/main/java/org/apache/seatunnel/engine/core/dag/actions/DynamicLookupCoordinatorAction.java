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

import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;

import lombok.NonNull;

import java.net.URL;
import java.util.Set;

/**
 * Control-plane action paired one-to-one with a {@link DynamicLookupAction}.
 *
 * <p>The coordinator ID is deterministically placed in the negative half of the ID space so that
 * physical-plan regeneration cannot create a second checkpoint identity.
 */
public final class DynamicLookupCoordinatorAction extends AbstractAction {

    private static final long serialVersionUID = 1L;

    /** Stable identity shared with the paired lookup action. */
    private final String operatorUid;

    /** Logical identity of the paired lookup action. */
    private final long lookupActionId;

    /**
     * Creates the control-plane action paired with a lookup declaration.
     *
     * @param id deterministic coordinator action ID
     * @param name coordinator action name
     * @param operatorUid stable lookup operator identity
     * @param lookupActionId paired lookup action ID
     * @param jarUrls action dependency URLs
     * @param connectorJarIdentifiers connector plugin dependencies
     */
    public DynamicLookupCoordinatorAction(
            long id,
            @NonNull String name,
            @NonNull String operatorUid,
            long lookupActionId,
            @NonNull Set<URL> jarUrls,
            @NonNull Set<ConnectorJarIdentifier> connectorJarIdentifiers) {
        super(id, name, jarUrls, connectorJarIdentifiers);
        this.operatorUid = operatorUid;
        this.lookupActionId = lookupActionId;
        setParallelism(1);
    }

    /** Creates the unique coordinator action for a dynamic lookup action. */
    public static DynamicLookupCoordinatorAction from(DynamicLookupAction lookupAction) {
        long coordinatorActionId = lookupAction.getId() ^ Long.MIN_VALUE;
        return new DynamicLookupCoordinatorAction(
                coordinatorActionId,
                lookupAction.getName() + "-Coordinator",
                lookupAction.getOperatorUid(),
                lookupAction.getId(),
                lookupAction.getJarUrls(),
                lookupAction.getConnectorJarIdentifiers());
    }

    public String getOperatorUid() {
        return operatorUid;
    }

    public long getLookupActionId() {
        return lookupActionId;
    }
}
