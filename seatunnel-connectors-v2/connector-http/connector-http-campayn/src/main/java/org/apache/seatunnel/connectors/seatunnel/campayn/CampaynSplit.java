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
package org.apache.seatunnel.connectors.seatunnel.campayn;

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.List;

/**
 * Immutable request and row progress across Campayn's root and child arrays.
 *
 * <p>A compact list of parent IDs is shared by all child reads and released after traversal. A
 * partial response stores only its fingerprint and emitted offset.
 */
final class CampaynSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final int resourceIndex;
    final List<String> parents;
    final int parentIndex;
    final int requests;
    final String fingerprint;
    final int published;

    CampaynSplit(CampaynConfig config) {
        this(config.scope, 0, List.of(), 0, 0, null, 0);
    }

    private CampaynSplit(
            String scope,
            int resourceIndex,
            List<String> parents,
            int parentIndex,
            int requests,
            String fingerprint,
            int published) {
        this.scope = scope;
        this.resourceIndex = resourceIndex;
        this.parents = List.copyOf(parents);
        this.parentIndex = parentIndex;
        this.requests = requests;
        this.fingerprint = fingerprint;
        this.published = published;
    }

    String parent(CampaynResource resource) {
        return resource.listScoped ? parents.get(parentIndex) : null;
    }

    CampaynSplit partial(String fingerprint, int published) {
        return new CampaynSplit(
                scope, resourceIndex, parents, parentIndex, requests, fingerprint, published);
    }

    CampaynSplit advance(CampaynConfig config, CampaynResource resource, List<String> discovered) {
        if (resource.listScoped && parentIndex + 1 < parents.size()) {
            return new CampaynSplit(
                    scope, resourceIndex, parents, parentIndex + 1, requests + 1, null, 0);
        }
        List<String> selectedParents = resource == CampaynResource.LISTS ? discovered : parents;
        int next = resourceIndex + 1;
        while (next < config.plan.size()
                && config.plan.get(next).listScoped
                && selectedParents.isEmpty()) {
            next++;
        }
        boolean needsParents = next < config.plan.size() && config.plan.get(next).listScoped;
        return new CampaynSplit(
                scope, next, needsParents ? selectedParents : List.of(), 0, requests + 1, null, 0);
    }

    @Override
    public String splitId() {
        return "campayn-tenant";
    }
}
