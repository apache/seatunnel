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
package org.apache.seatunnel.connectors.seatunnel.breezyhr;

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.ArrayList;
import java.util.List;

/**
 * Durable traversal progress for one bounded Breezy HR scan.
 *
 * <p>The queue holds at most one positions page of candidate scopes, its next parent page and the
 * pipeline request. Partial publication saves a page digest and row count.
 */
final class BreezyHrSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final List<BreezyHrWork> work;
    final int requests;
    final String fingerprint;
    final int published;

    BreezyHrSplit(BreezyHrConfig config) {
        this(config.scope, initial(config), 0, null, 0);
    }

    private BreezyHrSplit(
            String scope,
            List<BreezyHrWork> work,
            int requests,
            String fingerprint,
            int published) {
        this.scope = scope;
        this.work = List.copyOf(work);
        this.requests = requests;
        this.fingerprint = fingerprint;
        this.published = published;
    }

    BreezyHrSplit partial(String fingerprint, int published) {
        return new BreezyHrSplit(scope, work, requests, fingerprint, published);
    }

    BreezyHrSplit complete(List<BreezyHrWork> next) {
        List<BreezyHrWork> pending = new ArrayList<>(next.size() + work.size() - 1);
        pending.addAll(next);
        pending.addAll(work.subList(1, work.size()));
        return new BreezyHrSplit(scope, pending, requests + 1, null, 0);
    }

    private static List<BreezyHrWork> initial(BreezyHrConfig config) {
        List<BreezyHrWork> plan = new ArrayList<>(2);
        if (config.resources.contains(BreezyHrResource.POSITIONS)
                || config.resources.contains(BreezyHrResource.CANDIDATES)) {
            plan.add(new BreezyHrWork(BreezyHrResource.POSITIONS, null));
        }
        if (config.resources.contains(BreezyHrResource.PIPELINES)) {
            plan.add(new BreezyHrWork(BreezyHrResource.PIPELINES, null));
        }
        return plan;
    }

    @Override
    public String splitId() {
        return "breezyhr-company";
    }
}
