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
package org.apache.seatunnel.connectors.seatunnel.bugsnag;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Durable scan window and immutable progress across nested BugSnag requests.
 *
 * <p>The queue holds the current branch and deferred siblings. Partial output stores its page
 * fingerprint and accepted row count in the same checkpoint as the scheduled request.
 */
final class BugSnagSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private static final int MAX_PENDING_REQUESTS = 13000;

    final String scope;
    final Instant end;
    final List<BugSnagWork> work;
    final int requests;
    final String fingerprint;
    final int published;

    BugSnagSplit(BugSnagConfig config) {
        this(config.scope, config.scanEnd(), initial(config), 0, null, 0);
    }

    private BugSnagSplit(
            String scope,
            Instant end,
            List<BugSnagWork> work,
            int requests,
            String fingerprint,
            int published) {
        this.scope = scope;
        this.end = end;
        this.work = List.copyOf(work);
        this.requests = requests;
        this.fingerprint = fingerprint;
        this.published = published;
    }

    BugSnagSplit partial(String fingerprint, int published) {
        return new BugSnagSplit(scope, end, work, requests, fingerprint, published);
    }

    BugSnagSplit complete(List<BugSnagWork> additions) throws BugSnagReadException {
        int size = additions.size() + work.size() - 1;
        if (size > MAX_PENDING_REQUESTS) {
            throw work.get(0).failure("exceeded the pending discovery work limit");
        }
        List<BugSnagWork> pending = new ArrayList<>(size);
        pending.addAll(additions);
        pending.addAll(work.subList(1, work.size()));
        return new BugSnagSplit(scope, end, pending, requests + 1, null, 0);
    }

    private static List<BugSnagWork> initial(BugSnagConfig config) {
        List<BugSnagWork> plan = new ArrayList<>(2);
        if (config.resources.stream()
                .anyMatch(resource -> resource != BugSnagResource.SUPPORTED_INTEGRATIONS)) {
            plan.add(new BugSnagWork(BugSnagResource.ORGANIZATIONS, null, null, null));
        }
        if (config.resources.contains(BugSnagResource.SUPPORTED_INTEGRATIONS)) {
            plan.add(new BugSnagWork(BugSnagResource.SUPPORTED_INTEGRATIONS, null, null, null));
        }
        return plan;
    }

    @Override
    public String splitId() {
        return "bugsnag-data-access";
    }
}
