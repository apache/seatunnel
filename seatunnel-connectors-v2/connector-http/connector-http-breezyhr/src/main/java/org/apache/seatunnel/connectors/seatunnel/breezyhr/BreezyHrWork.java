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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Immutable request identity for one endpoint and position scope.
 *
 * <p>Adjacent overlap is detected with the previous page IDs. Sorted page fingerprints detect
 * repeat pages farther back. A new position starts an independent candidate list.
 */
final class BreezyHrWork implements Serializable {
    private static final long serialVersionUID = 1L;

    final BreezyHrResource resource;
    final String parent;
    final int page;
    final List<String> previousIds;
    private final List<String> seenPages;

    BreezyHrWork(BreezyHrResource resource, String parent) {
        this(resource, parent, 1, List.of(), List.of());
    }

    private BreezyHrWork(
            BreezyHrResource resource,
            String parent,
            int page,
            List<String> previousIds,
            List<String> seenPages) {
        this.resource = resource;
        this.parent = parent;
        this.page = page;
        this.previousIds = List.copyOf(previousIds);
        this.seenPages = List.copyOf(seenPages);
    }

    BreezyHrWork nextPage(List<String> currentIds) throws BreezyHrReadException {
        List<String> sorted = new ArrayList<>(currentIds);
        Collections.sort(sorted);
        String identity = BreezyHrConfig.digest(String.join("", sorted));
        int match = Collections.binarySearch(seenPages, identity);
        if (match >= 0) {
            throw failure("provider repeated a prior page");
        }
        List<String> updated = new ArrayList<>(seenPages);
        updated.add(-match - 1, identity);
        return new BreezyHrWork(resource, parent, page + 1, currentIds, updated);
    }

    BreezyHrReadException failure(String detail) {
        return new BreezyHrReadException(resource.id + " page " + page, detail);
    }
}
