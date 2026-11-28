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

package org.apache.seatunnel.connectors.seatunnel.confluence;

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** A versioned, secret-free position in a bounded API scan. */
final class ConfluenceSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private static final int VERSION = 1;

    final int version;
    final String binding;
    final String position;
    final int pages;
    final int emitted;
    final String prefix;
    final boolean finished;
    final Set<String> visited;

    ConfluenceSplit(
            String binding,
            String position,
            int pages,
            int emitted,
            String prefix,
            boolean finished,
            Set<String> visited) {
        this.version = VERSION;
        this.binding = binding;
        this.position = position;
        this.pages = pages;
        this.emitted = emitted;
        this.prefix = prefix;
        this.finished = finished;
        this.visited = new HashSet<>(visited);
    }

    static ConfluenceSplit initial(ConfluenceConfig config) {
        String position = config.initialPosition();
        Set<String> history =
                config.resource.cursor()
                        ? Collections.singleton(ConfluencePagination.fingerprint(position))
                        : Collections.emptySet();
        return new ConfluenceSplit(config.binding, position, 0, 0, null, false, history);
    }

    void check(ConfluenceConfig config) {
        if (version != VERSION
                || !config.binding.equals(binding)
                || position == null
                || pages < 0
                || pages > config.maxPages
                || emitted < 0
                || emitted > config.pageSize
                || (emitted == 0 ? prefix != null : prefix == null)
                || visited == null
                || visited.size() > config.maxPages
                || (finished && emitted != 0)) {
            throw config.failure("incompatible checkpoint; start a new full refresh");
        }
        if (config.resource.cursor()) {
            if ((!position.isEmpty() && !ConfluencePagination.cursorToken(position))
                    || !visited.contains(ConfluencePagination.fingerprint(position))) {
                throw config.failure("invalid checkpoint cursor; start a new full refresh");
            }
        } else {
            ConfluencePagination.offset(config, position);
        }
    }

    @Override
    public String splitId() {
        return "confluence";
    }
}
