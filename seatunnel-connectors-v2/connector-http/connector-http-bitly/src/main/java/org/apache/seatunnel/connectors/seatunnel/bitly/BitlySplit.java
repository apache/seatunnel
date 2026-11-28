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
package org.apache.seatunnel.connectors.seatunnel.bitly;

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.ArrayList;
import java.util.List;

/**
 * Immutable account traversal state for one Bitly worker.
 *
 * <p>The pending work identifies the owning organization, group and resource. A partial position
 * keeps only the response fingerprint and collector offset, without real records or image data.
 */
final class BitlySplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    static final int MAX_PENDING = 10000;

    final String scope;
    final List<BitlyWork> work;
    final int used;
    final String fingerprint;
    final int published;

    BitlySplit(BitlyConfig config) {
        this(config.scope, List.copyOf(BitlyWork.initial(config)), 0, null, 0);
    }

    private BitlySplit(
            String scope, List<BitlyWork> work, int used, String fingerprint, int published) {
        this.scope = scope;
        this.work = work;
        this.used = used;
        this.fingerprint = fingerprint;
        this.published = published;
    }

    /**
     * Replaces the head request with its children and continuation after its output is published.
     *
     * <p>Processing a group's descendants before later groups bounds the live continuation state.
     */
    BitlySplit complete(List<BitlyWork> children, int requests) throws BitlyReadException {
        int remaining = work.size() - 1;
        if (children.size() > MAX_PENDING - remaining) {
            throw work.get(0).failure("account fan-out exceeds the pending work limit");
        }
        List<BitlyWork> next = new ArrayList<>(children.size() + remaining);
        next.addAll(children);
        next.addAll(work.subList(1, work.size()));
        return new BitlySplit(scope, List.copyOf(next), used + requests, null, 0);
    }

    BitlySplit partial(String digest, int outputs) {
        return new BitlySplit(scope, work, used, digest, outputs);
    }

    @Override
    public String splitId() {
        return "bitly-account";
    }
}
