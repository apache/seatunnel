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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Checkpoint for a single logical scan. Only durable positions and topology are serialized. */
final class KinesisSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private static final int VERSION = 1;
    private final int version;
    final String scope;
    final Instant anchor;
    String arn;
    Instant created;
    boolean complete;
    final Map<String, ShardState> shards = new LinkedHashMap<>();

    KinesisSplit(String scope, Instant anchor) {
        version = VERSION;
        this.scope = scope;
        this.anchor = anchor;
    }

    private KinesisSplit(KinesisSplit state) {
        version = state.version;
        scope = state.scope;
        anchor = state.anchor;
        arn = state.arn;
        created = state.created;
        complete = state.complete;
        state.shards.forEach((id, shard) -> shards.put(id, new ShardState(shard)));
    }

    void verify(String expected) {
        if (version != VERSION || !scope.equals(expected)) {
            throw new IllegalStateException("Kinesis checkpoint version or stream settings differ");
        }
    }

    KinesisSplit copy() {
        return new KinesisSplit(this);
    }

    @Override
    public String splitId() {
        return "kinesis-stream";
    }

    static final class ShardState implements Serializable {
        private static final long serialVersionUID = 1L;
        final String id;
        final List<String> parents;
        final boolean discoveredAtStart;
        boolean closed;
        String committed;
        boolean done;

        ShardState(String id, List<String> parents, boolean discoveredAtStart, boolean closed) {
            this.id = id;
            this.parents = new ArrayList<>(parents);
            this.discoveredAtStart = discoveredAtStart;
            this.closed = closed;
        }

        ShardState(ShardState shard) {
            this(shard.id, shard.parents, shard.discoveredAtStart, shard.closed);
            committed = shard.committed;
            done = shard.done;
        }
    }
}
