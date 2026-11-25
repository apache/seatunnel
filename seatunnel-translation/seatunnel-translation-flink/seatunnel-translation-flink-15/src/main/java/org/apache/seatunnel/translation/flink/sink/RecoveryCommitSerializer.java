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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/** Tracks initial state reads without changing the existing committable format. */
final class RecoveryCommitSerializer<T> implements SimpleVersionedSerializer<CommitWrapper<T>> {
    private final SimpleVersionedSerializer<CommitWrapper<T>> delegate;
    private final RecoveredCommitTracker tracker;

    RecoveryCommitSerializer(
            SimpleVersionedSerializer<CommitWrapper<T>> delegate, RecoveredCommitTracker tracker) {
        this.delegate = delegate;
        this.tracker = tracker;
    }

    public int getVersion() {
        return delegate.getVersion();
    }

    public byte[] serialize(CommitWrapper<T> value) throws IOException {
        return delegate.serialize(value);
    }

    public CommitWrapper<T> deserialize(int version, byte[] bytes) throws IOException {
        CommitWrapper<T> wrapper = delegate.deserialize(version, bytes);
        tracker.deserialized(wrapper);
        return wrapper;
    }
}
