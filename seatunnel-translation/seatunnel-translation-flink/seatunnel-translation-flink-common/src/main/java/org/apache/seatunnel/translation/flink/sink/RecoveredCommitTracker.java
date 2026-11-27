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

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Set;

/** Tracks objects read from global operator state until its recovery callback. */
public final class RecoveredCommitTracker {
    private final Set<WeakReference<CommitWrapper<?>>> restored = new HashSet<>();
    private final ReferenceQueue<CommitWrapper<?>> queue = new ReferenceQueue<>();
    private boolean initializing = true;

    public void deserialized(CommitWrapper<?> wrapper) {
        if (initializing) {
            Reference<? extends CommitWrapper<?>> expired;
            while ((expired = queue.poll()) != null) {
                restored.remove(expired);
            }
            restored.add(new WeakReference<>(wrapper, queue));
        }
    }

    void finishRestore() {
        for (WeakReference<CommitWrapper<?>> reference : restored) {
            CommitWrapper<?> wrapper = reference.get();
            if (wrapper != null) {
                wrapper.markRecovered();
            }
        }
        restored.clear();
        initializing = false;
    }
}
