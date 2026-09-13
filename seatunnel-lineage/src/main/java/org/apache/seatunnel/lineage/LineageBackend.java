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

package org.apache.seatunnel.lineage;

/** A transport-independent extension point for lineage event delivery. */
public interface LineageBackend {

    /** Returns the name used by {@link LineageConfig#transport()}. */
    String getName();

    /**
     * Emits one already-built lineage event.
     *
     * <p>May block the calling thread synchronously; an implementation that retries against {@code
     * config.timeoutMs()} and {@code config.retryTimes()} can block for up to {@code (retryTimes +
     * 1) * timeoutMs} in the worst case (a receiver that neither responds nor refuses the
     * connection). Callers on a latency-sensitive thread should account for that bound.
     */
    void emit(LineageConfig config, LineageEvent event) throws Exception;
}
