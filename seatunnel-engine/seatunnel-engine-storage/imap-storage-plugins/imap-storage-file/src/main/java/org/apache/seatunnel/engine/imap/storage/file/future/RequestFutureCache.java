/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.future;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RequestFutureCache {

    private RequestFutureCache() {
        throw new IllegalStateException("Utility class");
    }

    private static AtomicLong REQUEST_ID_GEN = new AtomicLong(0);

    private static ConcurrentHashMap<Long, RequestFuture> REQUEST_MAP = new ConcurrentHashMap<>();

    public static void put(long requestId, RequestFuture requestFuture) {
        REQUEST_MAP.put(requestId, requestFuture);
    }

    public static RequestFuture get(Long requestId) {
        return REQUEST_MAP.get(requestId);
    }

    public static void remove(Long requestId) {
        REQUEST_MAP.remove(requestId);
    }

    public static long getRequestId() {
        return REQUEST_ID_GEN.incrementAndGet();
    }
}
