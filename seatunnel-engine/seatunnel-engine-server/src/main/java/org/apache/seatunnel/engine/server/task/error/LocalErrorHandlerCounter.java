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

package org.apache.seatunnel.engine.server.task.error;

import java.util.concurrent.atomic.AtomicLong;

/** In-memory counter used by unit tests and non-engine fallback paths. */
public class LocalErrorHandlerCounter implements ErrorHandlerCounter {

    private final AtomicLong totalRecords = new AtomicLong();
    private final AtomicLong errorRecords = new AtomicLong();

    @Override
    public long incrementTotalRecords() {
        return totalRecords.incrementAndGet();
    }

    @Override
    public long incrementErrorRecords() {
        return errorRecords.incrementAndGet();
    }

    @Override
    public long getTotalRecords() {
        return totalRecords.get();
    }

    @Override
    public long getErrorRecords() {
        return errorRecords.get();
    }
}
