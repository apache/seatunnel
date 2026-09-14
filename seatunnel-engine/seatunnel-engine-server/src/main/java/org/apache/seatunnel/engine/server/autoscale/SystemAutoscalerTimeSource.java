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

package org.apache.seatunnel.engine.server.autoscale;

import java.util.concurrent.TimeUnit;

public final class SystemAutoscalerTimeSource implements AutoscalerTimeSource {

    /**
     * Returns wall-clock time for timestamps that represent real-world instants and may be compared
     * by other components.
     */
    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Returns monotonic elapsed-time ticks for local duration checks; it is not a calendar time and
     * is unaffected by wall-clock adjustments.
     */
    @Override
    public long monotonicTimeMillis() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }
}
