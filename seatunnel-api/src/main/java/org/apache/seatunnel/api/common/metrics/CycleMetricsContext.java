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

package org.apache.seatunnel.api.common.metrics;

/** Plan to restore snapshot indicators */
public class CycleMetricsContext extends AbstractMetricsContext {

    public CycleMetricsContext() {}

    public CycleMetricsContext(MetricsContext metricsMap) {
        if (metricsMap instanceof AbstractMetricsContext) {
            this.metrics.putAll(((AbstractMetricsContext) metricsMap).getMetrics());
        }
    }

    /**
     * Clears the contents of this metricsContext.
     *
     * <p>This method removes all stored data, resetting the object to its initial state.
     */
    public void clear() {
        metrics.values().forEach(Metric::clear);
    }
}
