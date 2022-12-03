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

public interface Metric {

    /**
     * Returns the name of the associated metric.
     */

    String name();

    /**
     * Return the measurement unit for the associated metric. Meant
     * to provide further information on the type of value measured
     * by the user-defined metric. Doesn't affect the functionality of the
     * metric, it still remains a simple numeric value, but is used to
     * populate the {@link MetricTags#UNIT} tag in the metric's description.
     */

    Unit unit();

    /**
     * Increments the current value by 1.
     */
    void increment();

    /**
     * Increments the current value by the specified amount.
     */
    void increment(long amount);

    /**
     * Decrements the current value by 1.
     */
    void decrement();

    /**
     * Decrements the current value by the specified amount.
     */
    void decrement(long amount);

    /**
     * Sets the current value.
     */
    void set(long newValue);

}
