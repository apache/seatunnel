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

import java.io.Serializable;

/** Cumulative output counters. A null field is intentionally omitted from JSON. */
public final class LineageOutputStatistics implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Long rowCount;
    private final Long size;
    private final String semantics;

    /** Creates cumulative output statistics with an engine-specific counting semantic. */
    public LineageOutputStatistics(Long rowCount, Long size, String semantics) {
        this.rowCount = rowCount;
        this.size = size;
        this.semantics = semantics;
    }

    /** Returns the cumulative output row count, if available. */
    public Long rowCount() {
        return rowCount;
    }

    /** Returns the cumulative output size in bytes, if available. */
    public Long size() {
        return size;
    }

    /** Returns the counting semantic, such as {@code committed} or {@code attempted}. */
    public String semantics() {
        return semantics;
    }

    /** Returns whether at least one numeric statistic is present. */
    public boolean hasValue() {
        return rowCount != null || size != null;
    }
}
