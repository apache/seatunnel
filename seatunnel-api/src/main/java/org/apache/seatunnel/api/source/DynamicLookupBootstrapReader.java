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

package org.apache.seatunnel.api.source;

/** Optional dimension-reader contract used only by engine-native dynamic lookup jobs. */
public interface DynamicLookupBootstrapReader {

    /** Ordered record inserted before a dimension barrier after all bootstrap rows are emitted. */
    enum Status {
        COMPLETE
    }

    /**
     * Returns true only after all initial snapshot rows have been emitted by this reader.
     *
     * <p>Invoked under the source checkpoint lock. Returning true allows a checkpoint to become the
     * durable fact-position anchor; false must never be interpreted as an empty dimension.
     */
    boolean isDynamicLookupBootstrapComplete();
}
