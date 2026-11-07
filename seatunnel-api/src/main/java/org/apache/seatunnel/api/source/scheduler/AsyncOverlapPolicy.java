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

package org.apache.seatunnel.api.source.scheduler;

import org.apache.seatunnel.api.annotation.Experimental;

/** Behavior when work with the same {@link AsyncTaskKey} is already running. */
@Experimental
public enum AsyncOverlapPolicy {
    /** Retain at most one follow-up execution after the current execution finishes. */
    COALESCE_ONE,

    /** Drop the overlapping request. */
    SKIP,

    /** Fail the source because the connector violated its no-overlap contract. */
    FAIL
}
