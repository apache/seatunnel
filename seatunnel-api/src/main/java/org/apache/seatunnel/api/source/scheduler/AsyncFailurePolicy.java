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

/** Failure behavior for engine-managed coordinator async work. */
@Experimental
public enum AsyncFailurePolicy {
    /** Deliver the failure to the result handler; an unhandled failure fails the Source. */
    FAIL_SOURCE,

    /** Deliver the failure and allow the connector to schedule a bounded retry explicitly. */
    HANDLE_IN_COORDINATOR
}
