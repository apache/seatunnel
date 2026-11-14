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

package org.apache.seatunnel.api.cdc;

import org.apache.seatunnel.api.annotation.Experimental;

/** Describes the availability and precision of one CDC progress value. */
@Experimental
public enum CdcProgressAccuracy {
    /** The value comes directly from connector runtime state without approximation. */
    EXACT,

    /** The value is useful for diagnostics, but the connector cannot guarantee exact precision. */
    BEST_EFFORT,

    /** The connector or current implementation cannot provide this value. */
    UNSUPPORTED,

    /** The value is supported, but is not available at the time of observation. */
    UNAVAILABLE
}
