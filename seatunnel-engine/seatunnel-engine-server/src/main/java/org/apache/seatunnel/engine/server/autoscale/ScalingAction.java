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

public enum ScalingAction {
    /** Indicates that the autoscaler recommends adding workers. */
    SCALE_OUT,

    /** Indicates that scale-in conditions are met and the recommendation awaits stabilization. */
    SCALE_IN_CANDIDATE,

    /** Indicates that scale-in is blocked because required metrics are incomplete or invalid. */
    SCALE_IN_BLOCKED,

    /** Indicates that the current worker count should remain unchanged. */
    NO_ACTION
}
