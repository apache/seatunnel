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

package org.apache.seatunnel.api.source.managed;

import org.apache.seatunnel.api.annotation.Experimental;

/** Result of one cooperative, engine-budgeted source reader poll turn. */
@Experimental
public enum PollStatus {
    /** More records are immediately available and the engine may schedule another poll turn. */
    MORE_AVAILABLE,

    /** No record is currently available; the engine waits for the availability signal. */
    NOTHING_AVAILABLE,

    /** The bounded input and all assigned splits have been consumed. */
    END_OF_INPUT
}
