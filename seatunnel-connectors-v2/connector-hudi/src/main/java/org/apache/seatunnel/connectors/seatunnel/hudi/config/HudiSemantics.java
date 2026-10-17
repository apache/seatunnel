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

package org.apache.seatunnel.connectors.seatunnel.hudi.config;

/** The write semantics supported by the Hudi sink. */
public enum HudiSemantics {

    /**
     * Every flushed batch is committed to Hudi immediately. Data becomes visible as soon as the
     * batch is flushed, and a failed job can produce duplicated commits when the source replays
     * records, so this semantics is at-least-once.
     */
    AT_LEAST_ONCE,

    /**
     * Records are written into a Hudi instant while the job is running, but the instant is only
     * committed after the checkpoint that contains it completes. The write follows the two-phase
     * commit protocol of the engine, so a failed job never commits the data of a checkpoint that
     * was not completed, which gives exactly-once semantics. Data is visible only after a
     * checkpoint completes.
     */
    EXACTLY_ONCE
}
