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

package org.apache.seatunnel.engine.server.task.record;

/**
 * barrier flowing in data flow
 */
public interface Barrier {
    Long PREPARE_CLOSE_BARRIER_ID = Long.MAX_VALUE;

    /**
     * The ID of the barrier.
     */
    long getId();

    /**
     * Whether the task needs to perform a status snapshot after the barrier is aligned.
     * For example, DDL barrier does not require a snapshot.
     */
    boolean snapshot();

    /**
     * Barrier indicating that the task should prepare to close.
     */
    boolean prepareClose();

}
