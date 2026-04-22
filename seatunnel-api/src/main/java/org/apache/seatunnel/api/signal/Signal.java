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

package org.apache.seatunnel.api.signal;

/**
 * Interface for control-plane signals that the engine propagates through the data flow.
 *
 * <p>A {@code Signal} is not a data record: it carries no user payload and is intended to instruct
 * an operator (e.g. a {@link org.apache.seatunnel.api.sink.SinkWriter}) to perform a side-effecting
 * action such as flushing buffered data.
 *
 * <p>Every signal carries three pieces of metadata:
 *
 * <ul>
 *   <li>{@link #getJobId()} — the job that produced the signal.
 *   <li>{@link #getTaskId()} — the source task that produced the signal, useful for logging and
 *       per-subtask diagnostics.
 *   <li>{@link #getCreatedTime()} — the wall-clock time (epoch millis) at which the signal was
 *       created by the engine.
 * </ul>
 *
 * <p>Concrete signals should be small and immutable. New signal types are added by introducing new
 * implementations of this interface; the engine routes them by type via {@code instanceof}.
 */
public interface Signal {

    /** @return the id of the job that created this signal. */
    long getJobId();

    /** @return the id of the task that created this signal. */
    long getTaskId();

    /** @return the wall-clock creation time of this signal, in epoch milliseconds. */
    long getCreatedTime();
}
