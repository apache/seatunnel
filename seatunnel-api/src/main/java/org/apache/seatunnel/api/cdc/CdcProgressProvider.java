/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

/**
 * Non-blocking capability for reading the latest immutable connector-owned CDC progress.
 *
 * <p>Implementations must return an already-maintained snapshot. Collection can run concurrently
 * with reader or enumerator lifecycle callbacks, so this method must be thread-safe and must not
 * perform source, network, checkpoint, or other blocking I/O. Returning {@code null} means that no
 * report is currently available.
 */
@Experimental
public interface CdcProgressProvider<R extends CdcProgressReport> {

    /**
     * Returns the latest local report, or {@code null} when one is not available yet.
     *
     * @return an immutable connector-owned report, or {@code null}
     */
    R getCdcProgress();
}
