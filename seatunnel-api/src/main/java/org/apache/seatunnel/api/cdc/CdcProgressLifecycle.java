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

/**
 * Current lifecycle stage observed and reported by a CDC reader.
 *
 * <p>The stage is derived from the split the reader is processing, so the same values apply to
 * every startup mode. {@code CATCH_UP} only occurs after a snapshot phase; incremental-only startup
 * modes go from {@link #UNKNOWN} directly to {@link #INCREMENTAL}.
 */
@Experimental
public enum CdcProgressLifecycle {
    /** The reader is reading snapshot splits. */
    SNAPSHOT,

    /** Incremental consumption is catching up with snapshot high watermarks. */
    CATCH_UP,

    /** The reader is consuming only the incremental change stream. */
    INCREMENTAL,

    /**
     * The reader has finished every split it will receive and signalled that no more elements
     * follow. Every reader of a bounded job reaches this stage; in an unbounded job, a reader that
     * holds no change-stream split reaches it once the enumerator has nothing more to assign.
     */
    COMPLETED,

    /** The lifecycle is not known yet. */
    UNKNOWN
}
