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

package org.apache.seatunnel.engine.server.observability.cdc;

import org.apache.seatunnel.api.cdc.CdcProgressReport;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

/**
 * Engine task capability for collecting connector-owned CDC progress facts.
 *
 * <p>Implementations expose stable task and source-vertex identity for their full execution
 * attempt. Report collection must be non-blocking and thread-safe. The runtime adds attempt-local
 * ordering and removes reports when the owning pipeline is cleaned up.
 */
public interface CdcProgressReportSource<R extends CdcProgressReport> {

    /** Returns the runtime component that owns the report payload. */
    CdcProgressOwner getCdcProgressOwner();

    /** Returns the latest immutable report, or {@code null} when none is available. */
    R getCdcProgressReport();

    /** Returns the stable physical task identity for the current execution attempt. */
    TaskLocation getTaskLocation();

    /** Returns the logical source vertex represented by this report source. */
    long getCdcProgressSourceVertexId();

    /** Returns the next monotonically increasing sequence within this execution attempt. */
    long nextCdcProgressSequence();
}
