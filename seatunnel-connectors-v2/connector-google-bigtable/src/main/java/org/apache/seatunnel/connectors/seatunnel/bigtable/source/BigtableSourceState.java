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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class BigtableSourceState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Set<BigtableSourceSplit> assignedSplits;
    /**
     * Splits returned via {@code addSplitsBack()} but not yet reassigned. Added in #11144; absent
     * (null) when an older checkpoint payload is deserialized with the same {@link
     * #serialVersionUID}.
     */
    private final Set<BigtableSourceSplit> pendingSplits;

    public BigtableSourceState(
            Set<BigtableSourceSplit> assignedSplits, Set<BigtableSourceSplit> pendingSplits) {
        // Defensive copies: never alias the enumerator's live mutable sets into checkpoint state,
        // otherwise later mutations could corrupt an already-snapshotted state object.
        this.assignedSplits = copyOrEmpty(assignedSplits);
        this.pendingSplits = copyOrEmpty(pendingSplits);
    }

    public Set<BigtableSourceSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Set<BigtableSourceSplit> getPendingSplits() {
        // Null only when Java deserializes a pre-#11144 checkpoint that lacked this field.
        return pendingSplits == null ? Collections.emptySet() : pendingSplits;
    }

    private static Set<BigtableSourceSplit> copyOrEmpty(Set<BigtableSourceSplit> splits) {
        return splits == null ? new HashSet<>() : new HashSet<>(splits);
    }
}
