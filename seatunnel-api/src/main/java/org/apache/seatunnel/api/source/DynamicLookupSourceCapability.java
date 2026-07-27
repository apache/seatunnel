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

package org.apache.seatunnel.api.source;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * Optional source-level capability manifest consumed by the dynamic lookup planner.
 *
 * <p>Connectors implement this interface only after their reader and enumerator paths are certified
 * for the advertised capability. User configuration cannot grant a capability by name.
 */
public interface DynamicLookupSourceCapability extends Serializable {

    /** Fact source can close, snapshot, restore, and uniquely reactivate a source gate. */
    String FACT_SOURCE_GATE_V1 = "FACT_SOURCE_GATE_V1";

    /** Dimension source can finish an ordered bootstrap before fact input is activated. */
    String ORDERED_BOOTSTRAP_V1 = "ORDERED_BOOTSTRAP_V1";

    /** Dimension source rejects primary-key updates before emitting ambiguous changelog rows. */
    String PK_UPDATE_REJECT_V1 = "PK_UPDATE_REJECT_V1";

    /** Dimension source emits update pairs atomically within the same partitioned stream. */
    String ATOMIC_UPDATE_PAIR_V1 = "ATOMIC_UPDATE_PAIR_V1";

    /** Returns the connector-certified dynamic lookup capabilities. */
    default Set<String> dynamicLookupCapabilities() {
        return Collections.emptySet();
    }
}
