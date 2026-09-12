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

import org.apache.seatunnel.api.table.schema.SchemaChangeBehavior;

/**
 * Source-side contract for selecting how schema change events are handled after they are emitted by
 * the source.
 *
 * <p>The runtime resolves this behavior for each source before validating or forwarding schema
 * change events. Sources that do not implement this interface are treated as {@link
 * SchemaChangeBehavior#EVOLVE}.
 */
public interface SupportSchemaChangeBehavior {

    /**
     * Returns the configured schema change behavior for this source.
     *
     * @return behavior used to validate, forward, or ignore emitted schema change events
     */
    SchemaChangeBehavior getSchemaChangeBehavior();
}
