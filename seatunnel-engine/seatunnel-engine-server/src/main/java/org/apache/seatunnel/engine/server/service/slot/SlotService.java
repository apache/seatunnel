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

package org.apache.seatunnel.engine.server.service.slot;

import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

public interface SlotService {

    void init();

    /**
     * Rebuilds the slot service when the master asks a worker it has not registered to start over.
     *
     * <p>The service is rebuilt only while it has no assigned slots and no live slot contexts,
     * which gives it a new slot service sequence and a full set of unassigned slots. Otherwise it
     * is a no-op that logs a warning, so running tasks keep their slots and contexts. Slots kept
     * this way are not reconciled with the master; they are freed only by a normal {@link
     * #releaseSlot} call.
     */
    void reset();

    SlotAndWorkerProfile requestSlot(long jobID, ResourceProfile resourceProfile);

    SlotContext getSlotContext(SlotProfile slotProfile);

    void releaseSlot(long jobId, SlotProfile slotProfile);

    void close();

    WorkerProfile getWorkerProfile();
}
