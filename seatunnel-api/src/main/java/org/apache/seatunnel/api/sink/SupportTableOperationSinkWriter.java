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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.table.operation.event.TableOperationEvent;

import java.io.IOException;

/**
 * Writer-side contract for applying {@link TableOperationEvent}s after in-flight rows are flushed.
 */
public interface SupportTableOperationSinkWriter {

    /**
     * Apply a table operation to the third-party receiver. Implementations must flush buffered rows
     * for the target table before executing a destructive operation such as truncate.
     *
     * @param event table operation from upstream
     * @throws IOException if the operation cannot be applied
     */
    void applyTableOperation(TableOperationEvent event) throws IOException;
}
