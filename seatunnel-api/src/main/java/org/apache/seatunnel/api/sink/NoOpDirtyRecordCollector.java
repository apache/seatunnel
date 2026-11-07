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

import org.apache.seatunnel.api.table.catalog.CatalogTable;

/**
 * A no-operation dirty record collector that does nothing. This is used as a default implementation
 * when no dirty data collection is configured.
 */
public class NoOpDirtyRecordCollector implements DirtyRecordCollector {

    private static final long serialVersionUID = 1L;

    public static final NoOpDirtyRecordCollector INSTANCE = new NoOpDirtyRecordCollector();

    private NoOpDirtyRecordCollector() {}

    @Override
    public void collect(
            int subTaskIndex,
            Object dirtyRecord,
            Throwable exception,
            String errorMessage,
            CatalogTable catalogTable) {}

    private Object readResolve() {
        return INSTANCE;
    }
}
