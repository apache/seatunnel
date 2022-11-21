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

package org.apache.seatunnel.connectors.cdc.base.source.reader.external;

import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import java.util.Iterator;

/**
 * Fetcher to fetch data of a table split, the split is either snapshot split {@link SnapshotSplit}
 * or incremental split {@link IncrementalSplit}.
 */
public interface Fetcher<T, Split> {

    /** Add to task to fetch, this should call only when the reader is idle. */
    void submitTask(FetchTask<Split> fetchTask);

    /**
     * Fetched records from data source. The method should return null when reaching the end of the
     * split, the empty {@link Iterator} will be returned if the data of split is on pulling.
     */

    Iterator<T> pollSplitRecords() throws InterruptedException;

    /** Return the current fetch task is finished or not. */
    boolean isFinished();

    /** Close the client and releases all resources. */
    void close();
}
