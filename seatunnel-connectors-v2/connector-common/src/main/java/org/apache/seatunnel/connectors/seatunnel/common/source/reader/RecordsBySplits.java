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

package org.apache.seatunnel.connectors.seatunnel.common.source.reader;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class RecordsBySplits<E> implements RecordsWithSplitIds<E> {

    private final Set<String> finishedSplits;
    private final Iterator<Map.Entry<String, Collection<E>>> splitsIterator;
    private Iterator<E> recordsInCurrentSplit;

    public RecordsBySplits(Map<String, Collection<E>> recordsBySplit, Set<String> finishedSplits) {
        this.splitsIterator = checkNotNull(recordsBySplit, "recordsBySplit").entrySet().iterator();
        this.finishedSplits = checkNotNull(finishedSplits, "finishedSplits");
    }

    @Override
    public String nextSplit() {
        if (splitsIterator.hasNext()) {
            Map.Entry<String, Collection<E>> next = splitsIterator.next();
            recordsInCurrentSplit = next.getValue().iterator();
            return next.getKey();
        } else {
            return null;
        }
    }

    @Override
    public E nextRecordFromSplit() {
        if (recordsInCurrentSplit == null) {
            throw new IllegalStateException();
        }
        return recordsInCurrentSplit.hasNext() ? recordsInCurrentSplit.next() : null;
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }
}
