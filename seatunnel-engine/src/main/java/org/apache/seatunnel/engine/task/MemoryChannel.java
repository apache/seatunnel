/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.task;

import org.apache.seatunnel.engine.api.type.Row;
import org.apache.seatunnel.engine.api.type.TerminateRow;

import java.util.concurrent.ArrayBlockingQueue;

public class MemoryChannel implements Channel{
    private final int channelCapacity = 50000;

    private ArrayBlockingQueue<Row> queue = new ArrayBlockingQueue<Row>(channelCapacity);

    @Override
    public Row pull() {
        try {
            Row row = queue.take();
            return row instanceof TerminateRow ? null : row;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void push(Row row) {
        try {
            queue.put(row);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void flush() {

    }

    @Override
    public boolean isEmpty() {
        return queue.size() == 0;
    }
}
