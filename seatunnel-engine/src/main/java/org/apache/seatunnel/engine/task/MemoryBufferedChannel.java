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
import org.apache.seatunnel.engine.executionplan.JobInformation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryBufferedChannel implements Channel {

    private ArrayBlockingQueue<Row> queue = null;

    private List<Row> pullBuffer;
    private List<Row> pushBuffer;

    private int pullBufferIndex = 0;
    private int pullBufferSize = 0;

    private int pushBufferIndex = 0;
    private int pushBufferSize = 0;

    private int channelPullSize = 0;

    private ReentrantLock lock;

    private final Condition notInsufficient;
    private final Condition notEmpty;
    private final long sleepTime = 200L;

    public MemoryBufferedChannel(JobInformation jobInformation) {
        final int capacity = 50000;
        this.queue = new ArrayBlockingQueue<Row>(capacity);
        final int bufferSize = 1000;
        this.pullBufferSize = this.pushBufferSize = this.channelPullSize = bufferSize;
        this.pullBuffer = new ArrayList<Row>(bufferSize);
        this.pushBuffer = new ArrayList<Row>(bufferSize);
        lock = new ReentrantLock();
        notInsufficient = lock.newCondition();
        notEmpty = lock.newCondition();
    }

    @Override
    public void push(Row row) {

        boolean isFull = this.pushBufferIndex >= this.pushBufferSize;
        if (isFull) {
            flush();
        }

        this.pushBuffer.add(row);
        this.pushBufferIndex++;

    }

    @Override
    public void flush() {
        this.pushAll(this.pushBuffer);
        this.pushBuffer.clear();
        this.pushBufferIndex = 0;
    }

    public void pushAll(Collection<Row> rs) {
        try {
            lock.lockInterruptibly();
            while (rs.size() > this.queue.remainingCapacity()) {
                notInsufficient.await(sleepTime, TimeUnit.MILLISECONDS);
            }
            this.queue.addAll(rs);
            notEmpty.signalAll();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Row pull() {
        boolean isEmpty = this.pullBufferIndex >= this.pullBuffer.size();
        if (isEmpty) {
            receive();
        }

        Row row = this.pullBuffer.get(this.pullBufferIndex++);
        return row instanceof TerminateRow ? null : row;
    }

    private void receive() {
        this.pullAll(this.pullBuffer);
        this.pullBufferIndex = 0;
        this.pullBufferSize = this.pullBuffer.size();
    }

    public void pullAll(Collection<Row> rs) {
        assert rs != null;
        rs.clear();
        try {
            lock.lockInterruptibly();
            //Wait when there are no elements
            while (this.queue.drainTo(rs, channelPullSize) <= 0) {
                notEmpty.await(sleepTime, TimeUnit.MILLISECONDS);
            }
            notInsufficient.signalAll();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        return queue.size() == 0;
    }
}
