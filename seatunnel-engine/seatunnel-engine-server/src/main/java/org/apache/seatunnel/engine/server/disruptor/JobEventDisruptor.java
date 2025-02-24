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

package org.apache.seatunnel.engine.server.disruptor;

import org.apache.seatunnel.api.event.Event;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import lombok.Getter;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;

public class JobEventDisruptor implements Closeable {

    private volatile Disruptor<JobEvent> disruptor;

    private boolean isClosed = false;

    @Getter private RingBuffer<JobEvent> ringBuffer;

    private final int eventQueueSize;

    public JobEventDisruptor(int eventQueueSize) {
        this.eventQueueSize = findNextPowerOfTwo(eventQueueSize);
        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        this.disruptor =
                new Disruptor<>(
                        JobEvent.FACTORY,
                        this.eventQueueSize,
                        threadFactory,
                        ProducerType.SINGLE,
                        new BlockingWaitStrategy());

        disruptor.start();
        this.ringBuffer = disruptor.getRingBuffer();
    }

    private int findNextPowerOfTwo(int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    public boolean publish(Event event) {
        if (isClosed()) {
            return false;
        }
        long sequence = ringBuffer.next();
        try {
            JobEvent jobEvent = ringBuffer.get(sequence);
            jobEvent.setEvent(event);
        } finally {
            ringBuffer.publish(sequence);
        }
        return true;
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
        disruptor.shutdown();
    }

    public ArrayBlockingQueue<Event> storeJobHistory() {
        ArrayBlockingQueue<Event> events = new ArrayBlockingQueue<>(eventQueueSize);
        long nextSequence = 0;
        while (ringBuffer.getCursor() >= nextSequence) {
            JobEvent event = ringBuffer.get(nextSequence);
            events.add(event.getEvent());
            nextSequence++;
        }
        try {
            this.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return events;
    }
}
