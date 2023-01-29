/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.seatunnel.microbench.core.queue;

import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.CompilerControl.Mode;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueFactory {
    public enum QueueType {
        ArrayBlockingQueue,
        LinkedBlockingQueue,
        ConcurrentLinkedQueue
    }

    static Queue<Integer> build(QueueType type, int capacity) {
        switch (type) {
            case ArrayBlockingQueue:
                return new ArrayBlockingQueueAdapter<>(capacity);
            case LinkedBlockingQueue:
                return new LinkedBlockingQueueAdapter<>(capacity);
            case ConcurrentLinkedQueue:
                return new ConcurrentLinkedQueueAdapter<>();
            default:
                throw new RuntimeException("unexpected queue type");
        }
    }

    static class ArrayBlockingQueueAdapter<E> extends ArrayBlockingQueue<E> implements Queue<E> {

        public ArrayBlockingQueueAdapter(int capacity) {
            super(capacity);
        }

        @Override
        @CompilerControl(Mode.INLINE)
        public boolean offer(E e) {
            return super.offer(e);
        }

        @Override
        @CompilerControl(Mode.INLINE)
        public E poll() {
            return super.poll();
        }
    }

    static class LinkedBlockingQueueAdapter<E> extends LinkedBlockingQueue<E> implements Queue<E> {

        public LinkedBlockingQueueAdapter(int capacity) {
            super(capacity);
        }

        @Override
        @CompilerControl(Mode.INLINE)
        public boolean offer(E e) {
            return super.offer(e);
        }

        @Override
        @CompilerControl(Mode.INLINE)
        public E poll() {
            return super.poll();
        }
    }

    static class ConcurrentLinkedQueueAdapter<E> extends ConcurrentLinkedQueue<E> implements Queue<E> {

        @Override
        @CompilerControl(Mode.INLINE)
        public boolean offer(E e) {
            return super.offer(e);
        }

        @Override
        @CompilerControl(Mode.INLINE)
        public E poll() {
            return super.poll();
        }
    }
}
