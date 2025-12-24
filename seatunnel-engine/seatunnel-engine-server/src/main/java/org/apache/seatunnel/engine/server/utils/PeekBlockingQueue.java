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

package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.common.utils.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * PeekBlockingQueue implements blocking when peeking. Queues like BlockingQueue only support
 * blocking when take() is called. The original solution used sleep(2000) to check whether there was
 * data in the pending queue. This solution still had performance drawbacks, so it was changed to
 * use peek blocking, which allows tasks to be scheduled more efficiently.
 *
 * <p>Application scenario: In CoordinatorService, the following process needs to be executed: <br>
 * 1. Peek data from the queue. <br>
 * 2. Check if resources are sufficient. <br>
 * 3. If resources are sufficient, take() the data; otherwise, do not take data from the queue.
 */
@Slf4j
public class PeekBlockingQueue<E> {

    private final BlockingQueue<E> queue = new LinkedBlockingQueue<>();
    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    private final Map<Long, E> jobIdMap = new ConcurrentHashMap<>();
    private final Function<E, Long> idExtractor;

    public PeekBlockingQueue(Function<E, Long> idExtractor) {
        this.idExtractor = idExtractor;
    }

    public void put(E element) {
        lock.lock();
        try {
            queue.put(element);
            Long jobId = idExtractor.apply(element);
            jobIdMap.put(jobId, element);
            notEmpty.signalAll();
        } catch (InterruptedException e) {
            log.error("Put element into queue failed. {}", ExceptionUtils.getMessage(e));
        } finally {
            lock.unlock();
        }
    }

    public E take() throws InterruptedException {
        E element = queue.take();
        Long jobId = idExtractor.apply(element);
        jobIdMap.remove(jobId);
        return element;
    }

    public E peekBlocking() throws InterruptedException {
        lock.lock();
        try {
            while (queue.peek() == null) {
                notEmpty.await();
            }
            return queue.peek();
        } finally {
            lock.unlock();
        }
    }

    public Integer size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        lock.lock();
        try {
            queue.clear();
            jobIdMap.clear();
        } finally {
            lock.unlock();
        }
    }

    public E getById(Long jobId) {
        return jobIdMap.get(jobId);
    }

    public boolean removeById(Long jobId) {
        lock.lock();
        try {
            E element = jobIdMap.remove(jobId);
            if (element != null) {
                return queue.remove(element);
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public boolean contains(Long jobId) {
        return jobIdMap.containsKey(jobId);
    }

    public Map<Long, E> getJobIdMap() {
        return jobIdMap;
    }
}
