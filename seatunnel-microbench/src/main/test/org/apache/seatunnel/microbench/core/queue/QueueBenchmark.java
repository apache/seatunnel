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

import org.apache.seatunnel.microbench.base.AbstractMicrobenchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

@State(Scope.Thread)
public class QueueBenchmark extends AbstractMicrobenchmark {

    private Queue<Integer> queue;

    @Param
    QueueFactory.QueueType queueType;
    @Param(value = "10000")
    int qCapacity;
    @Param(value = "100000")
    int burstSize;

    @Setup
    public void buildMeCounterHearty() {
        queue = QueueFactory.build(queueType, qCapacity);
    }

    @Benchmark
    public int offerAndPoll() {
        final int fixedBurstSize = burstSize;
        for (int i = 0; i < fixedBurstSize; i++) {
            queue.offer(1);
        }
        int result = 0;
        for (int i = 0; i < fixedBurstSize; i++) {
            result = queue.poll();
        }
        return result;
    }

    @Benchmark
    @Threads(2)
    public boolean concurrentOffer() {
        return queue.offer(1);
    }

}
