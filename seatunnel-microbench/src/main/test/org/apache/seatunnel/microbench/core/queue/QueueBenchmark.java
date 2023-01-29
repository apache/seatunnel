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
            if (queue.poll() != null) {
                result = queue.poll();
            }
        }
        return result;
    }

    @Benchmark
    @Threads(2)
    public boolean concurrentOffer() {
        return queue.offer(1);
    }

}

/*
   Environment:

   # JMH version: 1.29
    # VM version: JDK 1.8.0_345, OpenJDK 64-Bit Server VM, 25.345-b01
    # VM invoker: C:\Users\lenovo\.jdks\temurin-1.8.0_345\jre\bin\java.exe
    # VM options: -javaagent:C:\Program Files\JetBrains\IntelliJ IDEA 2021.1\lib\idea_rt.jar=22892:C:\Program Files\JetBrains\IntelliJ IDEA 2021.1\bin -Dfile.encoding=UTF-8
    # Blackhole mode: full + dont-inline hint
    # Warmup: 5 iterations, 5 s each
    # Measurement: 5 iterations, 5 s each
    # Timeout: 10 min per iteration
    # Threads: 1 thread, will synchronize iterations
    # Benchmark mode: Throughput, ops/time
    # Benchmark: org.apache.seatunnel.microbench.core.queue.QueueBenchmark.offerAndPoll
    # Parameters: (burstSize = 100000, qCapacity = 10000, queueType = ArrayBlockingQueue)

    Benchmark                    (burstSize)  (qCapacity)            (queueType)   Mode  Cnt     Score    Error  Units
    QueueBenchmark.offerAndPoll       100000        10000     ArrayBlockingQueue  thrpt   10   271.481 ±  8.851  ops/s
    QueueBenchmark.offerAndPoll       100000        10000    LinkedBlockingQueue  thrpt   10  1123.445 ± 35.607  ops/s
    QueueBenchmark.offerAndPoll       100000        10000  ConcurrentLinkedQueue  thrpt   10   311.702 ±  8.535  ops/s
 */
