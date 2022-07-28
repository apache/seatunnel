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

package execution;

import static org.junit.Assert.assertTrue;

import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.TaskExecutionService;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.logging.ILogger;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;


public class TaskExecutionServiceTest {

    HazelcastInstanceImpl instance = ((HazelcastInstanceProxy) HazelcastInstanceFactory.newHazelcastInstance(new Config(), Thread.currentThread().getName(), new SeaTunnelNodeContext())).getOriginal();
    SeaTunnelServer service = instance.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
    ILogger logger = instance.node.nodeEngine.getLogger(TaskExecutionServiceTest.class);


    @Test
    public void testAll() throws InterruptedException {
        testCancel();
        testFinish();
    }

    public void testCancel() throws InterruptedException {
        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        AtomicBoolean stop = new AtomicBoolean(false);
        TestTask testTask = new TestTask(stop, logger);

        TaskExecutionContext taskExecutionContext = taskExecutionService.submitTask(testTask);

        Thread.sleep(3000);

        taskExecutionContext.cancel();

        Thread.sleep(30000);
        assertTrue(taskExecutionContext.executionFuture.isCompletedExceptionally());
    }

    public void testFinish() throws InterruptedException {
        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicBoolean futureMark = new AtomicBoolean(false);
        TestTask testTasklet = new TestTask(stop, logger);

        TaskExecutionContext taskExecutionContext = taskExecutionService.submitTask(testTasklet);
        taskExecutionContext.executionFuture.whenComplete(new BiConsumer<Void, Throwable>() {
            @Override
            public void accept(Void unused, Throwable throwable) {
                futureMark.set(true);
            }
        });

        Thread.sleep(3000);

        stop.set(true);

        Thread.sleep(1000);

        assertTrue(taskExecutionContext.executionFuture.isDone());
        assertTrue(futureMark.get());
    }
}