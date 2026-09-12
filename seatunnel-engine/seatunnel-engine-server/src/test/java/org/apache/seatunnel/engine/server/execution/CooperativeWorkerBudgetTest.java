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

package org.apache.seatunnel.engine.server.execution;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CooperativeWorkerBudgetTest {

    private static final long JOB_ID = 1L;
    private static final long OTHER_JOB_ID = 2L;

    @Test
    public void testAdmitsEveryPromotionWhenUnlimited() {
        CooperativeWorkerBudget budget =
                new CooperativeWorkerBudget(
                        CooperativeWorkerBudget.UNLIMITED, CooperativeWorkerBudget.UNLIMITED);

        for (int i = 0; i < 100; i++) {
            Assertions.assertTrue(budget.tryAcquire(JOB_ID));
        }

        Assertions.assertEquals(100, budget.getPromotedWorkers());
        Assertions.assertEquals(100, budget.getPromotedWorkers(JOB_ID));
        Assertions.assertEquals(100, budget.getTotalPromotions());
        Assertions.assertEquals(0, budget.getDeniedPromotions());
    }

    @Test
    public void testTreatsNegativeLimitAsUnlimited() {
        CooperativeWorkerBudget budget = new CooperativeWorkerBudget(-1, -5);

        Assertions.assertEquals(CooperativeWorkerBudget.UNLIMITED, budget.getMaxPromotedWorkers());
        Assertions.assertEquals(
                CooperativeWorkerBudget.UNLIMITED, budget.getMaxPromotedWorkersPerJob());
        Assertions.assertTrue(budget.tryAcquire(JOB_ID));
    }

    @Test
    public void testDeniesPromotionWhenGlobalLimitIsReached() {
        CooperativeWorkerBudget budget =
                new CooperativeWorkerBudget(2, CooperativeWorkerBudget.UNLIMITED);

        Assertions.assertTrue(budget.tryAcquire(JOB_ID));
        Assertions.assertTrue(budget.tryAcquire(OTHER_JOB_ID));
        Assertions.assertFalse(budget.tryAcquire(JOB_ID));

        Assertions.assertEquals(2, budget.getPromotedWorkers());
        Assertions.assertEquals(2, budget.getTotalPromotions());
        Assertions.assertEquals(1, budget.getDeniedPromotions());
    }

    @Test
    public void testDeniesPromotionWhenJobLimitIsReachedWithoutHoldingGlobalBudget() {
        CooperativeWorkerBudget budget = new CooperativeWorkerBudget(10, 1);

        Assertions.assertTrue(budget.tryAcquire(JOB_ID));
        Assertions.assertFalse(budget.tryAcquire(JOB_ID));

        // The denied promotion must not keep the global budget it reserved first.
        Assertions.assertEquals(1, budget.getPromotedWorkers());
        Assertions.assertEquals(1, budget.getPromotedWorkers(JOB_ID));

        // A different job is still admitted while one job is at its own limit.
        Assertions.assertTrue(budget.tryAcquire(OTHER_JOB_ID));
        Assertions.assertEquals(2, budget.getPromotedWorkers());
    }

    @Test
    public void testReleasedBudgetIsReusable() {
        CooperativeWorkerBudget budget = new CooperativeWorkerBudget(1, 1);

        Assertions.assertTrue(budget.tryAcquire(JOB_ID));
        Assertions.assertFalse(budget.tryAcquire(JOB_ID));

        budget.release(JOB_ID);

        Assertions.assertEquals(0, budget.getPromotedWorkers());
        Assertions.assertEquals(0, budget.getPromotedWorkers(JOB_ID));
        Assertions.assertTrue(budget.tryAcquire(JOB_ID));
    }

    @Test
    public void testReleaseOfUnknownJobKeepsCountersNonNegative() {
        CooperativeWorkerBudget budget = new CooperativeWorkerBudget(1, 1);

        budget.release(JOB_ID);

        Assertions.assertEquals(0, budget.getPromotedWorkers());
        Assertions.assertEquals(0, budget.getPromotedWorkers(JOB_ID));
    }

    @Test
    public void testConcurrentPromotionsNeverExceedTheLimit() throws InterruptedException {
        int limit = 4;
        int threads = 32;
        CooperativeWorkerBudget budget = new CooperativeWorkerBudget(limit, limit);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger admitted = new AtomicInteger();
        List<Thread> workers = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            Thread thread =
                    new Thread(
                            () -> {
                                try {
                                    start.await();
                                    if (budget.tryAcquire(JOB_ID)) {
                                        admitted.incrementAndGet();
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                } finally {
                                    done.countDown();
                                }
                            });
            workers.add(thread);
            thread.start();
        }

        start.countDown();
        Assertions.assertTrue(done.await(30, TimeUnit.SECONDS));
        for (Thread worker : workers) {
            worker.join();
        }

        Assertions.assertEquals(limit, admitted.get());
        Assertions.assertEquals(limit, budget.getPromotedWorkers());
        Assertions.assertEquals(threads - limit, budget.getDeniedPromotions());
    }
}
