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

package org.apache.seatunnel.api.tracing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MDCTracerTest {

    @Test
    public void testMDCTracedRunnable() {
        MDCContext mdcContext = MDCContext.of(1, 2, 3);
        Runnable tracedRunnable =
                MDCTracer.tracing(
                        mdcContext,
                        new Runnable() {
                            @Override
                            public void run() {
                                Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                            }
                        });

        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));

        tracedRunnable.run();

        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));
    }

    @Test
    public void testMDCTracedCallable() throws Exception {
        MDCContext mdcContext = MDCContext.of(1, 2, 3);

        Callable<Void> tracedCallable =
                MDCTracer.tracing(
                        mdcContext,
                        new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                                return null;
                            }
                        });

        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));

        tracedCallable.call();

        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));
    }

    @Test
    public void testMDCTracedSupplier() throws Exception {
        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));

        try (MDCContext ignored = MDCContext.of(1, 2, 3).activate()) {
            Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
            Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
            Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));

            CompletableFuture.supplyAsync(
                            MDCTracer.tracing(
                                    new Supplier<Object>() {
                                        @Override
                                        public Object get() {
                                            Assertions.assertEquals(
                                                    "1", MDC.get(MDCContext.JOB_ID));
                                            Assertions.assertEquals(
                                                    "2", MDC.get(MDCContext.PIPELINE_ID));
                                            Assertions.assertEquals(
                                                    "3", MDC.get(MDCContext.TASK_ID));
                                            return null;
                                        }
                                    }))
                    .get();

            Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
            Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
            Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
        }

        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));
    }

    @Test
    public void testMDCTracedExecutorService() throws Exception {
        MDCContext mdcContext = MDCContext.of(1, 2, 3);

        MDCExecutorService tracedExecutorService =
                MDCTracer.tracing(mdcContext, Executors.newSingleThreadExecutor());

        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));
        tracedExecutorService
                .submit(
                        new Runnable() {
                            @Override
                            public void run() {
                                Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                            }
                        })
                .get();
        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));

        tracedExecutorService
                .submit(
                        new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                                return null;
                            }
                        })
                .get();
        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));

        MDCScheduledExecutorService tracedScheduledExecutorService =
                MDCTracer.tracing(mdcContext, Executors.newSingleThreadScheduledExecutor());
        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));

        tracedScheduledExecutorService
                .schedule(
                        new Runnable() {
                            @Override
                            public void run() {
                                Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                            }
                        },
                        1,
                        TimeUnit.SECONDS)
                .get();
        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));

        tracedScheduledExecutorService
                .schedule(
                        new Callable<Object>() {
                            @Override
                            public Object call() {
                                Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                                return null;
                            }
                        },
                        1,
                        TimeUnit.SECONDS)
                .get();
        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));

        CompletableFuture<Boolean> futureWithScheduleAtFixedRate = new CompletableFuture<>();
        tracedScheduledExecutorService.scheduleAtFixedRate(
                new Runnable() {
                    AtomicInteger executeCount = new AtomicInteger(0);

                    @Override
                    public void run() {
                        Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                        Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                        Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                        executeCount.incrementAndGet();
                        if (executeCount.get() > 10 && !futureWithScheduleAtFixedRate.isDone()) {
                            futureWithScheduleAtFixedRate.complete(true);
                        }
                    }
                },
                0,
                10,
                TimeUnit.MILLISECONDS);
        futureWithScheduleAtFixedRate.join();

        CompletableFuture<Boolean> futureWithScheduleAtFixedDelay = new CompletableFuture<>();
        tracedScheduledExecutorService.scheduleWithFixedDelay(
                new Runnable() {
                    AtomicInteger executeCount = new AtomicInteger(0);

                    @Override
                    public void run() {
                        Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                        Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                        Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                        executeCount.incrementAndGet();
                        if (executeCount.get() > 10 && !futureWithScheduleAtFixedDelay.isDone()) {
                            futureWithScheduleAtFixedDelay.complete(true);
                        }
                    }
                },
                0,
                10,
                TimeUnit.MILLISECONDS);
        futureWithScheduleAtFixedDelay.join();

        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));
    }

    @Test
    public void testMDCTracedStream() throws Exception {
        MDCContext mdcContext = MDCContext.of(1, 2, 3);

        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));
        MDCTracer.tracing(
                        mdcContext,
                        IntStream.range(1, 100)
                                .boxed()
                                .collect(Collectors.toList())
                                .parallelStream())
                .filter(
                        integer -> {
                            Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                            Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                            Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                            return true;
                        })
                .map(
                        integer -> {
                            Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                            Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                            Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                            return integer;
                        })
                .sorted(
                        (o1, o2) -> {
                            Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                            Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                            Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                            return Integer.compare(o1, o2);
                        })
                .forEach(
                        integer -> {
                            Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                            Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                            Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                        });
        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));

        try (MDCContext ignored = MDCContext.of(1, 2, 3).activate()) {
            Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
            Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
            Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));

            MDCTracer.tracing(
                            IntStream.range(1, 100)
                                    .boxed()
                                    .collect(Collectors.toList())
                                    .parallelStream())
                    .filter(
                            integer -> {
                                Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                                return true;
                            })
                    .map(
                            integer -> {
                                Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                                return integer;
                            })
                    .sorted(
                            (o1, o2) -> {
                                Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                                return Integer.compare(o1, o2);
                            })
                    .forEach(
                            integer -> {
                                Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
                            });

            Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
            Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
            Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
        }

        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));

        try (MDCContext ignored = MDCContext.of(1, 2, 3).activate()) {
            Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
            Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
            Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));

            mdcContext = MDCContext.of(4, 5, 6);
            MDCTracer.tracing(
                            mdcContext,
                            IntStream.range(1, 100)
                                    .boxed()
                                    .collect(Collectors.toList())
                                    .parallelStream())
                    .filter(
                            integer -> {
                                Assertions.assertEquals("4", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("5", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("6", MDC.get(MDCContext.TASK_ID));
                                return true;
                            })
                    .map(
                            integer -> {
                                Assertions.assertEquals("4", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("5", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("6", MDC.get(MDCContext.TASK_ID));
                                return integer;
                            })
                    .sorted(
                            (o1, o2) -> {
                                Assertions.assertEquals("4", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("5", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("6", MDC.get(MDCContext.TASK_ID));
                                return Integer.compare(o1, o2);
                            })
                    .forEach(
                            integer -> {
                                Assertions.assertEquals("4", MDC.get(MDCContext.JOB_ID));
                                Assertions.assertEquals("5", MDC.get(MDCContext.PIPELINE_ID));
                                Assertions.assertEquals("6", MDC.get(MDCContext.TASK_ID));
                            });

            Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
            Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
            Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));
        }

        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));
    }

    @Test
    public void testMDCContext() throws Exception {
        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));

        MDCContext mdcContext = MDCContext.of(1, 2, 3);
        try (MDCContext ignored = mdcContext.activate()) {
            Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
            Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
            Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));

            MDCContext currentMDCCOntext = MDCContext.current();
            Assertions.assertEquals(mdcContext, currentMDCCOntext);
        }

        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));
    }
}
