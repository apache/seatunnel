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
import java.util.concurrent.Executors;

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
    }

    @Test
    public void testMDCContext() throws Exception {
        MDCContext.current();
        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));

        MDCContext mdcContext = MDCContext.of(1, 2, 3);
        mdcContext.put();
        Assertions.assertEquals("1", MDC.get(MDCContext.JOB_ID));
        Assertions.assertEquals("2", MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertEquals("3", MDC.get(MDCContext.TASK_ID));

        MDCContext currentMDCCOntext = MDCContext.current();
        Assertions.assertEquals(mdcContext, currentMDCCOntext);

        mdcContext.clear();
        Assertions.assertNull(MDC.get(MDCContext.JOB_ID));
        Assertions.assertNull(MDC.get(MDCContext.PIPELINE_ID));
        Assertions.assertNull(MDC.get(MDCContext.TASK_ID));
    }
}
