/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.inlong.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/** InlongThreadFactory, used for creating thread. */
public class InlongThreadFactory implements ThreadFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongThreadFactory.class);

    public static final String NAMED_THREAD_PLACEHOLDER = "inlong-thread-factory";

    private final AtomicInteger mThreadNum = new AtomicInteger(1);

    private final String threadType;

    public InlongThreadFactory(String threadType) {
        this.threadType = threadType;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t =
                new Thread(
                        r,
                        threadType
                                + "-"
                                + NAMED_THREAD_PLACEHOLDER
                                + "-"
                                + mThreadNum.getAndIncrement());
        LOGGER.debug("{} created", t.getName());
        return t;
    }

    /**
     * Replace a unique name for thread factory created thread, replace this name for placeholder
     *
     * @param uniqueName
     */
    public static void nameThread(String uniqueName) {
        Thread.currentThread()
                .setName(
                        Thread.currentThread()
                                .getName()
                                .replace(NAMED_THREAD_PLACEHOLDER, uniqueName));
    }
}
