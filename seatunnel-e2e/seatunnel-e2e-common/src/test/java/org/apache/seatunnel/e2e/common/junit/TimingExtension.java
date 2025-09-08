/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.common.junit;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public class TimingExtension implements BeforeTestExecutionCallback, AfterTestExecutionCallback {
    private static final Logger LOG = LoggerFactory.getLogger(TimingExtension.class);
    private static final String START_TIME = "start time";

    @Override
    public void afterTestExecution(ExtensionContext context) throws Exception {
        Class<?> testClass = context.getRequiredTestClass();
        Method testMethod = context.getRequiredTestMethod();
        long startTime = getStore(context).remove(START_TIME, long.class);
        long duration = System.currentTimeMillis() - startTime;
        LOG.info(
                " [{}#{}] E2E test case cost {}s.",
                testClass.getName(),
                testMethod.getName(),
                duration / 1000);
    }

    @Override
    public void beforeTestExecution(ExtensionContext context) throws Exception {
        getStore(context).put(START_TIME, System.currentTimeMillis());
    }

    private Store getStore(ExtensionContext context) {
        return context.getStore(
                ExtensionContext.Namespace.create(getClass(), context.getRequiredTestMethod()));
    }
}
