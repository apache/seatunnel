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

package org.apache.seatunnel.e2e.common.junit;

import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.ReusableTestContainer;

import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.Semaphore;

final class SharedTestContainerResource implements ExtensionContext.Store.CloseableResource {

    private final ReusableTestContainer container;
    private final Semaphore classLease = new Semaphore(1, true);
    private boolean started;

    SharedTestContainerResource(ReusableTestContainer container) {
        this.container = container;
    }

    ReusableTestContainer acquire(ContainerExtendedFactory extendedFactory) throws Exception {
        classLease.acquire();
        boolean acquired = false;
        try {
            if (!started) {
                container.startUp();
                started = true;
            }
            container.executeExtraCommands(extendedFactory);
            acquired = true;
            return container;
        } finally {
            if (!acquired) {
                classLease.release();
            }
        }
    }

    void release() throws Exception {
        try {
            container.cleanUpAfterTestClass();
        } finally {
            classLease.release();
        }
    }

    @Override
    public void close() throws Exception {
        if (started) {
            container.tearDown();
        }
    }
}
