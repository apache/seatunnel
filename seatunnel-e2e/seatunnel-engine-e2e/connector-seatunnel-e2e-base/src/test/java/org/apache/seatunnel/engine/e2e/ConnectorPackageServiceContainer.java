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

package org.apache.seatunnel.engine.e2e;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ConnectorPackageServiceContainer
        extends org.apache.seatunnel.e2e.common.container.seatunnel
                .ConnectorPackageServiceContainer {

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        super.startUp();
        log.info("The TestContainer[{}] is running.", identifier());
    }

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        super.tearDown();
        log.info("The TestContainer[{}] is closed.", identifier());
    }

    public Container.ExecResult executeSeaTunnelJob(String confFile)
            throws IOException, InterruptedException {
        return executeJob(confFile);
    }
}
