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

package org.apache.seatunnel.e2e.connector.google.firestore;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

@Disabled("Disabled because it needs your infura project url to run this test")
public class Web3jIT extends TestSuiteBase implements TestResource {

    private static final String FIRESTORE_CONF_FILE = "/firestore/web3j_to_assert.conf";

    @TestTemplate
    public void testWeb3j(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob(FIRESTORE_CONF_FILE);
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @Override
    public void startUp() throws Exception {}

    @Override
    public void tearDown() throws Exception {}
}
