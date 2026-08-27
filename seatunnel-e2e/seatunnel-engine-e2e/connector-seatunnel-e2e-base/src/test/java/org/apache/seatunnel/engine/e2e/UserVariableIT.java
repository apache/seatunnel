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

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UserVariableIT extends TestSuiteBase {

    @TestTemplate
    public void userVariableTest(TestContainer container) throws IOException, InterruptedException {
        List<String> variables = new ArrayList<>();
        String list = "[abc,def]";
        variables.add("resName=a$(date +\"%Y%m%d\")");
        variables.add("rowNum=10");
        variables.add("strTemplate=" + list);
        variables.add("nameType=string");
        variables.add("nameVal=abc");
        variables.add("pluginInputIdentifier=sql");
        Container.ExecResult execResult =
                container.executeJob("/fake_to_console.variables.conf", variables);
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void userVariableWithDefaultValueTest(TestContainer container)
            throws IOException, InterruptedException {
        List<String> variables = new ArrayList<>();
        String list = "[abc,def]";
        variables.add("strTemplate=" + list);
        variables.add("ageType=int");
        variables.add("nameVal=abc");
        variables.add("pluginInputIdentifier=sql");
        Container.ExecResult execResult =
                container.executeJob(
                        "/fake_to_console_with_default_value.variables.conf", variables);
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }
}
