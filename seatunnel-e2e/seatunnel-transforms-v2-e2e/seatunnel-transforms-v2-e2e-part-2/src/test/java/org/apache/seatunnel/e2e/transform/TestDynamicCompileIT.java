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

package org.apache.seatunnel.e2e.transform;

import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class TestDynamicCompileIT extends TestSuiteBase {

    private final String basePath = "/dynamic_compile/conf/";

    @TestTemplate
    public void testDynamicSingleCompileGroovy(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(basePath + "single_dynamic_groovy_compile_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testDynamicSingleCompileJava(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(basePath + "single_dynamic_java_compile_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testDynamicMultipleCompileGroovy(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(basePath + "multiple_dynamic_groovy_compile_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testDynamicMultipleCompileJava(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(basePath + "multiple_dynamic_java_compile_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testDynamicMixedCompileJavaAndGroovy(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(basePath + "mixed_dynamic_groovy_java_compile_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testDynamicSinglePathGroovy(TestContainer container)
            throws IOException, InterruptedException {
        container.copyFileToContainer("/dynamic_compile/source_file/GroovyFile", "/tmp/GroovyFile");
        Container.ExecResult execResult =
                container.executeJob(basePath + "single_groovy_path_compile.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testDynamicSinglePathJava(TestContainer container)
            throws IOException, InterruptedException {
        container.copyFileToContainer("/dynamic_compile/source_file/JavaFile", "/tmp/JavaFile");
        Container.ExecResult execResult =
                container.executeJob(basePath + "single_java_path_compile.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
