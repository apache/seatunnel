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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Optional;
import java.util.stream.Stream;

public class TestDynamicCompileIT extends TestSuiteBase implements TestResource {

    private final String basePath = "/dynamic_compile/conf/";

    private static final String TMP_DIR = "/tmp";
    private GenericContainer<?> mockserverContainer;
    private static final String IMAGE = "mockserver/mockserver:5.14.0";

    @BeforeAll
    @Override
    public void startUp() {
        Optional<URL> resource =
                Optional.ofNullable(
                        TestDynamicCompileIT.class.getResource(
                                "/dynamic_compile/conf/mockserver-config.json"));
        this.mockserverContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("mockserver")
                        .withExposedPorts(1080)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(
                                        new File(
                                                        resource.orElseThrow(
                                                                        () ->
                                                                                new IllegalArgumentException(
                                                                                        "Can not get config file of mockServer"))
                                                                .getPath())
                                                .getAbsolutePath()),
                                TMP_DIR + "/mockserver-config.json")
                        .withEnv(
                                "MOCKSERVER_INITIALIZATION_JSON_PATH",
                                TMP_DIR + "/mockserver-config.json")
                        .withEnv("MOCKSERVER_LOG_LEVEL", "WARN")
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .waitingFor(new HttpWaitStrategy().forPath("/").forStatusCode(404));
        Startables.deepStart(Stream.of(mockserverContainer)).join();
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Fake/lib && cd /tmp/seatunnel/plugins/Fake/lib && wget  "
                                        + "https://repo1.maven.org/maven2/cn/hutool/hutool-all/5.3.6/hutool-all-5.3.6.jar");
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @AfterAll
    @Override
    public void tearDown() {
        if (mockserverContainer != null) {
            mockserverContainer.stop();
        }
    }

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
    public void testDynamicSingleCompileJavaMultiTable(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        basePath + "single_dynamic_java_compile_transform_multi_table.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testDynamicSingleCompileJavaOldVersionCompatible(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        basePath + "single_dynamic_java_compile_transform_compatible.conf");
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

    @TestTemplate
    public void testHttpDynamic(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(basePath + "single_dynamic_http_compile_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
