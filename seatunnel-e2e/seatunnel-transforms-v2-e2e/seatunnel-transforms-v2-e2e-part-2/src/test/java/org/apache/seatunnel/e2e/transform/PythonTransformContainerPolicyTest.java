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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.spark.Spark2Container;
import org.apache.seatunnel.e2e.common.container.spark.Spark35Container;
import org.apache.seatunnel.e2e.common.container.spark.Spark3Container;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.EngineImageJdkUpgrader;
import org.apache.seatunnel.e2e.common.util.PythonTestImage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

class PythonTransformContainerPolicyTest {

    private static final String CONFIG = "/python_transform/python_transform.conf";

    @ParameterizedTest
    @EnumSource(
            value = TestContainerId.class,
            names = {"SPARK_2_4", "SPARK_3_3", "SPARK_3_5"})
    void preservesPreparedSparkImageAndLaunchCommands(TestContainerId containerId)
            throws Exception {
        String engineImage;
        String starter;
        TestContainer original;
        switch (containerId) {
            case SPARK_2_4:
                engineImage = "java11/tyrantlucifer/spark:2.4.6";
                starter = "start-seatunnel-spark-2-connector-v2.sh";
                original = new Spark2Container();
                break;
            case SPARK_3_3:
                engineImage = "java11/tyrantlucifer/spark:3.3.0";
                starter = "start-seatunnel-spark-3-connector-v2.sh";
                original = new Spark3Container();
                break;
            case SPARK_3_5:
                engineImage = "apache/spark:3.5.8";
                starter = "start-seatunnel-spark-3.5-connector-v2.sh";
                original = new Spark35Container();
                break;
            default:
                throw new IllegalArgumentException("Unsupported Spark container " + containerId);
        }
        String preparedImage = "seatunnel-python-test:" + containerId.name().toLowerCase();
        List<String> runtimeImages = new ArrayList<>();
        Container.ExecResult success = Mockito.mock(Container.ExecResult.class);
        Mockito.when(success.getExitCode()).thenReturn(0);
        Mockito.when(success.getStdout()).thenReturn("");
        Mockito.when(success.getStderr()).thenReturn("");
        // Stub external image/build/start boundaries, then observe the real harness startup and
        // job submission. No Docker daemon or shared host volume is touched by this regression.
        try (MockedStatic<PythonTestImage> images = Mockito.mockStatic(PythonTestImage.class);
                MockedStatic<EngineImageJdkUpgrader> jdkImages =
                        Mockito.mockStatic(EngineImageJdkUpgrader.class);
                MockedStatic<ContainerUtil> files = Mockito.mockStatic(ContainerUtil.class);
                MockedStatic<FileUtils> directories = Mockito.mockStatic(FileUtils.class);
                MockedStatic<Startables> starts = Mockito.mockStatic(Startables.class);
                MockedConstruction<GenericContainer> runtimes =
                        Mockito.mockConstruction(
                                GenericContainer.class,
                                Mockito.withSettings().defaultAnswer(Mockito.RETURNS_SELF),
                                (runtime, context) -> {
                                    runtimeImages.add((String) context.arguments().get(0));
                                    Mockito.when(runtime.getDockerImageName())
                                            .thenReturn(preparedImage);
                                    Mockito.when(
                                                    runtime.execInContainer(
                                                            Mockito.any(String[].class)))
                                            .thenReturn(success);
                                })) {
            jdkImages
                    .when(() -> EngineImageJdkUpgrader.toJava11(Mockito.anyString()))
                    .thenAnswer(invocation -> "java11/" + invocation.getArgument(0));
            images.when(() -> PythonTestImage.withPython(engineImage)).thenReturn(preparedImage);
            starts.when(() -> Startables.deepStart(Mockito.any(Stream.class)))
                    .thenReturn(CompletableFuture.completedFuture(null));
            files.when(() -> ContainerUtil.adaptPathForWin(Mockito.anyString()))
                    .thenCallRealMethod();
            files.when(
                            () ->
                                    ContainerUtil.copyConfigFileToContainer(
                                            Mockito.any(), Mockito.eq(CONFIG)))
                    .thenReturn("/tmp/seatunnel/config/python_transform.conf");

            TestContainer prepared = TestPythonTransformIT.withPythonTransformPolicy(original);
            Assertions.assertEquals(containerId, prepared.identifier());
            prepared.startUp();
            Assertions.assertEquals(Collections.singletonList(preparedImage), runtimeImages);
            Assertions.assertEquals(1, runtimes.constructed().size());
            GenericContainer<?> runtime = runtimes.constructed().get(0);
            if (containerId == TestContainerId.SPARK_3_5) {
                Mockito.verify(runtime)
                        .withCommand(
                                "/opt/spark/bin/spark-class",
                                "org.apache.spark.deploy.master.Master",
                                "--host",
                                "0.0.0.0");
            } else {
                // Spark 2/3 retain their inherited image command.
                Mockito.verify(runtime, Mockito.never()).withCommand(Mockito.any(String[].class));
            }
            Mockito.verify(runtime, Mockito.never())
                    .withEnv(Mockito.eq("JAVA_TOOL_OPTIONS"), Mockito.anyString());
            Assertions.assertEquals(0, prepared.executeJob(CONFIG).getExitCode());
            Mockito.verify(runtime)
                    .execInContainer(
                            "bash",
                            "-c",
                            "/tmp/seatunnel/bin/"
                                    + starter
                                    + " --config /tmp/seatunnel/config/python_transform.conf"
                                    + " --name python_transform.conf --master local --deploy-mode client");
            images.verify(() -> PythonTestImage.withPython(engineImage), Mockito.atLeastOnce());
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "python_transform.conf",
                "python_transform_path.conf",
                "python_transform_multi_table.conf",
                "python_transform_table_transform_only.conf"
            })
    void keepsPythonPolicyInSparkDriverAndExecutorJobConfig(String resource) {
        Config env =
                ConfigFactory.parseURL(getClass().getResource("/python_transform/" + resource))
                        .getConfig("env");
        String expected =
                "-Dseatunnel.transform.python.enabled=true"
                        + " -Dseatunnel.transform.python.allowed-executables="
                        + "/usr/bin/python3,/usr/local/bin/python3,/opt/bitnami/python/bin/python3,"
                        + "/usr/bin/python,/usr/local/bin/python";
        Assertions.assertEquals(expected, env.getString("spark.driver.extraJavaOptions"));
        Assertions.assertEquals(expected, env.getString("spark.executor.extraJavaOptions"));
    }
}
