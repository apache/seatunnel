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

package org.apache.seatunnel.e2e.spark;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is the base class of SparkEnvironment test. The before method will create a Spark master, and after method will close the Spark master.
 * You can use {@link SparkContainer#executeSeaTunnelSparkJob} to submit a seatunnel conf and a seatunnel spark job.
 */
public abstract class SparkContainer {

    private static final Logger LOG = LoggerFactory.getLogger(SparkContainer.class);

    private static final String SPARK_DOCKER_IMAGE = "bitnami/spark:2.4.3";
    public static final Network NETWORK = Network.newNetwork();

    protected GenericContainer<?> master;
    private static final Path PROJECT_ROOT_PATH = Paths.get(System.getProperty("user.dir")).getParent().getParent();
    private static final String SEATUNNEL_SPARK_JAR = "seatunnel-core-spark.jar";
    private static final String PLUGIN_MAPPING_FILE = "plugin-mapping.properties";
    private static final String SEATUNNEL_HOME = "/tmp/spark/seatunnel";
    private static final String SPARK_JAR_PATH = Paths.get(SEATUNNEL_HOME, "lib", SEATUNNEL_SPARK_JAR).toString();
    private static final String CONNECTORS_PATH = Paths.get(SEATUNNEL_HOME, "connectors").toString();

    private static final int WAIT_SPARK_JOB_SUBMIT = 5000;

    @Before
    public void before() throws InterruptedException {
        master = new GenericContainer<>(SPARK_DOCKER_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("spark-master")
            .withExposedPorts()
            .withEnv("SPARK_MODE", "master")
            .withLogConsumer(new Slf4jLogConsumer(LOG));
        // In most case we can just use standalone mode to execute a spark job, if we want to use cluster mode, we need to
        // start a worker.

        Startables.deepStart(Stream.of(master)).join();
        copySeaTunnelSparkFile();
        LOG.info("Spark container started");
    }

    @After
    public void close() {
        if (master != null) {
            master.stop();
        }
    }

    public Container.ExecResult executeSeaTunnelSparkJob(String confFile) throws IOException, InterruptedException {
        final String confPath = getResource(confFile);
        if (!new File(confPath).exists()) {
            throw new IllegalArgumentException(confFile + " doesn't exist");
        }
        final String targetConfInContainer = Paths.get("/tmp", confFile).toString();
        master.copyFileToContainer(MountableFile.forHostPath(confPath), targetConfInContainer);

        // TODO: use start-seatunnel-spark.sh to run the spark job. Need to modified the SparkStarter can find the seatunnel-core-spark.jar.
        // Running IT use cases under Windows requires replacing \ with /
        String jar = SPARK_JAR_PATH.replaceAll("\\\\", "/");
        String conf = targetConfInContainer.replaceAll("\\\\", "/");
        final List<String> command = new ArrayList<>();
        command.add("spark-submit");
        command.add("--class");
        command.add("org.apache.seatunnel.core.spark.SeatunnelSpark");
        command.add("--name");
        command.add("SeaTunnel");
        command.add("--master");
        command.add("local");
        command.add("--jars");
        command.add(
            getConnectorJarFiles()
                .stream()
                .map(j -> getConnectorPath(j.getName()))
                .collect(Collectors.joining(",")));
        command.add("--deploy-mode");
        command.add("client");
        command.add(jar);
        command.add("-c");
        command.add(conf);
        command.add("--master");
        command.add("local");
        command.add("--deploy-mode");
        command.add("client");

        Container.ExecResult execResult = master.execInContainer("bash", "-c", String.join(" ", command));
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        // wait job start
        Thread.sleep(WAIT_SPARK_JOB_SUBMIT);
        return execResult;
    }

    protected void copySeaTunnelSparkFile() {
        // copy jar to container
        String seatunnelCoreSparkJarPath = PROJECT_ROOT_PATH
            + "/seatunnel-core/seatunnel-core-spark/target/seatunnel-core-spark.jar";
        master.copyFileToContainer(MountableFile.forHostPath(seatunnelCoreSparkJarPath), SPARK_JAR_PATH);

        // copy connectors jar
        getConnectorJarFiles()
            .forEach(jar ->
                master.copyFileToContainer(
                    MountableFile.forHostPath(jar.getAbsolutePath()),
                    getConnectorPath(jar.getName())));

        // copy plugin-mapping.properties
        master.copyFileToContainer(
            MountableFile.forHostPath(PROJECT_ROOT_PATH + "/seatunnel-connectors/plugin-mapping.properties"),
            Paths.get(CONNECTORS_PATH, PLUGIN_MAPPING_FILE).toString());
    }

    private String getResource(String confFile) {
        return System.getProperty("user.dir") + "/src/test/resources" + confFile;
    }

    private String getConnectorPath(String fileName) {
        return Paths.get(CONNECTORS_PATH, "spark", fileName).toString();
    }

    private List<File> getConnectorJarFiles() {
        File jars = new File(PROJECT_ROOT_PATH +
            "/seatunnel-connectors/seatunnel-connectors-spark-dist/target/lib");
        return Arrays.stream(
                Objects.requireNonNull(
                    jars.listFiles(
                        f -> f.getName().contains("seatunnel-connector-spark"))))
            .collect(Collectors.toList());
    }
}
