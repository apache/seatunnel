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
package org.apache.seatunnel.e2e.connector.file.hadoop;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.stream.Stream;

@Slf4j
public class HadoopFileIT extends TestSuiteBase implements TestResource {

    private static final String HADOOP_IMAGE = "liverrrr/hdfs:2.7.5";

    private static final String HOST = "hadoop001";
    private static final Integer HDFS_PORT = 8020;
    private static final Integer[] HADOOP_PORTS = {22, 8020, 50070, 50010, 50020, 50075};

    private GenericContainer<?> hadoopContainer;
    private FileSystem fileSystem;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        hadoopContainer =
                new GenericContainer<>(HADOOP_IMAGE)
                        .withExposedPorts(HADOOP_PORTS)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        hadoopContainer.setPortBindings(
                Lists.newArrayList(
                        "22022:22",
                        "8020:8020",
                        "50070:50070",
                        "50010:50010",
                        "50020:50020",
                        "50075:50075"));
        hadoopContainer.start();
        Startables.deepStart(Stream.of(hadoopContainer)).join();
        log.info("hadoop container started");

        Configuration configuration = new Configuration();
        fileSystem =
                FileSystem.get(
                        new URI(
                                String.format(
                                        "hdfs://%s:%d", hadoopContainer.getHost(), HDFS_PORT)),
                        configuration);
        copyFileIntoHadoop("/json/e2e.json", "/seatunnel/read/json/e2e.json");
    }

    @TestTemplate
    public void testHadoopFileReadAndWrite(TestContainer container)
            throws InterruptedException, IOException {
        TestHelper helper = new TestHelper(container);
        // test write hadoop json file
        helper.execute("/json/fake_to_hadoop_file_json.conf");
        // test read hadoop json file
        helper.execute("/json/hadoop_file_json_to_assert.conf");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (fileSystem != null) {
            fileSystem.close();
        }
        if (hadoopContainer != null) {
            hadoopContainer.close();
        }
    }

    private void copyFileIntoHadoop(String fileName, String targetPath) throws IOException {
        InputStream in =
                new BufferedInputStream(Files.newInputStream(getResourceFile(fileName).toPath()));
        OutputStream out = fileSystem.create(new Path(targetPath));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    private File getResourceFile(String local) {
        File file =
                new File(Paths.get(System.getProperty("user.dir")) + "/src/test/resources" + local);
        if (file.exists()) {
            return file;
        }
        throw new IllegalArgumentException(local + " doesn't exist");
    }
}
