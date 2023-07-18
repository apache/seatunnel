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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.YamlSeaTunnelConfigBuilder;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.internal.config.AbstractConfigLocator;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.Stream;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.YAML_ACCEPTED_SUFFIXES;

@DisabledOnOs(OS.WINDOWS)
@Slf4j
public class SavePointToKafkaTest extends SavePointTest {

    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";
    private static KafkaContainer kafkaContainer =
            new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                    .withLogConsumer(
                            new Slf4jLogConsumer(DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));

    @BeforeAll
    public void before() {
        if (!kafkaContainer.isRunning()) {
            Startables.deepStart(Stream.of(kafkaContainer)).join();
            log.info("Kafka container started");
        }

        String name = SavePointToKafkaTest.class.getName();
        String yaml =
                "hazelcast:\n"
                        + "  cluster-name: seatunnel\n"
                        + "  network:\n"
                        + "    rest-api:\n"
                        + "      enabled: true\n"
                        + "      endpoint-groups:\n"
                        + "        CLUSTER_WRITE:\n"
                        + "          enabled: true\n"
                        + "    join:\n"
                        + "      tcp-ip:\n"
                        + "        enabled: true\n"
                        + "        member-list:\n"
                        + "          - localhost\n"
                        + "    port:\n"
                        + "      auto-increment: true\n"
                        + "      port-count: 100\n"
                        + "      port: 5801\n"
                        + "\n"
                        + "  properties:\n"
                        + "    hazelcast.invocation.max.retry.count: 200\n"
                        + "    hazelcast.tcp.join.port.try.count: 30\n"
                        + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                        + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                        + "    hazelcast.logging.type: log4j2\n"
                        + "    hazelcast.operation.generic.thread.count: 200\n";
        Config hazelcastConfig = Config.loadFromString(yaml);
        hazelcastConfig.setClusterName(
                TestUtils.getClusterName("AbstractSeaTunnelServerTest_" + name));

        YamlSeaTunnelConfigLocatorForKafka yamlConfigLocator =
                new YamlSeaTunnelConfigLocatorForKafka();
        SeaTunnelConfig seaTunnelConfig;
        if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            seaTunnelConfig =
                    new YamlSeaTunnelConfigBuilder(yamlConfigLocator.getIn())
                            .setProperties(null)
                            .build();
        } else {
            throw new RuntimeException("can't find yaml in resources");
        }
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        // Override bootstrap.servers
        seaTunnelConfig
                .getEngineConfig()
                .getCheckpointConfig()
                .getStorage()
                .getStoragePluginConfig()
                .put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        nodeEngine = instance.node.nodeEngine;
        server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        LOGGER = nodeEngine.getLogger(AbstractSeaTunnelServerTest.class);
    }

    @Override
    public void after() {
        super.after();
        kafkaContainer.close();
    }

    @Override
    public void restartServer() {
        super.after();
        this.before();
    }

    public final class YamlSeaTunnelConfigLocatorForKafka extends AbstractConfigLocator {

        String LOAD_CONFIG_FILE_FOR_KAFKA_PREFIX =
                Constant.HAZELCAST_SEATUNNEL_CONF_FILE_PREFIX.concat("-kafka");
        String HAZELCAST_SEATUNNEL_DEFAULT_YAML_FOR_KAFKA =
                LOAD_CONFIG_FILE_FOR_KAFKA_PREFIX.concat(".yaml");

        public YamlSeaTunnelConfigLocatorForKafka() {}

        @Override
        public boolean locateFromSystemProperty() {
            return loadFromSystemProperty(
                    Constant.SYSPROP_SEATUNNEL_CONFIG, YAML_ACCEPTED_SUFFIXES);
        }

        @Override
        protected boolean locateFromSystemPropertyOrFailOnUnacceptedSuffix() {
            return loadFromSystemPropertyOrFailOnUnacceptedSuffix(
                    Constant.SYSPROP_SEATUNNEL_CONFIG, YAML_ACCEPTED_SUFFIXES);
        }

        @Override
        protected boolean locateInWorkDir() {
            return loadFromWorkingDirectory(
                    LOAD_CONFIG_FILE_FOR_KAFKA_PREFIX, YAML_ACCEPTED_SUFFIXES);
        }

        @Override
        protected boolean locateOnClasspath() {
            return loadConfigurationFromClasspath(
                    LOAD_CONFIG_FILE_FOR_KAFKA_PREFIX, YAML_ACCEPTED_SUFFIXES);
        }

        @Override
        public boolean locateDefault() {
            loadDefaultConfigurationFromClasspath(HAZELCAST_SEATUNNEL_DEFAULT_YAML_FOR_KAFKA);
            return true;
        }
    }
}
