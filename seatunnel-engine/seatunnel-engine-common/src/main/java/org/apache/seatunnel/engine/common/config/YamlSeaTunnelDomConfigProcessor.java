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

package org.apache.seatunnel.engine.common.config;

import org.apache.seatunnel.engine.common.config.server.AllocateStrategy;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarHAStorageConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageMode;
import org.apache.seatunnel.engine.common.config.server.CoordinatorServiceConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.config.server.QueueType;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.config.server.ServerConfigOptions;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.common.config.server.TelemetryConfig;
import org.apache.seatunnel.engine.common.config.server.TelemetryLogsConfig;
import org.apache.seatunnel.engine.common.config.server.TelemetryMetricConfig;
import org.apache.seatunnel.engine.common.config.server.ThreadShareMode;

import org.apache.commons.lang3.StringUtils;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.config.AbstractDomConfigProcessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;

public class YamlSeaTunnelDomConfigProcessor extends AbstractDomConfigProcessor {
    private static final ILogger LOGGER = Logger.getLogger(YamlSeaTunnelDomConfigProcessor.class);

    private final SeaTunnelConfig config;

    YamlSeaTunnelDomConfigProcessor(boolean domLevel3, SeaTunnelConfig config) {
        super(domLevel3);
        this.config = config;
    }

    @Override
    public void buildConfig(Node rootNode) {
        for (Node node : childElements(rootNode)) {
            String nodeName = cleanNodeName(node);
            if (occurrenceSet.contains(nodeName)) {
                throw new InvalidConfigurationException(
                        "Duplicate '" + nodeName + "' definition found in the configuration.");
            }
            if (handleNode(node, nodeName)) {
                continue;
            }
            if (!SeaTunnelConfigSections.canOccurMultipleTimes(nodeName)) {
                occurrenceSet.add(nodeName);
            }
        }
    }

    private boolean handleNode(Node node, String name) {
        if (SeaTunnelConfigSections.ENGINE.isEqual(name)) {
            parseEngineConfig(node, config);
        } else {
            return true;
        }
        return false;
    }

    private SlotServiceConfig parseSlotServiceConfig(Node slotServiceNode) {
        SlotServiceConfig slotServiceConfig = new SlotServiceConfig();
        for (Node node : childElements(slotServiceNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.DYNAMIC_SLOT.key().equals(name)) {
                slotServiceConfig.setDynamicSlot(getBooleanValue(getTextContent(node)));
            } else if (ServerConfigOptions.SLOT_NUM.key().equals(name)) {
                slotServiceConfig.setSlotNum(
                        getIntegerValue(ServerConfigOptions.SLOT_NUM.key(), getTextContent(node)));
            } else if (ServerConfigOptions.SLOT_ALLOCATE_STRATEGY.key().equals(name)) {
                slotServiceConfig.setAllocateStrategy(
                        AllocateStrategy.valueOf(getTextContent(node).toUpperCase()));
            } else {
                LOGGER.warning("Unrecognized element: " + name);
            }
        }
        return slotServiceConfig;
    }

    private CoordinatorServiceConfig parseCoordinatorServiceConfig(Node coordinatorServiceNode) {
        CoordinatorServiceConfig coordinatorServiceConfig = new CoordinatorServiceConfig();
        for (Node node : childElements(coordinatorServiceNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.MAX_THREAD_NUM.key().equals(name)) {
                coordinatorServiceConfig.setMaxThreadNum(
                        getIntegerValue(
                                ServerConfigOptions.MAX_THREAD_NUM.key(), getTextContent(node)));
            } else if (ServerConfigOptions.CORE_THREAD_NUM.key().equals(name)) {
                coordinatorServiceConfig.setCoreThreadNum(
                        getIntegerValue(
                                ServerConfigOptions.CORE_THREAD_NUM.key(), getTextContent(node)));
            } else {
                LOGGER.warning("Unrecognized element: " + name);
            }
        }
        return coordinatorServiceConfig;
    }

    private void parseEngineConfig(Node engineNode, SeaTunnelConfig config) {
        final EngineConfig engineConfig = config.getEngineConfig();
        for (Node node : childElements(engineNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.BACKUP_COUNT.key().equals(name)) {
                engineConfig.setBackupCount(
                        getIntegerValue(
                                ServerConfigOptions.BACKUP_COUNT.key(), getTextContent(node)));
            } else if (ServerConfigOptions.QUEUE_TYPE.key().equals(name)) {
                engineConfig.setQueueType(
                        QueueType.valueOf(getTextContent(node).toUpperCase(Locale.ROOT)));
            } else if (ServerConfigOptions.PRINT_EXECUTION_INFO_INTERVAL.key().equals(name)) {
                engineConfig.setPrintExecutionInfoInterval(
                        getIntegerValue(
                                ServerConfigOptions.PRINT_EXECUTION_INFO_INTERVAL.key(),
                                getTextContent(node)));
            } else if (ServerConfigOptions.PRINT_JOB_METRICS_INFO_INTERVAL.key().equals(name)) {
                engineConfig.setPrintJobMetricsInfoInterval(
                        getIntegerValue(
                                ServerConfigOptions.PRINT_JOB_METRICS_INFO_INTERVAL.key(),
                                getTextContent(node)));
            } else if (ServerConfigOptions.JOB_METRICS_BACKUP_INTERVAL.key().equals(name)) {
                engineConfig.setJobMetricsBackupInterval(
                        getIntegerValue(
                                ServerConfigOptions.JOB_METRICS_BACKUP_INTERVAL.key(),
                                getTextContent(node)));
            } else if (ServerConfigOptions.TASK_EXECUTION_THREAD_SHARE_MODE.key().equals(name)) {
                String mode = getTextContent(node).toUpperCase(Locale.ROOT);
                if (!Arrays.asList("ALL", "OFF", "PART").contains(mode)) {
                    throw new IllegalArgumentException(
                            ServerConfigOptions.TASK_EXECUTION_THREAD_SHARE_MODE
                                    + " must in [ALL, OFF, PART]");
                }
                engineConfig.setTaskExecutionThreadShareMode(ThreadShareMode.valueOf(mode));
            } else if (ServerConfigOptions.SLOT_SERVICE.key().equals(name)) {
                engineConfig.setSlotServiceConfig(parseSlotServiceConfig(node));
            } else if (ServerConfigOptions.CHECKPOINT.key().equals(name)) {
                engineConfig.setCheckpointConfig(parseCheckpointConfig(node));
            } else if (ServerConfigOptions.HISTORY_JOB_EXPIRE_MINUTES.key().equals(name)) {
                engineConfig.setHistoryJobExpireMinutes(
                        getIntegerValue(
                                ServerConfigOptions.HISTORY_JOB_EXPIRE_MINUTES.key(),
                                getTextContent(node)));
            } else if (ServerConfigOptions.CONNECTOR_JAR_STORAGE_CONFIG.key().equals(name)) {
                engineConfig.setConnectorJarStorageConfig(parseConnectorJarStorageConfig(node));
            } else if (ServerConfigOptions.CLASSLOADER_CACHE_MODE.key().equals(name)) {
                engineConfig.setClassloaderCacheMode(getBooleanValue(getTextContent(node)));
            } else if (ServerConfigOptions.EVENT_REPORT_HTTP.equalsIgnoreCase(name)) {
                NamedNodeMap attributes = node.getAttributes();
                Node urlNode = attributes.getNamedItem(ServerConfigOptions.EVENT_REPORT_HTTP_URL);
                if (urlNode != null) {
                    engineConfig.setEventReportHttpApi(getTextContent(urlNode));
                    Node headersNode =
                            attributes.getNamedItem(ServerConfigOptions.EVENT_REPORT_HTTP_HEADERS);
                    if (headersNode != null) {
                        Map<String, String> headers = new LinkedHashMap<>();
                        NodeList nodeList = headersNode.getChildNodes();
                        for (int i = 0; i < nodeList.getLength(); i++) {
                            Node item = nodeList.item(i);
                            headers.put(cleanNodeName(item), getTextContent(item));
                        }
                        engineConfig.setEventReportHttpHeaders(headers);
                    }
                }
            } else if (ServerConfigOptions.TELEMETRY.key().equals(name)) {
                engineConfig.setTelemetryConfig(parseTelemetryConfig(node));
            } else if (ServerConfigOptions.JOB_SCHEDULE_STRATEGY.key().equals(name)) {
                engineConfig.setScheduleStrategy(
                        ScheduleStrategy.valueOf(getTextContent(node).toUpperCase(Locale.ROOT)));
            } else if (ServerConfigOptions.HTTP.key().equals(name)) {
                engineConfig.setHttpConfig(parseHttpConfig(node));
            } else if (ServerConfigOptions.COORDINATOR_SERVICE.key().equals(name)) {
                engineConfig.setCoordinatorServiceConfig(parseCoordinatorServiceConfig(node));
            } else {
                LOGGER.warning("Unrecognized element: " + name);
            }
        }

        if (engineConfig.getSlotServiceConfig().isDynamicSlot()) {
            // If dynamic slot is enabled, the schedule strategy must be REJECT
            LOGGER.info("Dynamic slot is enabled, the schedule strategy is set to REJECT");
            engineConfig.setScheduleStrategy(ScheduleStrategy.REJECT);
        }
    }

    private CheckpointConfig parseCheckpointConfig(Node checkpointNode) {
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        for (Node node : childElements(checkpointNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.CHECKPOINT_INTERVAL.key().equals(name)) {
                checkpointConfig.setCheckpointInterval(
                        getIntegerValue(
                                ServerConfigOptions.CHECKPOINT_INTERVAL.key(),
                                getTextContent(node)));
            } else if (ServerConfigOptions.CHECKPOINT_TIMEOUT.key().equals(name)) {
                checkpointConfig.setCheckpointTimeout(
                        getIntegerValue(
                                ServerConfigOptions.CHECKPOINT_TIMEOUT.key(),
                                getTextContent(node)));
            } else if (ServerConfigOptions.SCHEMA_CHANGE_CHECKPOINT_TIMEOUT.key().equals(name)) {
                checkpointConfig.setSchemaChangeCheckpointTimeout(
                        getIntegerValue(
                                ServerConfigOptions.SCHEMA_CHANGE_CHECKPOINT_TIMEOUT.key(),
                                getTextContent(node)));
            } else if (ServerConfigOptions.CHECKPOINT_STORAGE.key().equals(name)) {
                checkpointConfig.setStorage(parseCheckpointStorageConfig(node));
            } else {
                LOGGER.warning("Unrecognized element: " + name);
            }
        }

        return checkpointConfig;
    }

    private CheckpointStorageConfig parseCheckpointStorageConfig(Node checkpointStorageConfigNode) {
        CheckpointStorageConfig checkpointStorageConfig = new CheckpointStorageConfig();
        for (Node node : childElements(checkpointStorageConfigNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.CHECKPOINT_STORAGE_TYPE.key().equals(name)) {
                checkpointStorageConfig.setStorage(getTextContent(node));
            } else if (ServerConfigOptions.CHECKPOINT_STORAGE_MAX_RETAINED.key().equals(name)) {
                checkpointStorageConfig.setMaxRetainedCheckpoints(
                        getIntegerValue(
                                ServerConfigOptions.CHECKPOINT_STORAGE_MAX_RETAINED.key(),
                                getTextContent(node)));
            } else if (ServerConfigOptions.CHECKPOINT_STORAGE_PLUGIN_CONFIG.key().equals(name)) {
                Map<String, String> pluginConfig = parseCheckpointPluginConfig(node);
                checkpointStorageConfig.setStoragePluginConfig(pluginConfig);
            } else {
                LOGGER.warning("Unrecognized element: " + name);
            }
        }
        return checkpointStorageConfig;
    }

    /**
     * Parse checkpoint plugin config.
     *
     * @param checkpointPluginConfigNode checkpoint plugin config node
     * @return checkpoint plugin config
     */
    private Map<String, String> parseCheckpointPluginConfig(Node checkpointPluginConfigNode) {
        Map<String, String> checkpointPluginConfig = new HashMap<>();
        for (Node node : childElements(checkpointPluginConfigNode)) {
            String name = node.getNodeName();
            checkpointPluginConfig.put(name, getTextContent(node));
        }
        return checkpointPluginConfig;
    }

    private ConnectorJarStorageConfig parseConnectorJarStorageConfig(
            Node connectorJarStorageConfigNode) {
        ConnectorJarStorageConfig connectorJarStorageConfig = new ConnectorJarStorageConfig();
        for (Node node : childElements(connectorJarStorageConfigNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.ENABLE_CONNECTOR_JAR_STORAGE.key().equals(name)) {
                connectorJarStorageConfig.setEnable(getBooleanValue(getTextContent(node)));
            } else if (ServerConfigOptions.CONNECTOR_JAR_STORAGE_MODE.key().equals(name)) {
                String mode = getTextContent(node).toUpperCase();
                if (StringUtils.isNotBlank(mode)
                        && !Arrays.asList("SHARED", "ISOLATED").contains(mode)) {
                    throw new IllegalArgumentException(
                            ServerConfigOptions.CONNECTOR_JAR_STORAGE_MODE
                                    + " must in [SHARED, ISOLATED]");
                }
                connectorJarStorageConfig.setStorageMode(ConnectorJarStorageMode.valueOf(mode));
            } else if (ServerConfigOptions.CONNECTOR_JAR_STORAGE_PATH.key().equals(name)) {
                connectorJarStorageConfig.setStoragePath(getTextContent(node));
            } else if (ServerConfigOptions.CONNECTOR_JAR_CLEANUP_TASK_INTERVAL.key().equals(name)) {
                connectorJarStorageConfig.setCleanupTaskInterval(
                        getIntegerValue(
                                ServerConfigOptions.CONNECTOR_JAR_CLEANUP_TASK_INTERVAL.key(),
                                getTextContent(node)));
            } else if (ServerConfigOptions.CONNECTOR_JAR_EXPIRY_TIME.key().equals(name)) {
                connectorJarStorageConfig.setConnectorJarExpiryTime(
                        getIntegerValue(
                                ServerConfigOptions.CONNECTOR_JAR_EXPIRY_TIME.key(),
                                getTextContent(node)));
            } else if (ServerConfigOptions.CONNECTOR_JAR_HA_STORAGE_CONFIG.key().equals(name)) {
                connectorJarStorageConfig.setConnectorJarHAStorageConfig(
                        parseConnectorJarHAStorageConfig(node));
            } else {
                LOGGER.warning("Unrecognized element: " + name);
            }
        }
        return connectorJarStorageConfig;
    }

    private ConnectorJarHAStorageConfig parseConnectorJarHAStorageConfig(
            Node connectorJarHAStorageConfigNode) {
        ConnectorJarHAStorageConfig connectorJarHAStorageConfig = new ConnectorJarHAStorageConfig();
        for (Node node : childElements(connectorJarHAStorageConfigNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.CONNECTOR_JAR_HA_STORAGE_TYPE.key().equals(name)) {
                String type = getTextContent(node);
                if (StringUtils.isNotBlank(type)
                        && !Arrays.asList("localfile", "hdfs").contains(type)) {
                    throw new IllegalArgumentException(
                            ServerConfigOptions.CONNECTOR_JAR_HA_STORAGE_TYPE
                                    + " must in [localfile, hdfs]");
                }
                connectorJarHAStorageConfig.setType(type);
            } else if (ServerConfigOptions.CONNECTOR_JAR_HA_STORAGE_PLUGIN_CONFIG
                    .key()
                    .equals(name)) {
                Map<String, String> connectorJarHAStoragePluginConfig =
                        parseConnectorJarHAStoragePluginConfig(node);
                connectorJarHAStorageConfig.setStoragePluginConfig(
                        connectorJarHAStoragePluginConfig);
            } else {
                LOGGER.warning("Unrecognized element: " + name);
            }
        }
        return connectorJarHAStorageConfig;
    }

    private Map<String, String> parseConnectorJarHAStoragePluginConfig(
            Node connectorJarHAStoragePluginConfigNode) {
        Map<String, String> connectorJarHAStoragePluginConfig = new HashMap<>();
        for (Node node : childElements(connectorJarHAStoragePluginConfigNode)) {
            String name = node.getNodeName();
            connectorJarHAStoragePluginConfig.put(name, getTextContent(node));
        }
        return connectorJarHAStoragePluginConfig;
    }

    private TelemetryConfig parseTelemetryConfig(Node telemetryNode) {
        TelemetryConfig telemetryConfig = new TelemetryConfig();
        for (Node node : childElements(telemetryNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.TELEMETRY_METRIC.key().equals(name)) {
                telemetryConfig.setMetric(parseTelemetryMetricConfig(node));
            } else if (ServerConfigOptions.TELEMETRY_LOGS.key().equals(name)) {
                telemetryConfig.setLogs(parseTelemetryLogsConfig(node));
            } else {
                LOGGER.warning("Unrecognized element: " + name);
            }
        }

        return telemetryConfig;
    }

    private TelemetryMetricConfig parseTelemetryMetricConfig(Node metricNode) {
        TelemetryMetricConfig metricConfig = new TelemetryMetricConfig();
        for (Node node : childElements(metricNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.TELEMETRY_METRIC_ENABLED.key().equals(name)) {
                metricConfig.setEnabled(getBooleanValue(getTextContent(node)));
            } else {
                LOGGER.warning("Unrecognized element: " + name);
            }
        }

        return metricConfig;
    }

    private TelemetryLogsConfig parseTelemetryLogsConfig(Node logsNode) {
        TelemetryLogsConfig logsConfig = new TelemetryLogsConfig();
        for (Node node : childElements(logsNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.TELEMETRY_LOGS_SCHEDULED_DELETION_ENABLE.key().equals(name)) {
                logsConfig.setEnabled(getBooleanValue(getTextContent(node)));
            } else {
                LOGGER.warning("Unrecognized element: " + name);
            }
        }

        return logsConfig;
    }

    private HttpConfig parseHttpConfig(Node httpNode) {
        HttpConfig httpConfig = new HttpConfig();
        for (Node node : childElements(httpNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.PORT.key().equals(name)) {
                httpConfig.setPort(
                        getIntegerValue(ServerConfigOptions.PORT.key(), getTextContent(node)));
            } else if (ServerConfigOptions.CONTEXT_PATH.key().equals(name)) {
                httpConfig.setContextPath(getTextContent(node));
            } else if (ServerConfigOptions.ENABLE_HTTP.key().equals(name)) {
                httpConfig.setEnabled(getBooleanValue(getTextContent(node)));
            } else if (ServerConfigOptions.ENABLE_DYNAMIC_PORT.key().equals(name)) {
                httpConfig.setEnableDynamicPort(getBooleanValue(getTextContent(node)));
            } else if (ServerConfigOptions.PORT_RANGE.key().equals(name)) {
                httpConfig.setPortRange(
                        getIntegerValue(
                                ServerConfigOptions.PORT_RANGE.key(), getTextContent(node)));
            } else {
                LOGGER.warning("Unrecognized element: " + name);
            }
        }
        return httpConfig;
    }
}
