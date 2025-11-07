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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.common;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** Tools for creating RocketMq topic and group. */
public class RocketMqAdminUtil {

    public static String createUniqInstance(String prefix) {
        return prefix.concat("-").concat(UUID.randomUUID().toString());
    }

    public static RPCHook getAclRpcHook(String accessKey, String secretKey) {
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }

    /** Init default lite pull consumer */
    public static DefaultLitePullConsumer initDefaultLitePullConsumer(
            RocketMqBaseConfiguration config, boolean autoCommit) {
        DefaultLitePullConsumer consumer = null;
        if (Objects.isNull(consumer)) {
            if (StringUtils.isBlank(config.getAccessKey())
                    && StringUtils.isBlank(config.getSecretKey())) {
                consumer = new DefaultLitePullConsumer(config.getGroupId());
            } else {
                consumer =
                        new DefaultLitePullConsumer(
                                config.getGroupId(),
                                getAclRpcHook(config.getAccessKey(), config.getSecretKey()));
            }
        }
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        String uniqueName = createUniqInstance(config.getNamesrvAddr());
        consumer.setInstanceName(uniqueName);
        consumer.setUnitName(uniqueName);
        consumer.setAutoCommit(autoCommit);
        if (config.getBatchSize() != null) {
            consumer.setPullBatchSize(config.getBatchSize());
        }
        return consumer;
    }

    /** Init transaction producer */
    public static TransactionMQProducer initTransactionMqProducer(
            RocketMqBaseConfiguration config, TransactionListener listener) {
        RPCHook rpcHook = null;
        if (config.isAclEnable()) {
            rpcHook =
                    new AclClientRPCHook(
                            new SessionCredentials(config.getAccessKey(), config.getSecretKey()));
        }
        TransactionMQProducer producer = new TransactionMQProducer(config.getGroupId(), rpcHook);
        producer.setNamesrvAddr(config.getNamesrvAddr());
        producer.setInstanceName(createUniqInstance(config.getNamesrvAddr()));
        producer.setLanguage(LanguageCode.JAVA);
        producer.setTransactionListener(listener);
        if (config.getMaxMessageSize() != null) {
            producer.setMaxMessageSize(config.getMaxMessageSize());
        }
        if (config.getSendMsgTimeout() != null) {
            producer.setSendMsgTimeout(config.getSendMsgTimeout());
        }

        return producer;
    }

    public static DefaultMQProducer initDefaultMqProducer(RocketMqBaseConfiguration config) {
        RPCHook rpcHook = null;
        if (config.isAclEnable()) {
            rpcHook =
                    new AclClientRPCHook(
                            new SessionCredentials(config.getAccessKey(), config.getSecretKey()));
        }
        DefaultMQProducer producer = new DefaultMQProducer(rpcHook);
        producer.setNamesrvAddr(config.getNamesrvAddr());
        producer.setInstanceName(createUniqInstance(config.getNamesrvAddr()));
        producer.setProducerGroup(config.getGroupId());
        producer.setLanguage(LanguageCode.JAVA);
        if (config.getMaxMessageSize() != null && config.getMaxMessageSize() > 0) {
            producer.setMaxMessageSize(config.getMaxMessageSize());
        }
        if (config.getSendMsgTimeout() != null && config.getMaxMessageSize() > 0) {
            producer.setSendMsgTimeout(config.getSendMsgTimeout());
        }
        return producer;
    }

    private static DefaultMQAdminExt startMQAdminTool(RocketMqBaseConfiguration config)
            throws MQClientException {
        DefaultMQAdminExt admin;
        if (config.isAclEnable()) {
            admin =
                    new DefaultMQAdminExt(
                            new AclClientRPCHook(
                                    new SessionCredentials(
                                            config.getAccessKey(), config.getSecretKey())));
        } else {
            admin = new DefaultMQAdminExt();
        }
        admin.setNamesrvAddr(config.getNamesrvAddr());
        admin.setAdminExtGroup(config.getGroupId());
        admin.setInstanceName(createUniqInstance(config.getNamesrvAddr()));
        admin.start();
        return admin;
    }

    /** Create rocketMq topic */
    public static void createTopic(RocketMqBaseConfiguration config, TopicConfig topicConfig) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(config);
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            HashMap<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Set<String> clusterNameSet = clusterAddrTable.keySet();
            for (String clusterName : clusterNameSet) {
                Set<String> masterSet =
                        CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
                }
            }
        } catch (Exception e) {
            throw new RocketMqConnectorException(RocketMqConnectorErrorCode.CREATE_TOPIC_ERROR, e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }

    /** check topic exist */
    public static boolean topicExist(RocketMqBaseConfiguration config, String topic) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        boolean foundTopicRouteInfo = false;
        try {
            defaultMQAdminExt = startMQAdminTool(config);
            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
            if (topicRouteData != null) {
                foundTopicRouteInfo = true;
            }
        } catch (Exception e) {
            if (e instanceof MQClientException) {
                if (((MQClientException) e).getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                    foundTopicRouteInfo = false;
                } else {
                    throw new RocketMqConnectorException(
                            RocketMqConnectorErrorCode.TOPIC_NOT_EXIST_ERROR, e);
                }
            } else {
                throw new RocketMqConnectorException(
                        RocketMqConnectorErrorCode.TOPIC_NOT_EXIST_ERROR, e);
            }
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return foundTopicRouteInfo;
    }

    /** Get topic offsets */
    public static List<Map<MessageQueue, TopicOffset>> offsetTopics(
            RocketMqBaseConfiguration config, List<String> topics) {
        List<Map<MessageQueue, TopicOffset>> offsets = Lists.newArrayList();
        DefaultMQAdminExt adminClient = null;
        try {
            adminClient = RocketMqAdminUtil.startMQAdminTool(config);
            for (String topic : topics) {
                TopicStatsTable topicStatsTable = adminClient.examineTopicStats(topic);
                offsets.add(topicStatsTable.getOffsetTable());
            }
            return offsets;
        } catch (MQClientException
                | MQBrokerException
                | RemotingException
                | InterruptedException e) {
            throw new RocketMqConnectorException(
                    RocketMqConnectorErrorCode.GET_MIN_AND_MAX_OFFSETS_ERROR, e);
        } finally {
            if (adminClient != null) {
                adminClient.shutdown();
            }
        }
    }

    /** Flat topics offsets */
    public static Map<MessageQueue, TopicOffset> flatOffsetTopics(
            RocketMqBaseConfiguration config, List<String> topics) {
        Map<MessageQueue, TopicOffset> messageQueueTopicOffsets = Maps.newConcurrentMap();
        offsetTopics(config, topics)
                .forEach(
                        offsetTopic -> {
                            messageQueueTopicOffsets.putAll(offsetTopic);
                        });
        return messageQueueTopicOffsets;
    }

    /** Search offsets by timestamp */
    public static Map<MessageQueue, Long> searchOffsetsByTimestamp(
            RocketMqBaseConfiguration config,
            Collection<MessageQueue> messageQueues,
            Long timestamp) {
        Map<MessageQueue, Long> offsets = Maps.newConcurrentMap();
        DefaultMQAdminExt adminClient = null;
        try {
            adminClient = RocketMqAdminUtil.startMQAdminTool(config);
            for (MessageQueue messageQueue : messageQueues) {
                long offset = adminClient.searchOffset(messageQueue, timestamp);
                offsets.put(messageQueue, offset);
            }
            return offsets;
        } catch (MQClientException e) {
            throw new RocketMqConnectorException(
                    RocketMqConnectorErrorCode.GET_CONSUMER_GROUP_OFFSETS_TIMESTAMP_ERROR, e);
        } finally {
            if (adminClient != null) {
                adminClient.shutdown();
            }
        }
    }

    /** Get consumer group offset */
    public static Map<MessageQueue, Long> currentOffsets(
            RocketMqBaseConfiguration config,
            List<String> topics,
            Set<MessageQueue> messageQueues) {
        // Get consumer group offset
        DefaultMQAdminExt adminClient = null;
        try {
            adminClient = RocketMqAdminUtil.startMQAdminTool(config);
            Map<MessageQueue, OffsetWrapper> consumerOffsets = Maps.newConcurrentMap();
            for (String topic : topics) {
                ConsumeStats consumeStats =
                        adminClient.examineConsumeStats(config.getGroupId(), topic);
                consumerOffsets.putAll(consumeStats.getOffsetTable());
            }
            return consumerOffsets.keySet().stream()
                    .filter(messageQueue -> messageQueues.contains(messageQueue))
                    .collect(
                            Collectors.toMap(
                                    messageQueue -> messageQueue,
                                    messageQueue ->
                                            consumerOffsets.get(messageQueue).getConsumerOffset()));
        } catch (MQClientException
                | MQBrokerException
                | RemotingException
                | InterruptedException e) {
            if (e instanceof MQClientException) {
                if (((MQClientException) e).getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                    return Collections.emptyMap();
                } else {
                    throw new RocketMqConnectorException(
                            RocketMqConnectorErrorCode.GET_CONSUMER_GROUP_OFFSETS_ERROR, e);
                }
            } else {
                throw new RocketMqConnectorException(
                        RocketMqConnectorErrorCode.GET_CONSUMER_GROUP_OFFSETS_ERROR, e);
            }
        } finally {
            if (adminClient != null) {
                adminClient.shutdown();
            }
        }
    }
}
