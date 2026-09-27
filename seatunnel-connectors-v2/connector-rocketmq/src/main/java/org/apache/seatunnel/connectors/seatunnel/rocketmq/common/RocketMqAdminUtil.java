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
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

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

import lombok.extern.slf4j.Slf4j;

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
@Slf4j
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

    /**
     * Get consumer group offset.
     *
     * <p>An empty result means the lookup succeeded and this group has committed nothing for the
     * requested queues. A failed lookup is never reported that way: callers such as {@code
     * RocketMqSourceSplitEnumerator}'s {@code CONSUME_FROM_GROUP_OFFSETS} branch read an empty map
     * as "this group has committed nothing" and rewind to the first offset, so mapping a failure
     * onto that answer would silently re-deliver a whole topic.
     *
     * <p>{@code TOPIC_NOT_EXIST} (code 17) needs care, because it is the only failure code this
     * call can raise for a route problem and it covers two very different situations. See {@link
     * #currentOffsets(DefaultMQAdminExt, String, List, Set)} for how they are told apart.
     */
    public static Map<MessageQueue, Long> currentOffsets(
            RocketMqBaseConfiguration config,
            List<String> topics,
            Set<MessageQueue> messageQueues) {
        DefaultMQAdminExt adminClient = null;
        try {
            adminClient = RocketMqAdminUtil.startMQAdminTool(config);
            return currentOffsets(adminClient, config.getGroupId(), topics, messageQueues);
        } catch (MQClientException e) {
            throw new RocketMqConnectorException(
                    RocketMqConnectorErrorCode.GET_CONSUMER_GROUP_OFFSETS_ERROR, e);
        } finally {
            if (adminClient != null) {
                adminClient.shutdown();
            }
        }
    }

    /**
     * Collects committed group offsets through an already-started admin client. Package-private so
     * that tests can drive {@link DefaultMQAdminExt#examineConsumeStats} deterministically.
     *
     * <p>{@code examineConsumeStats} resolves its route solely from the group's auto-generated
     * retry topic, never from the requested topic, so a {@code TOPIC_NOT_EXIST} response always
     * refers to that retry topic. It is raised both when the group has never registered, in which
     * case the retry topic has not been created yet, and when a route has been lost, in which case
     * the offsets exist but are briefly unreadable. Answering "nothing committed" is correct for
     * the first and causes a full replay for the second.
     *
     * <p>The two are separated by re-resolving the requested topic. The name server removes routes
     * per broker rather than per topic, so an outage takes the requested topic's route along with
     * the retry topic's, while a group that has simply never registered leaves it intact. A group
     * also cannot commit an offset without first registering, which is what creates the retry
     * topic, so a missing retry topic on a healthy name server implies nothing was committed.
     */
    static Map<MessageQueue, Long> currentOffsets(
            DefaultMQAdminExt adminClient,
            String groupId,
            List<String> topics,
            Set<MessageQueue> messageQueues) {
        Map<MessageQueue, OffsetWrapper> consumerOffsets = Maps.newConcurrentMap();
        for (String topic : topics) {
            try {
                ConsumeStats consumeStats = adminClient.examineConsumeStats(groupId, topic);
                consumerOffsets.putAll(consumeStats.getOffsetTable());
            } catch (MQClientException e) {
                if (e.getResponseCode() == ResponseCode.TOPIC_NOT_EXIST
                        && topicRouteAvailable(adminClient, topic)) {
                    // The retry topic is per group, not per topic, so this applies to every topic
                    // in the request and the whole lookup is legitimately empty.
                    //
                    // Returning here discards anything consumerOffsets has already collected for
                    // earlier topics, and topics is a supported multi-topic list
                    // (RocketMqSourceOptions.TOPICS). That is safe only under the invariant
                    // above: a group cannot commit an offset for any topic without first
                    // registering, and registering is what creates the retry topic, so a missing
                    // retry topic means no topic in the list has committed anything and the map
                    // is necessarily still empty here. If that ever stops holding, this has to
                    // become a continue that keeps the earlier offsets, otherwise a later
                    // cold-start topic silently rewinds the topics already read.
                    log.warn(
                            "Consumer group {} has no retry topic yet, so it has never registered "
                                    + "and has committed nothing. Topic {} still resolves, so this "
                                    + "is not a route outage.",
                            groupId,
                            topic);
                    return Collections.emptyMap();
                }
                throw new RocketMqConnectorException(
                        RocketMqConnectorErrorCode.GET_CONSUMER_GROUP_OFFSETS_ERROR, e);
            } catch (MQBrokerException | RemotingException e) {
                throw new RocketMqConnectorException(
                        RocketMqConnectorErrorCode.GET_CONSUMER_GROUP_OFFSETS_ERROR, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RocketMqConnectorException(
                        RocketMqConnectorErrorCode.GET_CONSUMER_GROUP_OFFSETS_ERROR, e);
            }
        }
        return consumerOffsets.keySet().stream()
                .filter(messageQueue -> messageQueues.contains(messageQueue))
                .collect(
                        Collectors.toMap(
                                messageQueue -> messageQueue,
                                messageQueue ->
                                        consumerOffsets.get(messageQueue).getConsumerOffset()));
    }

    /**
     * Reports whether the name server can still resolve a route for {@code topic}. Any failure is
     * reported as unavailable, since the caller only uses this to decide whether a route problem is
     * broker wide.
     */
    private static boolean topicRouteAvailable(DefaultMQAdminExt adminClient, String topic) {
        try {
            return adminClient.examineTopicRouteInfo(topic) != null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (MQClientException | RemotingException e) {
            return false;
        }
    }
}
