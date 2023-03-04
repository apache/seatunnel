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

package org.apache.seatunnel.connectors.seatunnel.kafka.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.Config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is a KafkaCatalog implementation.
 *
 * <p>In kafka the database and table both are the topic name.
 */
public class KafkaCatalog implements Catalog {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCatalog.class);
    private final String catalogName;
    private final String bootstrapServers;
    private final String defaultTopic;

    private AdminClient adminClient;

    public KafkaCatalog(String catalogName, String defaultTopic, String bootstrapServers) {
        this.catalogName = checkNotNull(catalogName, "catalogName cannot be null");
        this.bootstrapServers = checkNotNull(bootstrapServers, "bootstrapServers cannot be null");
        this.defaultTopic = checkNotNull(defaultTopic, "defaultTopic cannot be null");
    }

    @Override
    public void open() throws CatalogException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        adminClient = AdminClient.create(properties);
        try {
            TopicDescription topicDescription = getTopicDescription(defaultTopic);
            if (topicDescription == null) {
                throw new DatabaseNotExistException(catalogName, defaultTopic);
            }
            LOGGER.info(
                    "Catalog {} is established connection to {}, the default database is {}",
                    catalogName,
                    bootstrapServers,
                    topicDescription.name());
        } catch (DatabaseNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Catalog : %s establish connection to %s error",
                            catalogName, bootstrapServers),
                    e);
        }
    }

    @Override
    public void close() throws CatalogException {
        adminClient.close();
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultTopic;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkNotNull(databaseName, "databaseName cannot be null");
        try {
            TopicDescription topicDescription = getTopicDescription(databaseName);
            return topicDescription != null;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Catalog : %s check database : %s exists error",
                            catalogName, databaseName),
                    e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topics = listTopicsResult.names().get();
            return Lists.newArrayList(topics);
        } catch (InterruptedException | ExecutionException e) {
            throw new CatalogException(
                    String.format("Listing database in catalog %s error", catalogName), e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
        return Lists.newArrayList(databaseName);
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        return databaseExists(tablePath.getDatabaseName());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        checkNotNull(tablePath, "tablePath cannot be null");
        TopicDescription topicDescription;
        try {
            topicDescription = getTopicDescription(tablePath.getTableName());
            if (topicDescription == null) {
                throw new TableNotExistException(catalogName, tablePath);
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new CatalogException(
                    String.format("Catalog : %s get table : %s error", catalogName, tablePath), e);
        }
        TableIdentifier tableIdentifier =
                TableIdentifier.of(
                        catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
        // todo: Set the schema of the table?
        TableSchema tableSchema = TableSchema.builder().build();
        return CatalogTable.of(
                tableIdentifier,
                tableSchema,
                buildConnectorOptions(topicDescription),
                Collections.emptyList(),
                "");
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        if (tableExists(tablePath)) {
            throw new TableAlreadyExistException(catalogName, tablePath);
        }
        Map<String, String> options = table.getOptions();
        int partitionNumber = Integer.parseInt(options.get(Config.PARTITION.key()));
        short replicationFactor = Short.parseShort(options.get(Config.REPLICATION_FACTOR));
        NewTopic newTopic =
                new NewTopic(tablePath.getTableName(), partitionNumber, replicationFactor);
        CreateTopicsResult createTopicsResult =
                adminClient.createTopics(Lists.newArrayList(newTopic));
        try {
            createTopicsResult.all().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new CatalogException(
                    String.format(
                            "Catalog : %s create table : %s error",
                            catalogName, tablePath.getFullName()),
                    e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        DeleteTopicsResult deleteTopicsResult =
                adminClient.deleteTopics(Lists.newArrayList(tablePath.getTableName()));
        try {
            deleteTopicsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new CatalogException(
                    String.format(
                            "Catalog : %s drop table : %s error",
                            catalogName, tablePath.getFullName()),
                    e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        // todo: We cannot create topic here, since we don't know the partition number and
        // replication factor.
        throw new UnsupportedOperationException("Kafka catalog does not support create database");
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        // todo:
        dropTable(tablePath, ignoreIfNotExists);
    }

    private TopicDescription getTopicDescription(String topicName)
            throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult =
                adminClient.describeTopics(Lists.newArrayList(topicName));
        KafkaFuture<TopicDescription> topicDescriptionKafkaFuture =
                describeTopicsResult.topicNameValues().get(topicName);
        return topicDescriptionKafkaFuture.get();
    }

    private Map<String, String> buildConnectorOptions(TopicDescription topicDescription) {
        String topicName = topicDescription.name();
        List<TopicPartitionInfo> partitions = topicDescription.partitions();
        List<Node> replicas = partitions.get(0).replicas();
        // todo: Do we need to support partition has different replication factor?
        Map<String, String> options = new HashMap<>();
        options.put(Config.BOOTSTRAP_SERVERS.key(), bootstrapServers);
        options.put(Config.TOPIC.key(), topicName);
        options.put(Config.PARTITION.key(), String.valueOf(partitions.size()));
        options.put(Config.REPLICATION_FACTOR, String.valueOf(replicas.size()));
        return options;
    }
}
