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

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.Properties;

/**
 * A {@link KafkaProducer} that allow resume transaction from transactionId
 */
@Slf4j
public class KafkaInternalProducer<K, V> extends KafkaProducer<K, V> {

    private static final String TRANSACTION_MANAGER_STATE_ENUM =
            "org.apache.kafka.clients.producer.internals.TransactionManager$State";
    private static final String PRODUCER_ID_AND_EPOCH_FIELD_NAME = "producerIdAndEpoch";
    private String transactionalId;

    public KafkaInternalProducer(Properties properties, String transactionId) {
        super(properties);
        this.transactionalId = transactionId;
    }

    @Override
    public void initTransactions() {
        setTransactionalId(this.transactionalId);
        super.initTransactions();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        super.beginTransaction();
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        super.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        super.abortTransaction();
    }

    public void setTransactionalId(String transactionalId) {
        if (!transactionalId.equals(this.transactionalId)) {
            Object transactionManager = getTransactionManager();
            synchronized (transactionManager) {
                ReflectionUtils.setField(transactionManager, "transactionalId", transactionalId);
                ReflectionUtils.setField(transactionManager, "currentState",
                        getTransactionManagerState("UNINITIALIZED"));
                this.transactionalId = transactionalId;
            }
        }
    }

    public short getEpoch() {
        Object transactionManager = getTransactionManager();
        Optional<Object> producerIdAndEpoch = ReflectionUtils.getField(transactionManager,
                PRODUCER_ID_AND_EPOCH_FIELD_NAME);
        return (short) ReflectionUtils.getField(producerIdAndEpoch.get(), "epoch").get();
    }

    public long getProducerId() {
        Object transactionManager = getTransactionManager();
        Object producerIdAndEpoch = ReflectionUtils.getField(transactionManager,
                PRODUCER_ID_AND_EPOCH_FIELD_NAME).get();
        return (long) ReflectionUtils.getField(producerIdAndEpoch, "producerId").get();
    }

    public void resumeTransaction(long producerId, short epoch) {

        log.info(
                "Attempting to resume transaction {} with producerId {} and epoch {}",
                transactionalId,
                producerId,
                epoch);

        Object transactionManager = getTransactionManager();
        synchronized (transactionManager) {
            Object topicPartitionBookkeeper =
                    ReflectionUtils.getField(transactionManager, transactionManager.getClass(),
                            "topicPartitionBookkeeper").get();

            transitionTransactionManagerStateTo(transactionManager, "INITIALIZING");
            ReflectionUtils.invoke(topicPartitionBookkeeper, "reset");

            ReflectionUtils.setField(
                    transactionManager, PRODUCER_ID_AND_EPOCH_FIELD_NAME,
                    createProducerIdAndEpoch(producerId, epoch));

            transitionTransactionManagerStateTo(transactionManager, "READY");

            transitionTransactionManagerStateTo(transactionManager, "IN_TRANSACTION");
            ReflectionUtils.setField(transactionManager, "transactionStarted", true);
        }
    }

    private static Object createProducerIdAndEpoch(long producerId, short epoch) {
        try {
            Field field =
                    TransactionManager.class.getDeclaredField(PRODUCER_ID_AND_EPOCH_FIELD_NAME);
            Class<?> clazz = field.getType();
            Constructor<?> constructor = clazz.getDeclaredConstructor(Long.TYPE, Short.TYPE);
            constructor.setAccessible(true);
            return constructor.newInstance(producerId, epoch);
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException |
                 NoSuchFieldException | NoSuchMethodException e) {
            throw new KafkaConnectorException(KafkaConnectorErrorCode.VERSION_INCOMPATIBLE,
                    "Incompatible KafkaProducer version", e);
        }
    }

    private Object getTransactionManager() {
        Optional<Object> transactionManagerOptional = ReflectionUtils.getField(this, KafkaProducer.class,
                "transactionManager");
        if (!transactionManagerOptional.isPresent()) {
            throw new KafkaConnectorException(KafkaConnectorErrorCode.GET_TRANSACTIONMANAGER_FAILED,
                    "Can't get transactionManager in KafkaProducer");
        }
        return transactionManagerOptional.get();
    }

    private static void transitionTransactionManagerStateTo(
            Object transactionManager, String state) {
        ReflectionUtils.invoke(transactionManager, "transitionTo", getTransactionManagerState(state));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Enum<?> getTransactionManagerState(String enumName) {
        try {
            Class<Enum> cl = (Class<Enum>) Class.forName(TRANSACTION_MANAGER_STATE_ENUM);
            return Enum.valueOf(cl, enumName);
        } catch (ClassNotFoundException e) {
            throw new KafkaConnectorException(KafkaConnectorErrorCode.VERSION_INCOMPATIBLE,
                    "Incompatible KafkaProducer version", e);
        }
    }

}
