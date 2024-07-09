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

package org.apache.seatunnel.connectors.seatunnel.emqx.client;

import org.apache.seatunnel.connectors.seatunnel.emqx.source.ClientMetadata;

import org.apache.commons.lang3.StringUtils;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttClientUtil {

    public static MqttClient createMqttClient(ClientMetadata metadata, int index)
            throws MqttException {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        if (StringUtils.isNotEmpty(metadata.getUsername())) {
            connOpts.setUserName(metadata.getUsername());
            connOpts.setPassword(metadata.getPassword().toCharArray());
        }
        if (metadata.getTopic() != null
                && (metadata.getTopic().startsWith("$share")
                        || metadata.getTopic().startsWith("$queue"))) {
            connOpts.setCleanSession(true);
        } else {
            connOpts.setCleanSession(metadata.isCleanSession());
        }
        connOpts.setAutomaticReconnect(true);
        MemoryPersistence persistence = new MemoryPersistence();
        MqttClient client =
                new MqttClient(
                        metadata.getBroker(), metadata.getClientId() + "_" + index, persistence);
        client.connect(connOpts);
        return client;
    }
}
