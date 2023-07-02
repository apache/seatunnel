/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.kafka;

import org.apache.seatunnel.engine.imap.storage.api.IMapStorage;
import org.apache.seatunnel.engine.imap.storage.api.IMapStorageFactory;
import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.kafka.config.KafkaConfigurationConstants;

import com.google.auto.service.AutoService;

import java.util.Map;

@AutoService(IMapStorageFactory.class)
public class IMapKafkaStorageFactory implements IMapStorageFactory {
    @Override
    public String factoryIdentifier() {
        return "kafka";
    }

    @Override
    public IMapStorage create(Map<String, Object> configuration) throws IMapStorageException {
        CheckConfigNull(configuration);
        IMapKafkaStorage iMapKafkaStorage = new IMapKafkaStorage();
        iMapKafkaStorage.initialize(configuration);
        return iMapKafkaStorage;
    }

    private void CheckConfigNull(Map<String, Object> configuration) {
        if (!configuration.containsKey(KafkaConfigurationConstants.KAFKA_BOOTSTRAP_SERVERS)) {
            throw new IllegalArgumentException(
                    KafkaConfigurationConstants.KAFKA_BOOTSTRAP_SERVERS + " is required");
        }
    }
}
