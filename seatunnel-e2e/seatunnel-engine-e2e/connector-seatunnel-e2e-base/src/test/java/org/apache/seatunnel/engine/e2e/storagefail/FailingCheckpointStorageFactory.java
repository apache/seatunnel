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

package org.apache.seatunnel.engine.e2e.storagefail;

import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import com.google.auto.service.AutoService;

import java.util.Map;

/**
 * Registers {@link FailingCheckpointStorage} as the job's checkpoint storage for the engine E2E
 * test that injects a storage-side write failure into a periodic checkpoint.
 */
@AutoService(CheckpointStorageFactory.class)
public class FailingCheckpointStorageFactory implements CheckpointStorageFactory {

    /** Storage name referenced by the test's engine configuration. */
    public static final String IDENTIFIER = "FailingStorageTest";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public CheckpointStorage create(Map<String, String> configuration)
            throws CheckpointStorageException {
        return new FailingCheckpointStorage(configuration);
    }
}
