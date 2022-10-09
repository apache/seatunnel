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

package org.apache.seatunnel.engine.checkpoint.storage.api;

import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import java.util.Map;

/**
 * All checkpoint storage plugins need to implement it
 */
public interface CheckpointStorageFactory extends Factory {

    /**
     * create storage plugin instance
     *
     * @param configuration storage system config params
     *                      key: storage system config key
     *                      value: storage system config value
     *                      e.g.
     *                      key: "FS_DEFAULT_NAME_KEY"
     *                      value: "fs.defaultFS"
     *                      return storage plugin instance
     */
    CheckpointStorage create(Map<String, String> configuration) throws CheckpointStorageException;
}
