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

package org.apache.seatunnel.engine.checkpoint.storage.hdfs.common;

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public abstract class AbstractConfiguration {

    protected static final String HDFS_IMPL_KEY = "impl";

    /**
     * check the configuration keys
     *
     * @param config configuration
     * @param keys  keys
     */
    void checkConfiguration(Map<String, String> config, String... keys) {
        for (String key : keys) {
            if (!config.containsKey(key) || null == config.get(key)) {
                throw new IllegalArgumentException(key + " is required");
            }
        }
    }

    public abstract Configuration buildConfiguration(Map<String, String> config) throws CheckpointStorageException;

    /**
     * set extra options for configuration
     *
     * @param hadoopConf hadoop configuration
     * @param config     extra options
     * @param prefix     prefix of extra options
     */
    void setExtraConfiguration(Configuration hadoopConf, Map<String, String> config, String prefix) {
        config.forEach((k, v) -> {
            if (k.startsWith(prefix)) {
                hadoopConf.set(k, v);
            }
        });
    }

}
