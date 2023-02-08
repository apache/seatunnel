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
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.HdfsStorage;

import java.util.Map;

public class HdfsFileStorageInstance {
    private HdfsFileStorageInstance() {
        throw new IllegalStateException("Utility class");
    }

    private static HdfsStorage HDFS_STORAGE;
    private static final Object LOCK = new Object();

    public static boolean isFsNull() {
        return HDFS_STORAGE == null;
    }

    public static HdfsStorage getHdfsStorage() {
        return HDFS_STORAGE;
    }

    public static HdfsStorage getOrCreateStorage(Map<String, String> config) throws CheckpointStorageException {
        if (null != HDFS_STORAGE) {
            return HDFS_STORAGE;
        }
        synchronized (LOCK) {
            if (null != HDFS_STORAGE) {
                return HDFS_STORAGE;
            }
            HDFS_STORAGE = new HdfsStorage(config);
            return HDFS_STORAGE;
        }
    }

}
