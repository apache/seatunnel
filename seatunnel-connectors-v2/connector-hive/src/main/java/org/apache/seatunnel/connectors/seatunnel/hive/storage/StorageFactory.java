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

package org.apache.seatunnel.connectors.seatunnel.hive.storage;

public class StorageFactory {

    public static Storage getStorageType(String hiveSdLocation) {
        if (hiveSdLocation.startsWith(StorageType.S3.name().toLowerCase())) {
            return new S3Storage();
        } else if (hiveSdLocation.startsWith(StorageType.OSS.name().toLowerCase())) {
            return new OSSStorage();
        } else if (hiveSdLocation.startsWith(StorageType.COS.name().toLowerCase())) {
            return new COSStorage();
        } else if (hiveSdLocation.startsWith(StorageType.FILE.name().toLowerCase())) {
            // Currently used in e2e, When Hive uses local files as storage, "file:" needs to be
            // replaced with "file:/" to avoid being recognized as HDFS storage.
            return new HDFSStorage(hiveSdLocation.replace("file:", "file:/"));
        } else {
            return new HDFSStorage(hiveSdLocation);
        }
    }
}
