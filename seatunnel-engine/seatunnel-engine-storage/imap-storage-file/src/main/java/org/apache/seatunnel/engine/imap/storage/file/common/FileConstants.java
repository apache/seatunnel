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

package org.apache.seatunnel.engine.imap.storage.file.common;

import org.apache.hadoop.conf.Configuration;

public class FileConstants {

    public static final String DEFAULT_IMAP_NAMESPACE = "/seatunnel-imap";

    public static final String DEFAULT_IMAP_FILE_PATH_SPLIT = "/";

    public static final byte FILE_DATA_DELIMITER =  28;

    /**
     * init file storage
     */
    public interface FileInitProperties {

        /****************** The following are required parameters for initialization **************/

        String NAMESPACE_KEY = "namespace";

        /**
         * like OSS bucket name
         * It is used to distinguish data storage locations of different business.
         * Type: String
         */
        String BUSINESS_KEY = "businessName";

        /**
         * This parameter is primarily used for cluster isolation
         * we can use this to distinguish different cluster, like cluster1, cluster2
         * and this is also used to distinguish different business
         * <p>
         * Type: String
         */
        String CLUSTER_NAME = "clusterName";

        /**
         * We used hdfs api read/write file
         * so, used this storage need provide hdfs configuratio
         * <p>
         * Type:
         *
         * @see Configuration
         */
        String HDFS_CONFIG_KEY = "hdfsConfig";

    }
}
