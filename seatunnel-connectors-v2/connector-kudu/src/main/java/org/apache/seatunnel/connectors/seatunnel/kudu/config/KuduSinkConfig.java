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

package org.apache.seatunnel.connectors.seatunnel.kudu.config;

import lombok.Data;
import lombok.NonNull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class KuduSinkConfig {

    private static final String KUDU_SAVE_MODE = "save_mode";



    private static final String KUDU_MASTER = "kudu_master";
    private static final String KUDU_TABLE_NAME = "kudu_table";

    private SaveMode saveMode = SaveMode.APPEND;

    private String kuduMaster;

    /**
     * Specifies the name of the table
     */
    private String kuduTableName;




    public enum SaveMode {
        APPEND(),
        OVERWRITE();

        public static SaveMode fromStr(String str) {
            if ("overwrite".equals(str)) {
                return OVERWRITE;
            } else {
                return APPEND;
            }
        }
    }

    public KuduSinkConfig(@NonNull Config pluginConfig) {



        this.saveMode = StringUtils.isBlank(pluginConfig.getString(KUDU_SAVE_MODE)) ? SaveMode.APPEND : SaveMode.fromStr(pluginConfig.getString(KUDU_SAVE_MODE));

        this.kuduMaster = pluginConfig.getString(KUDU_MASTER);
        this.kuduTableName = pluginConfig.getString(KUDU_TABLE_NAME);


    }
}
