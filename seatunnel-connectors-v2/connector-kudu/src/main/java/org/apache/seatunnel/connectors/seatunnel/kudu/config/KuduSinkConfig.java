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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.kudu.client.SessionConfiguration;

import lombok.Getter;
import lombok.ToString;

import java.util.Locale;

@Getter
@ToString
public class KuduSinkConfig extends CommonConfig {

    private SaveMode saveMode;

    private String table;

    private SessionConfiguration.FlushMode flushMode;

    private int maxBufferSize;

    private int flushInterval;

    private boolean ignoreNotFound;

    private boolean ignoreDuplicate;

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

    public KuduSinkConfig(ReadonlyConfig config) {
        super(config);
        this.table = config.get(KuduSinkOptions.TABLE_NAME);
        this.saveMode = config.get(KuduSinkOptions.SAVE_MODE);
        this.flushMode = fromStrFlushMode(config.get(KuduSinkOptions.FLUSH_MODE));
        this.maxBufferSize = config.get(KuduSinkOptions.BATCH_SIZE);
        this.flushInterval = config.get(KuduSinkOptions.BUFFER_FLUSH_INTERVAL);
        this.ignoreNotFound = config.get(KuduSinkOptions.IGNORE_NOT_FOUND);
        this.ignoreDuplicate = config.get(KuduSinkOptions.IGNORE_DUPLICATE);
    }

    private SessionConfiguration.FlushMode fromStrFlushMode(String flushMode) {
        switch (flushMode.toUpperCase(Locale.ENGLISH)) {
            case "MANUAL_FLUSH":
                return SessionConfiguration.FlushMode.MANUAL_FLUSH;
            case "AUTO_FLUSH_BACKGROUND":
                return SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND;
            case "AUTO_FLUSH_SYNC":
            default:
                return SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC;
        }
    }
}
