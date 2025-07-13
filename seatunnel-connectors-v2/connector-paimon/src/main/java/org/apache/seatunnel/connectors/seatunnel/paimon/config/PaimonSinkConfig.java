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

package org.apache.seatunnel.connectors.seatunnel.paimon.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;

import org.apache.paimon.CoreOptions;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Getter
@Slf4j
public class PaimonSinkConfig extends PaimonConfig {

    private final SchemaSaveMode schemaSaveMode;
    private final DataSaveMode dataSaveMode;
    private final CoreOptions.ChangelogProducer changelogProducer;
    private final String changelogTmpPath;
    private final Boolean nonPrimaryKey;
    private final List<String> primaryKeys;
    private final List<String> partitionKeys;
    private final Map<String, String> writeProps;

    public PaimonSinkConfig(ReadonlyConfig readonlyConfig) {
        super(readonlyConfig);
        this.schemaSaveMode = readonlyConfig.get(PaimonSinkOptions.SCHEMA_SAVE_MODE);
        this.dataSaveMode = readonlyConfig.get(PaimonSinkOptions.DATA_SAVE_MODE);
        this.nonPrimaryKey = readonlyConfig.get(PaimonSinkOptions.NON_PRIMARY_KEY);
        this.primaryKeys = stringToList(readonlyConfig.get(PaimonSinkOptions.PRIMARY_KEYS), ",");
        if (this.nonPrimaryKey && !this.primaryKeys.isEmpty()) {
            String message =
                    String.format(
                            " `%s` will is empty when `%s`is true, but is %s",
                            PaimonSinkOptions.PRIMARY_KEYS.key(),
                            PaimonSinkOptions.NON_PRIMARY_KEY.key(),
                            this.primaryKeys);
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.NON_PRIMARY_KEY_CHECK_ERROR, message);
        }
        this.partitionKeys =
                stringToList(readonlyConfig.get(PaimonSinkOptions.PARTITION_KEYS), ",");
        this.writeProps = readonlyConfig.get(PaimonSinkOptions.WRITE_PROPS);
        this.changelogProducer =
                Stream.of(CoreOptions.ChangelogProducer.values())
                        .filter(
                                cp ->
                                        cp.toString()
                                                .equalsIgnoreCase(
                                                        writeProps.getOrDefault(
                                                                CoreOptions.CHANGELOG_PRODUCER
                                                                        .key(),
                                                                "")))
                        .findFirst()
                        .orElse(null);
        this.changelogTmpPath =
                writeProps.getOrDefault(
                        PaimonSinkOptions.CHANGELOG_TMP_PATH, System.getProperty("java.io.tmpdir"));
        checkConfig();
    }

    private void checkConfig() {
        if (this.primaryKeys.isEmpty() && "-1".equals(this.writeProps.get("bucket"))) {
            log.warn("Append only table currently do not support dynamic bucket");
        }
    }
}
