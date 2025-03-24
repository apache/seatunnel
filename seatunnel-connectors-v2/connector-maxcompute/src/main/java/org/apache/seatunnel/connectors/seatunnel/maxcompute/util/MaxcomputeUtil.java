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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.util;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;

import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ACCESS_ID;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PARTITION_SPEC;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.TABLE_NAME;

@Slf4j
public class MaxcomputeUtil {
    public static Table getTable(ReadonlyConfig readonlyConfig) {
        Odps odps = getOdps(readonlyConfig);
        return odps.tables().get(readonlyConfig.get(TABLE_NAME));
    }

    public static TableTunnel getTableTunnel(ReadonlyConfig readonlyConfig) {
        Odps odps = getOdps(readonlyConfig);
        return new TableTunnel(odps);
    }

    public static Odps getOdps(ReadonlyConfig readonlyConfig) {
        Account account =
                new AliyunAccount(readonlyConfig.get(ACCESS_ID), readonlyConfig.get(ACCESS_KEY));
        Odps odps = new Odps(account);
        odps.setEndpoint(readonlyConfig.get(ENDPOINT));
        odps.setDefaultProject(readonlyConfig.get(PROJECT));
        return odps;
    }

    public static TableTunnel.DownloadSession getDownloadSession(ReadonlyConfig readonlyConfig) {
        TableTunnel tunnel = getTableTunnel(readonlyConfig);
        TableTunnel.DownloadSession session;
        try {
            if (readonlyConfig.getOptional(PARTITION_SPEC).isPresent()) {
                PartitionSpec partitionSpec = new PartitionSpec(readonlyConfig.get(PARTITION_SPEC));
                session =
                        tunnel.createDownloadSession(
                                readonlyConfig.get(PROJECT),
                                readonlyConfig.get(TABLE_NAME),
                                partitionSpec);
            } else {
                session =
                        tunnel.createDownloadSession(
                                readonlyConfig.get(PROJECT), readonlyConfig.get(TABLE_NAME));
            }
        } catch (Exception e) {
            throw new MaxcomputeConnectorException(
                    CommonErrorCodeDeprecated.READER_OPERATION_FAILED, e);
        }
        return session;
    }

    public static TableTunnel.DownloadSession getDownloadSession(
            ReadonlyConfig readonlyConfig, TablePath tablePath, String partitionSpec) {
        TableTunnel tunnel = getTableTunnel(readonlyConfig);
        TableTunnel.DownloadSession session;
        try {
            if (StringUtils.isNotEmpty(partitionSpec)) {
                PartitionSpec partition = new PartitionSpec(partitionSpec);
                session =
                        tunnel.createDownloadSession(
                                tablePath.getDatabaseName(), tablePath.getTableName(), partition);
            } else {
                session =
                        tunnel.createDownloadSession(
                                tablePath.getDatabaseName(), tablePath.getTableName());
            }
        } catch (Exception e) {
            throw new MaxcomputeConnectorException(
                    CommonErrorCodeDeprecated.READER_OPERATION_FAILED, e);
        }
        return session;
    }

    public static Table parseTable(Odps odps, String projectName, String tableName) {
        try {
            Table table = odps.tables().get(projectName, tableName);
            table.reload();
            return table;
        } catch (Exception ex) {
            throw new MaxcomputeConnectorException(
                    CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED,
                    String.format(
                            "get table %s.%s info with exception, error:%s",
                            projectName, tableName, ex.getMessage()),
                    ex);
        }
    }
}
