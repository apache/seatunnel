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
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;

import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MaxcomputeUtil {
    public static Table getTable(ReadonlyConfig readonlyConfig) {
        Odps odps = getOdps(readonlyConfig);
        return odps.tables().get(readonlyConfig.get(MaxcomputeBaseOptions.TABLE_NAME));
    }

    public static TableTunnel getTableTunnel(ReadonlyConfig readonlyConfig) {
        Odps odps = getOdps(readonlyConfig);
        TableTunnel tableTunnel = new TableTunnel(odps);
        if (StringUtils.isNotEmpty(readonlyConfig.get(MaxcomputeBaseOptions.TUNNEL_ENDPOINT))) {
            tableTunnel.setEndpoint(readonlyConfig.get(MaxcomputeBaseOptions.TUNNEL_ENDPOINT));
        }
        return tableTunnel;
    }

    public static Odps getOdps(ReadonlyConfig readonlyConfig) {
        Account account =
                new AliyunAccount(
                        readonlyConfig.get(MaxcomputeBaseOptions.ACCESS_ID),
                        readonlyConfig.get(MaxcomputeBaseOptions.ACCESS_KEY));
        Odps odps = new Odps(account);
        odps.setEndpoint(readonlyConfig.get(MaxcomputeBaseOptions.ENDPOINT));
        odps.setDefaultProject(readonlyConfig.get(MaxcomputeBaseOptions.PROJECT));
        return odps;
    }

    public static TableTunnel.DownloadSession getDownloadSession(ReadonlyConfig readonlyConfig) {
        TableTunnel tunnel = getTableTunnel(readonlyConfig);
        TableTunnel.DownloadSession session;
        try {
            if (readonlyConfig.getOptional(MaxcomputeBaseOptions.PARTITION_SPEC).isPresent()) {
                PartitionSpec partitionSpec =
                        new PartitionSpec(readonlyConfig.get(MaxcomputeBaseOptions.PARTITION_SPEC));
                session =
                        buildDownloadSession(
                                tunnel,
                                readonlyConfig.get(MaxcomputeBaseOptions.PROJECT),
                                readonlyConfig.get(MaxcomputeBaseOptions.TABLE_NAME),
                                partitionSpec);
            } else {
                session =
                        buildDownloadSession(
                                tunnel,
                                readonlyConfig.get(MaxcomputeBaseOptions.PROJECT),
                                readonlyConfig.get(MaxcomputeBaseOptions.TABLE_NAME),
                                null);
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
                        buildDownloadSession(
                                tunnel,
                                tablePath.getDatabaseName(),
                                tablePath.getTableName(),
                                partition);
            } else {
                session =
                        buildDownloadSession(
                                tunnel,
                                tablePath.getDatabaseName(),
                                tablePath.getTableName(),
                                null);
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

    private static TableTunnel.DownloadSession buildDownloadSession(
            TableTunnel tunnel, String projectName, String tableName, PartitionSpec partitionSpec)
            throws TunnelException {
        return tunnel.buildDownloadSession(projectName, tableName)
                .setSchemaName(tunnel.getConfig().getOdps().getCurrentSchema())
                .setPartitionSpec(partitionSpec)
                .build();
    }
}
