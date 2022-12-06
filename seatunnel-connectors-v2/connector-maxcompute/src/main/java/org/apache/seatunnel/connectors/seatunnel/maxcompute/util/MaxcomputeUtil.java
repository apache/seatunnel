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

import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ACCESS_ID;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.OVERWRITE;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PARTITION_SPEC;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.TABLE_NAME;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MaxcomputeUtil {
    public static Table getTable(Config pluginConfig) {
        Odps odps = getOdps(pluginConfig);
        Table table = odps.tables().get(pluginConfig.getString(TABLE_NAME.key()));
        return table;
    }

    public static TableTunnel getTableTunnel(Config pluginConfig) {
        Odps odps = getOdps(pluginConfig);
        TableTunnel tunnel = new TableTunnel(odps);
        return tunnel;
    }

    public static Odps getOdps(Config pluginConfig) {
        Account account = new AliyunAccount(pluginConfig.getString(ACCESS_ID.key()), pluginConfig.getString(ACCESS_KEY.key()));
        Odps odps = new Odps(account);
        odps.setEndpoint(pluginConfig.getString(ENDPOINT.key()));
        odps.setDefaultProject(pluginConfig.getString(PROJECT.key()));
        return odps;
    }

    public static TableTunnel.DownloadSession getDownloadSession(Config pluginConfig) {
        TableTunnel tunnel = getTableTunnel(pluginConfig);
        TableTunnel.DownloadSession session;
        try {
            if (pluginConfig.hasPath(PARTITION_SPEC.key())) {
                PartitionSpec partitionSpec = new PartitionSpec(pluginConfig.getString(PARTITION_SPEC.key()));
                session = tunnel.createDownloadSession(pluginConfig.getString(PROJECT.key()), pluginConfig.getString(TABLE_NAME.key()), partitionSpec);
            } else {
                session = tunnel.createDownloadSession(pluginConfig.getString(PROJECT.key()), pluginConfig.getString(TABLE_NAME.key()));
            }
        } catch (Exception e) {
            throw new MaxcomputeConnectorException(CommonErrorCode.READER_OPERATION_FAILED, e);
        }
        return session;
    }

    public static void initTableOrPartition(Config pluginConfig) {
        Boolean overwrite = OVERWRITE.defaultValue();
        if (pluginConfig.hasPath(OVERWRITE.key())) {
            overwrite = pluginConfig.getBoolean(OVERWRITE.key());
        }
        try {
            Table table = MaxcomputeUtil.getTable(pluginConfig);
            if (pluginConfig.hasPath(PARTITION_SPEC.key())) {
                PartitionSpec partitionSpec = new PartitionSpec(pluginConfig.getString(PARTITION_SPEC.key()));
                if (overwrite) {
                    try {
                        table.deletePartition(partitionSpec, true);
                    } catch (NullPointerException e) {
                        log.debug("NullPointerException when delete table partition");
                    }
                }
                table.createPartition(partitionSpec, true);
            } else {
                if (overwrite) {
                    try {
                        table.truncate();
                    } catch (NullPointerException e) {
                        log.debug("NullPointerException when truncate table");
                    }
                }
            }
        } catch (Exception e) {
            throw new MaxcomputeConnectorException(CommonErrorCode.READER_OPERATION_FAILED, e);
        }
    }
}
