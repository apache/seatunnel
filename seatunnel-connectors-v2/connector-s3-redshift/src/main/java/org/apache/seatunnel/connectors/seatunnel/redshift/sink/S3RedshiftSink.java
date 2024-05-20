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

package org.apache.seatunnel.connectors.seatunnel.redshift.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink.BaseHdfsFileSink;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3ConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.redshift.commit.S3RedshiftSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftJdbcConnectorException;

import com.google.auto.service.AutoService;

import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class S3RedshiftSink extends BaseHdfsFileSink {

    @Override
    public String getPluginName() {
        return "S3Redshift";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult checkResult =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        S3ConfigOptions.S3_BUCKET.key(),
                        S3ConfigOptions.S3A_AWS_CREDENTIALS_PROVIDER.key(),
                        S3RedshiftConfigOptions.JDBC_URL.key(),
                        S3RedshiftConfigOptions.JDBC_USER.key(),
                        S3RedshiftConfigOptions.JDBC_PASSWORD.key(),
                        S3RedshiftConfigOptions.EXECUTE_SQL.key());
        if (!checkResult.isSuccess()) {
            throw new S3RedshiftJdbcConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, checkResult.getMsg()));
        }
        this.pluginConfig = pluginConfig;
        hadoopConf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromConfig(pluginConfig));
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return Optional.of(new S3RedshiftSinkAggregatedCommitter(hadoopConf, pluginConfig));
    }
}
