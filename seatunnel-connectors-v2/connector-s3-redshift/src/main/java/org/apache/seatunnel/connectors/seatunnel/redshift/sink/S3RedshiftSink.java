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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink.BaseHdfsFileSink;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.redshift.commit.S3RedshiftSinkAggregatedCommitter;

import java.util.Optional;

public class S3RedshiftSink extends BaseHdfsFileSink {

    public S3RedshiftSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.pluginConfig = readonlyConfig;
        hadoopConf = S3HadoopConf.buildWithReadOnlyConfig(pluginConfig);
        if (pluginConfig.getOptional(FileBaseSinkOptions.HDFS_SITE_PATH).isPresent()) {
            hadoopConf.setHdfsSitePath(pluginConfig.get(FileBaseSinkOptions.HDFS_SITE_PATH));
        }
        if (pluginConfig.getOptional(FileBaseSinkOptions.REMOTE_USER).isPresent()) {
            hadoopConf.setRemoteUser(pluginConfig.get(FileBaseSinkOptions.REMOTE_USER));
        }
        if (pluginConfig.getOptional(FileBaseSinkOptions.KRB5_PATH).isPresent()) {
            hadoopConf.setKrb5Path(pluginConfig.get(FileBaseSinkOptions.KRB5_PATH));
        }
        if (pluginConfig.getOptional(FileBaseSinkOptions.KERBEROS_PRINCIPAL).isPresent()) {
            hadoopConf.setKerberosPrincipal(
                    pluginConfig.get(FileBaseSinkOptions.KERBEROS_PRINCIPAL));
        }
        if (pluginConfig.getOptional(FileBaseSinkOptions.KERBEROS_KEYTAB_PATH).isPresent()) {
            hadoopConf.setKerberosKeytabPath(
                    pluginConfig.get(FileBaseSinkOptions.KERBEROS_KEYTAB_PATH));
        }
        this.fileSinkConfig = new FileSinkConfig(pluginConfig, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public String getPluginName() {
        return "S3Redshift";
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return Optional.of(new S3RedshiftSinkAggregatedCommitter(hadoopConf, pluginConfig));
    }
}
