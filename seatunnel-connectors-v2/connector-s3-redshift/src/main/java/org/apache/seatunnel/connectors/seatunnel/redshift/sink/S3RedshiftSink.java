package org.apache.seatunnel.connectors.seatunnel.redshift.sink;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink.BaseHdfsFileSink;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Conf;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Config;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.redshift.commit.S3RedshiftSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

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
        CheckResult checkResult = CheckConfigUtil.checkAllExists(pluginConfig, S3Config.S3_BUCKET.key(), S3RedshiftConfig.JDBC_URL.key(),
            S3RedshiftConfig.JDBC_USER.key(), S3RedshiftConfig.JDBC_PASSWORD.key(), S3RedshiftConfig.EXECUTE_SQL.key());
        if (!checkResult.isSuccess()) {
            throw new PrepareFailException(checkResult.getMsg(), PluginType.SINK, getPluginName());
        }
        this.pluginConfig = pluginConfig;
        hadoopConf = S3Conf.buildWithConfig(pluginConfig);
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>> createAggregatedCommitter() {
        return Optional.of(new S3RedshiftSinkAggregatedCommitter(hadoopConf, pluginConfig));
    }
}
