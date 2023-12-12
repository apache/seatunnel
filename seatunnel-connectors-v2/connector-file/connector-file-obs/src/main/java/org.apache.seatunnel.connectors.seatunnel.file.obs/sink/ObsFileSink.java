package org.apache.seatunnel.connectors.seatunnel.obs.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.BaseFileSink;
import org.apache.seatunnel.connectors.seatunnel.obs.config.ObsConf;
import org.apache.seatunnel.connectors.seatunnel.obs.config.ObsConfig;
import org.apache.seatunnel.connectors.seatunnel.obs.exception.ObsConnectorException;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSink.class)
public class ObsFileSink extends BaseFileSink {
    @Override
    public String getPluginName() {
        return "ObsFile";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        super.prepare(pluginConfig);
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        ObsConfig.FILE_PATH.key(),
                        ObsConfig.ENDPOINT.key(),
                        ObsConfig.ACCESS_KEY.key(),
                        ObsConfig.SECURITY_KEY.key(),
                        ObsConfig.BUCKET.key());
        if (!result.isSuccess()) {
            throw new ObsConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        hadoopConf = ObsConf.buildWithConfig(pluginConfig);
    }
}
