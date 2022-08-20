package org.apache.seatunnel.connectors.seatunnel.file.sink.ftp;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.connectors.seatunnel.file.sink.AbstractFileSink;
import org.apache.seatunnel.connectors.seatunnel.file.sink.ftp.util.FtpFileUtils;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.SinkFileSystemPlugin;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSink.class)
public class FtpFileSink extends AbstractFileSink {

    @Override
    public SinkFileSystemPlugin getSinkFileSystemPlugin() {
        return new FtpFileSinkPlugin();
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {

        super.prepare(pluginConfig);
        FtpFileUtils.FTP_HOST = String.valueOf(pluginConfig.getString("ftp_host"));
        FtpFileUtils.FTP_USERNAME = String.valueOf(pluginConfig.getString("ftp_username"));
        FtpFileUtils.FTP_PASSWORD = String.valueOf(pluginConfig.getString("ftp_password"));
        FtpFileUtils.FTP_PORT = pluginConfig.getInt("ftp_port");

        FtpFileUtils.getFTPClient();
    }
}
