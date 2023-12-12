
package org.apache.seatunnel.connectors.seatunnel.obs.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import java.util.HashMap;

public class ObsConf extends HadoopConf {

    private static final String HDFS_IMPL = "org.apache.hadoop.fs.obs.OBSFileSystem";
    private static final String SCHEMA = "obs";

    @Override
    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    @Override
    public String getSchema() {
        return SCHEMA;
    }

    public ObsConf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    public static HadoopConf buildWithConfig(Config config) {
        HadoopConf hadoopConf = new ObsConf(config.getString(ObsConfig.BUCKET.key()));
        HashMap<String, String> obsOptions = new HashMap<>();
        obsOptions.put("fs.AbstractFileSystem.obs.impl", "org.apache.hadoop.fs.obs.OBS");
        obsOptions.put("fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");
        obsOptions.put("fs.obs.access.key", config.getString(ObsConfig.ACCESS_KEY.key()));
        obsOptions.put("fs.obs.secret.key", config.getString(ObsConfig.SECURITY_KEY.key()));
        obsOptions.put("fs.obs.endpoint", config.getString(ObsConfig.ENDPOINT.key()));
        hadoopConf.setExtraOptions(obsOptions);
        return hadoopConf;
    }
}
