package org.apache.seatunnel.connectors.seatunnel.tikv.config;

import lombok.Data;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;

/**
 * @author XuJiaWei
 * @since 2022-09-15 18:25
 */
@Data
public class TiKVParameters implements Serializable {
    /**
     * PD server host
     */
    public String host;
    /**
     * PD server port
     */
    public Integer pdPort;
    /**
     * TiKV server addresses
     */
    public String pdAddresses;

    private String startKey;

    private String endKey;
    private String keyField;

    private String keysPattern;

    private Integer limit;

    private TiKVDataType tikvDataType;

    public String getPdAddresses() {
        return this.getHost() + ":" + this.getPdPort();
    }

    public void initConfig(Config config) {
        // set host
        this.host = config.getString(TiKVConfig.HOST);
        // set port
        this.pdPort = config.getInt(TiKVConfig.PD_PORT);
        // set key
        if (config.hasPath(TiKVConfig.KEY)) {
            this.keyField = config.getString(TiKVConfig.KEY);
        }
        // set keysPattern
        if (config.hasPath(TiKVConfig.KEY_PATTERN)) {
            this.keysPattern = config.getString(TiKVConfig.KEY_PATTERN);
        }
        // default 10000
        if (config.hasPath(TiKVConfig.LIMIT)) {
            this.limit = TiKVConfig.LIMIT_DEFAULT;
        }

        // default KEY
        this.tikvDataType = tikvDataType.getDataType(config.getString(TiKVConfig.DATA_TYPE));
    }

}
