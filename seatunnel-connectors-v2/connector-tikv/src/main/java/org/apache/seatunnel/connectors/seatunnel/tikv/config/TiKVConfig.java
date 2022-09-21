package org.apache.seatunnel.connectors.seatunnel.tikv.config;

import java.io.Serializable;

/**
 * @author XuJiaWei
 * @since 2022-09-15 18:15
 */
public class TiKVConfig implements Serializable {

    public static final String NAME = "TiKV";

    public static final String HOST = "host";

    public static final String PD_PORT = "port";

    public static final String DATA_TYPE = "data_type";

    public static final String KEY = "key";

    public static final String KEY_PATTERN = "keys";

    public static final String LIMIT = "limit";

    public static final String FORMAT = "format";

    public static final Integer LIMIT_DEFAULT = 10_000;

}
