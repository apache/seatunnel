package org.apache.seatunnel.connectors.seatunnel.druid.config;

import java.io.Serializable;

/**
 * guanbo
 */
public class DruidSinkConfig implements Serializable {
    public static final String COORDINATOR_URL = "coordinator_url";
    public static final String DATASOURCE = "datasource";
    public static final String TIMESTAMP_COLUMN = "timestamp_column";
    public static final String TIMESTAMP_FORMAT = "timestamp_format";
    public static final String TIMESTAMP_MISSING_VALUE = "timestamp_missing_value";
    public static final String COLUMNS = "columns";

}
