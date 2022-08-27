package org.apache.seatunnel.connectors.seatunnel.druid.config;

import java.io.Serializable;

/**
 * guanbo
 */
public class DruidSourceConfig implements Serializable {
    public static final String URL = "url";
    public static final String DATASOURCE = "datasource";
    public static final String START_TIMESTAMP = "start_date";
    public static final String END_TIMESTAMP = "end_date";
    public static final String COLUMNS = "columns";
}
