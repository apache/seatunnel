package org.apache.seatunnel.connectors.seatunnel.mongodbv2.serde;

public enum TimestampFormat {
    /**
     * Options to specify TIMESTAMP/TIMESTAMP_WITH_LOCAL_ZONE format. It will parse TIMESTAMP in
     * "yyyy-MM-dd HH:mm:ss.s{precision}" format, TIMESTAMP_WITH_LOCAL_TIMEZONE in "yyyy-MM-dd
     * HH:mm:ss.s{precision}'Z'" and output in the same format.
     */
    SQL,

    /**
     * Options to specify TIMESTAMP/TIMESTAMP_WITH_LOCAL_ZONE format. It will parse TIMESTAMP in
     * "yyyy-MM-ddTHH:mm:ss.s{precision}" format, TIMESTAMP_WITH_LOCAL_TIMEZONE in
     * "yyyy-MM-ddTHH:mm:ss.s{precision}'Z'" and output in the same format.
     */
    ISO_8601

}
