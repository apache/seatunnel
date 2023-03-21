package org.apache.seatunnel.connectors.seatunnel.mongodb.config;

import java.io.Serializable;

/***
 * <p>Deprecated. Please use {@link MongoConnectorOptions} instead.
 */
public class MongoOptions implements Serializable {

    public static final String SINK_TRANSACTION_ENABLED = "sink.transaction.enable";

    public static final String SINK_FLUSH_ON_CHECKPOINT = "sink.flush.on-checkpoint";

    public static final String SINK_FLUSH_SIZE = "sink.flush.size";

    public static final String SINK_FLUSH_INTERVAL = "sink.flush.interval";
}
