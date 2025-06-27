package org.apache.seatunnel.connectors.seatunnel.tablestore.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class TableStoreSinkOptions extends TableStoreCommonOptions {

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(25)
                    .withDescription(" Tablestore batch_size");
}
