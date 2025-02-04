package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;

import java.util.Map;

public class AmazonDynamoDBSourceOptions extends AmazonDynamoDBBaseOptions {

    public static final Option<Integer> SCAN_ITEM_LIMIT =
            Options.key("scan_item_limit")
                    .intType()
                    .defaultValue(1)
                    .withDescription("number of item each scan request should return");

    public static final Option<Integer> PARALLEL_SCAN_THREADS =
            Options.key("parallel_scan_threads")
                    .intType()
                    .defaultValue(2)
                    .withDescription("number of logical segments for parallel scan");

    public static final Option<Map<String, Object>> SCHEMA = TableSchemaOptions.SCHEMA;
}
