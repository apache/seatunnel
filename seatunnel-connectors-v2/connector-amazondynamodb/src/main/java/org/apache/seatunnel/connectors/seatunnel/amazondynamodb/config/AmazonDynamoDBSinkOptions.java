package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class AmazonDynamoDBSinkOptions extends AmazonDynamoDBBaseOptions {

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(25)
                    .withDescription("The batch size of Amazon DynamoDB");
}
