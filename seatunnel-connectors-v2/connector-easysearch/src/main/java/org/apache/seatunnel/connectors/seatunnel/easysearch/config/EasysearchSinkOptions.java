package org.apache.seatunnel.connectors.seatunnel.easysearch.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class EasysearchSinkOptions extends EasysearchSinkCommonOptions {

    public static final Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription("Primary key fields used to generate the document `_id`");

    public static final Option<String> KEY_DELIMITER =
            Options.key("key_delimiter")
                    .stringType()
                    .defaultValue("_")
                    .withDescription(
                            "Delimiter for composite keys (\"_\" by default), e.g., \"$\" would result in document `_id` \"KEY1$KEY2$KEY3\".");

    public static final Option<Integer> MAX_BATCH_SIZE =
            Options.key("max_batch_size")
                    .intType()
                    .defaultValue(10)
                    .withDescription("batch bulk doc max size");

    public static final Option<Integer> MAX_RETRY_COUNT =
            Options.key("max_retry_count")
                    .intType()
                    .defaultValue(3)
                    .withDescription("one bulk request max try count");
}
