package org.apache.seatunnel.connectors.seatunnel.hive.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class HiveSourceOptions extends HiveBaseOptions {

    public static final Option<Boolean> USE_REGEX =
            Options.key("use_regex")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Use regular expression for `table_name` matching. "
                                    + "When set to true, the `table_name` will be treated as a regex pattern and can match multiple tables.");

    public static final Option<List<String>> READ_PARTITIONS =
            Options.key("read_partitions")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The partitions that the user want to read");

    public static final Option<List<String>> READ_COLUMNS =
            Options.key("read_columns")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The columns list that the user want to read");
}
