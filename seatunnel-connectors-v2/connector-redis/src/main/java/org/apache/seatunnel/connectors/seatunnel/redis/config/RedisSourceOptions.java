package org.apache.seatunnel.connectors.seatunnel.redis.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class RedisSourceOptions extends RedisBaseOptions {
    public enum HashKeyParseMode {
        ALL,
        KV;
    }

    public static final Option<HashKeyParseMode> HASH_KEY_PARSE_MODE =
            Options.key("hash_key_parse_mode")
                    .enumType(HashKeyParseMode.class)
                    .defaultValue(HashKeyParseMode.ALL)
                    .withDescription(
                            "hash key parse mode, support all or kv, default value is all");
}
