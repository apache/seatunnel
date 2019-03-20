package io.github.interestinglab.waterdrop.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import java.util.LinkedHashMap;
import java.util.Map;

public class TypesafeConfigUtils {

    /**
     * Extract sub config with fixed prefix
     * @param source
     * @param prefix
     * @param keepPrefix
     * */
    public static Config extractSubConfig(Config source, String prefix, boolean keepPrefix) {

        // use LinkedHashMap to keep insertion order
        Map<String, String> values = new LinkedHashMap<>();

        for (Map.Entry<String, ConfigValue> entry : source.entrySet()) {
            final String key = entry.getKey();
            final String value = String.valueOf(entry.getValue().unwrapped());

            if (key.startsWith(prefix)) {

                if (keepPrefix) {
                    values.put(key, value);
                } else {
                    values.put(key.substring(prefix.length()), value);
                }
            }
        }

        return ConfigFactory.parseMap(values);
    }

    /**
     * Check if config with specific prefix exists
     * */
    public static boolean hasSubConfig(Config source, String prefix) {

        boolean hasConfig = false;

        for (Map.Entry<String, ConfigValue> entry : source.entrySet()) {
            final String key = entry.getKey();

            if (key.startsWith(prefix)) {
                hasConfig = true;
                break;
            }
        }

        return hasConfig;
    }

    public static Config extractSubConfigThrowable(Config source, String prefix, boolean keepPrefix) {

        Config config = extractSubConfig(source, prefix, keepPrefix);

        if (config.isEmpty()) {
            throw new ConfigRuntimeException("config is empty");
        }

        return config;
    }
}
