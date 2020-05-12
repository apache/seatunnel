package io.github.interestinglab.waterdrop.common;

import io.github.interestinglab.waterdrop.config.Config;

import java.util.Properties;

public class PropertiesUtil {

    public static void setProperties(Config config, Properties properties,String prefix, boolean keepPrefix){
        config.entrySet().forEach(entry -> {
            String key = entry.getKey();
            Object value = entry.getValue().unwrapped();
            if (key.startsWith(prefix)) {
                if (keepPrefix){
                    properties.put(key,value);
                }else {
                    properties.put(key.substring(prefix.length()), value);
                }
            }
        });
    }
}
