package io.github.interestinglab.waterdrop.common;

import com.typesafe.config.waterdrop.Config;

import java.util.Properties;

/**
 * @author mr_xiong
 * @date 2019-07-12 18:25
 * @description
 */
public class PropertiesUtil {

    public static void setProperties(Config config, Properties properties,String prefix){
        config.entrySet().forEach(entry -> {
            String key = entry.getKey();
            Object value = entry.getValue().unwrapped();
            if (key.startsWith(prefix)) {
                properties.put(key.substring(prefix.length()), value);
            }
        });
    }
}
