package io.github.interestinglab.waterdrop.config;


import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.File;

import io.github.interestinglab.waterdrop.config.SparkDriverSettings;


public class ExposeSparkDriverConf {

    public static List<String> splitKey(String key) {
        List<String> keys = new LinkedList<>();
        int index = 0;
        for (int i = 0; i < key.length(); i++) {
            char symbol = key.charAt(i);
            if (symbol >= 'A' && symbol <= 'Z') {
                keys.add(key.substring(index, i).toLowerCase());
                index = i;
            }
        }

        keys.add(key.substring(index).toLowerCase());

        if (keys.get(0).equals("extra")) {
            return keys.subList(1, keys.size());
        }
        return keys;
    }

    public static void main(String[] args) throws Exception {
        Config appConfig = ConfigFactory.parseFile(new File(args[0]))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

        String driverPrefix = "spark.driver.";
        Config sparkConfig = appConfig.getConfig("spark");

        if (!TypesafeConfigUtils.hasSubConfig(sparkConfig, driverPrefix)) {
            System.out.println("");
        } else {
            Config sparkDriverConfig = TypesafeConfigUtils.extractSubConfig(sparkConfig, driverPrefix, true);
            StringBuilder stringBuilder = new StringBuilder();
            for (Map.Entry<String, ConfigValue> entry : sparkDriverConfig.entrySet()) {
                String key = entry.getKey();
                SparkDriverSettings settings = SparkDriverSettings.fromProperty(key);
                if (settings != null) {
                    String conf = String.format("%s=%s", settings.option, entry.getValue().unwrapped());
                    stringBuilder.append(conf);
                }
            }

            System.out.println(stringBuilder.toString());
        }
    }
}
