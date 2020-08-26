package io.github.interestinglab.waterdrop.config;

import java.io.File;
import java.util.Map;

public class ExposeSparkConf {

    public static void main(String[] args) throws Exception {
        Config appConfig = ConfigFactory.parseFile(new File(args[0]))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, ConfigValue> entry: appConfig.getConfig("env").entrySet()) {
            String conf = String.format(" --conf \"%s=%s\" ", entry.getKey(), entry.getValue().unwrapped());
            stringBuilder.append(conf);
        }

        System.out.print(stringBuilder.toString());
    }
}
