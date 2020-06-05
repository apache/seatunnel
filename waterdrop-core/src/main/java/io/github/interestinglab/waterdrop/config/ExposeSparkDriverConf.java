package io.github.interestinglab.waterdrop.config;


import java.io.File;
import java.util.Map;

public class ExposeSparkDriverConf {

    public static void main(String[] args) throws Exception {
        Config appConfig = ConfigFactory.parseFile(new File(args[0]))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

        String driverPrefix = "spark.driver.";
        Config sparkConfig = appConfig.getConfig("spark");

        if (!TypesafeConfigUtils.hasSubConfig(sparkConfig, driverPrefix)) {
            System.out.println("");
        } else {
            Config sparkDriverConfig = TypesafeConfigUtils.extractSubConfig(sparkConfig, driverPrefix, false);
            StringBuilder stringBuilder = new StringBuilder();
            for (Map.Entry<String, ConfigValue> entry: sparkDriverConfig.entrySet()) {
                String conf = String.format(" --driver-%s=%s ", entry.getKey(), entry.getValue().unwrapped());
                stringBuilder.append(conf);
            }

            System.out.println(stringBuilder.toString());
        }
    }
}
