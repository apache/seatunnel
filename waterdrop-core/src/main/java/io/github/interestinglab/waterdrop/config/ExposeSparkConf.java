package io.github.interestinglab.waterdrop.config;


import java.io.File;
import java.util.Map;

public class ExposeSparkConf {

    public static void main(String[] args) throws Exception {
        Config appConfig = ConfigFactory.parseFile(new File(args[0]))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, ConfigValue> entry : appConfig.getConfig("spark").entrySet()) {
            String key = entry.getKey();
            if (key.equals("spark.yarn.keytab") || key.equals("spark.yarn.principal")) {
                String argKey = key.substring(key.lastIndexOf(".") + 1); // keytab, principal
                String conf = String.format(" --%s %s", argKey, entry.getValue().unwrapped());
                stringBuilder.append(conf);
            } else {
                String conf = String.format(" --conf \"%s=%s\" ", key, entry.getValue().unwrapped());
                stringBuilder.append(conf);
            }
        }

        System.out.print(stringBuilder.toString());
    }
}
