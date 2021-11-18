package io.github.interestinglab.waterdrop.config;


import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ExposeSparkConf {

    private static final String spark_driver_extraJavaOptions = "spark.driver.extraJavaOptions";
    private static final String spark_executor_extraJavaOptions = "spark.executor.extraJavaOptions";

    public static void main(String[] args) throws Exception {
        Config appConfig = ConfigFactory.parseFile(new File(args[0]))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

        StringBuilder stringBuilder = new StringBuilder();
        Map<String, String> sparkConfs = new LinkedHashMap<String, String>();
        for (Map.Entry<String, ConfigValue> entry : appConfig.getConfig("spark").entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().unwrapped().toString();
            if (key.equals("spark.yarn.keytab") || key.equals("spark.yarn.principal")) {
                String argKey = key.substring(key.lastIndexOf(".") + 1); // keytab, principal
                String conf = String.format(" --%s %s", argKey, value);
                stringBuilder.append(conf);
            } else {
                String v = sparkConfs.getOrDefault(key, null);
                if (StringUtils.isBlank(v)) {
                    sparkConfs.put(key, value);
                } else {
                    sparkConfs.put(key, v + " " + value);
                }
            }
        }

        for (Map.Entry<String, String> c : sparkConfs.entrySet()) {
            String v = addLogPropertiesIfNeeded(c.getKey(), c.getValue());
            String conf = String.format(" --conf %s=%s ", c.getKey(), v);
            stringBuilder.append(conf);
        }

        if (!sparkConfs.containsKey(spark_driver_extraJavaOptions)) {
            stringBuilder.append(" --conf " + spark_driver_extraJavaOptions + "=" + logConfiguration());
        }

        if (!sparkConfs.containsKey(spark_executor_extraJavaOptions)) {
            stringBuilder.append(" --conf " + spark_executor_extraJavaOptions + "=" + logConfiguration());
        }

        System.out.print(stringBuilder.toString());
    }

    private static String addLogPropertiesIfNeeded(String key, String value) {
        if (!value.contains("-Dlog4j.configuration")) {
            if (key.equals(spark_driver_extraJavaOptions)
                || key.equals(spark_executor_extraJavaOptions) ) {

                return value + " " + logConfiguration();
            }
        }

        return value;
    }

    private static String runningJarPath() {
        try {
            return new File(ExposeSparkConf.class.getProtectionDomain().getCodeSource().getLocation()
              .toURI()).getParent();
        } catch (URISyntaxException e) {
            throw new RuntimeException("failed to get work dir of seatunnel");
        }
    }

    private static String logConfiguration() {
        return String.format("-Dlog4j.configuration=file:%s/config/log4j.properties", runningJarPath());
    }
}
