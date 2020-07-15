package io.github.interestinglab.waterdrop.config;

import java.util.*;

public enum SparkDriverSettings {

    DRIVER_MEMORY("spark.driver.memory", "--driver-memory"),
    DRIVER_JAVA_OPTIONS("spark.driver.extraJavaOptions", "--driver-java-options"),
    DRIVER_LIBRARY_PATH("spark.driver.extraLibraryPath", "--driver-library-path"),
    DRIVER_CLASS_PATH("spark.driver.extraClassPath", "--driver-class-path");


    private static final Map<String, SparkDriverSettings> driverSettings;

    static {
        Map<String, SparkDriverSettings> map = new HashMap<String, SparkDriverSettings>();
        for (SparkDriverSettings config : values())
            map.put(config.property, config);

        driverSettings = Collections.unmodifiableMap(map);
    }

    private final String property;
    public final String option;

    SparkDriverSettings(String property, String option) {
        this.property = property;
        this.option = option;
    }

    public static SparkDriverSettings fromProperty(String property) {
        return driverSettings.getOrDefault(property, null);
    }
}
