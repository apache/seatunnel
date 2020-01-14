package io.github.interestinglab.waterdrop.config;

import com.typesafe.config.waterdrop.Config;
import com.typesafe.config.waterdrop.ConfigFactory;
import com.typesafe.config.waterdrop.ConfigRenderOptions;
import com.typesafe.config.waterdrop.ConfigResolveOptions;
import io.github.interestinglab.waterdrop.apis.BaseSink;
import io.github.interestinglab.waterdrop.apis.BaseSource;
import io.github.interestinglab.waterdrop.apis.BaseTransform;
import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchExecution;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamExecution;
import io.github.interestinglab.waterdrop.plugin.Plugin;
import io.github.interestinglab.waterdrop.spark.SparkEnvironment;
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchExecution;
import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingExecution;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * @author mr_xiong
 * @date 2019-12-29 14:56
 * @description
 */
public class ConfigBuilder {

    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private String configFile;
    private String engine;
    private ConfigPackage configPackage;
    private Config config;
    private boolean streaming;
    private Config envConfig;
    private RuntimeEnv env;

    public ConfigBuilder(String configFile, String engine) {
        this.configFile = configFile;
        this.engine = engine;
        this.configPackage = new ConfigPackage(engine);
        this.config = load();
        this.env = createEnv();
    }

    public ConfigBuilder(String configFile) {
        this.configFile = configFile;
        this.engine = "";
        this.config = load();
        this.env = createEnv();
    }

    private Config load() {

        if (configFile.isEmpty()) {
            throw new ConfigRuntimeException("Please specify config file");
        }

        System.out.println("[INFO] Loading config file: " + configFile);

        // variables substitution / variables resolution order:
        // config file --> system environment --> java properties
        Config config = ConfigFactory
                .parseFile(new File(configFile))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));

        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        System.out.println("[INFO] parsed config file: " + config.root().render(options));
        return config;
    }


    public Config getEnvConfigs() {
        return envConfig;
    }

    public RuntimeEnv getEnv() {
        return env;
    }

    private boolean checkIsStreaming() {
        List<? extends Config> sourceConfigList = config.getConfigList("source");

        return sourceConfigList.get(0).getString(PLUGIN_NAME_KEY).toLowerCase().endsWith("stream");
    }

    /**
     * Get full qualified class name by reflection api, ignore case.
     **/
    private String buildClassFullQualifier(String name, String classType) throws Exception {

        if (name.split("\\.").length == 1) {
            String packageName = null;
            Iterable<? extends Plugin> plugins = null;
            switch (classType) {
                case "source":
                    packageName = configPackage.sourcePackage();
                    Class baseSource = Class.forName(configPackage.baseSourcePackage());
                    plugins = ServiceLoader.load(baseSource);
                    break;
                case "transform":
                    packageName = configPackage.transformPackage();
                    Class baseTransform = Class.forName(configPackage.baseTransformPackage());
                    plugins = ServiceLoader.load(baseTransform);
                    break;
                case "sink":
                    packageName = configPackage.sinkPackage();
                    Class baseSink = Class.forName(configPackage.baseSinkPackage());
                    plugins = ServiceLoader.load(baseSink);
                    break;
                default:
                    break;
            }

            String qualifierWithPackage = packageName + "." + name;
            for (Plugin plugin : plugins) {
                Class serviceClass = plugin.getClass();
                String serviceClassName = serviceClass.getName();
                String clsNameToLower = serviceClassName.toLowerCase();
                if (clsNameToLower.equals(qualifierWithPackage.toLowerCase())) {
                    return serviceClassName;
                }
            }
            return qualifierWithPackage;
        } else {
            return name;
        }
    }


    /**
     * check if config is valid.
     **/
    public void checkConfig() {
        this.createEnv();
        this.createPlugins("source", BaseSource.class);
        this.createPlugins("transform", BaseTransform.class);
        this.createPlugins("sink", BaseSink.class);
    }

    public <T extends Plugin> List<T> createPlugins(String type, Class<T> clazz) {

        List<T> basePluginList = new ArrayList<>();

        List<? extends Config> configList = config.getConfigList(type);

        configList.forEach(plugin -> {
            try {
                final String className = buildClassFullQualifier(plugin.getString(PLUGIN_NAME_KEY), type);
                T t = null;

                t = (T) Class.forName(className).newInstance();

                t.setConfig(plugin);
                basePluginList.add(t);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        return basePluginList;
    }

    private RuntimeEnv createEnv() {
        envConfig = config.getConfig("env");
        streaming = checkIsStreaming();
        RuntimeEnv env = null;
        switch (engine) {
            case "spark":
                env = new SparkEnvironment();
                break;
            case "flink":
                env = new FlinkEnvironment();
                break;
            default:
                break;
        }
        env.setConfig(envConfig);
        env.prepare(streaming);
        return env;
    }


    public Execution createExecution() {
        Execution execution = null;
        switch (engine) {
            case "spark":
                SparkEnvironment sparkEnvironment = (SparkEnvironment) env;
                if (streaming) {
                    execution = new SparkStreamingExecution(sparkEnvironment);
                } else {
                    execution = new SparkBatchExecution(sparkEnvironment);
                }
                break;
            case "flink":
                FlinkEnvironment flinkEnvironment = (FlinkEnvironment) env;
                if (streaming) {
                    execution = new FlinkStreamExecution(flinkEnvironment);
                } else {
                    execution = new FlinkBatchExecution(flinkEnvironment);
                }
                break;
            default:
                break;
        }
        return execution;
    }


}
