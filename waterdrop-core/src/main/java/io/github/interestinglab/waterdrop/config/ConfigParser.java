package io.github.interestinglab.waterdrop.config;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.interestinglab.waterdrop.apis.BaseSink;
import io.github.interestinglab.waterdrop.apis.BaseSource;
import io.github.interestinglab.waterdrop.apis.BaseTransform;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchEnv;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnv;
import io.github.interestinglab.waterdrop.plugin.Plugin;
import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingEnv;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Data
public class ConfigParser {

    private static final Logger logger = LoggerFactory.getLogger(ConfigParser.class);

    public static class ConfigError extends Exception {

        public ConfigError(String message) {
            super(message);
        }
    }

    private Config config;
    private List<BaseSource> sources = new ArrayList<>();
    private List<BaseTransform> transforms = new ArrayList<>();
    private List<BaseSink> sinks = new ArrayList<>();
    private RuntimeEnv runtimeEnv;

    public ConfigParser(File file) {

        this.config = ConfigFactory.parseFile(file);
    }


    /**
     * 配置解析
     *
     * @throws ConfigError
     */
    public void parse() throws ConfigError {

        logger.info("Parsing Config: \n" + config.root().render());

        if (config.getConfig("base").hasPath("engine")) {
            String engine = config.getString("base.engine");
            switch (engine) {
                case "flinkStream":
                    runtimeEnv = new FlinkStreamEnv();
                    break;
                case "flinkBatch":
                    runtimeEnv = new FlinkBatchEnv();
                    break;
                case "sparkBatch":
//                    runtimeEnv = new SparkbatchEnv();
                    break;
                case "sparkStreaming":
                    runtimeEnv = new SparkStreamingEnv();
                    break;
                case "structStreaming":
//                    runtimeEnv = new StructuredStreamingEnv();
                    break;
                default:
                    throw new RuntimeException("not found engine :" + engine);
            }
            runtimeEnv.setConfig(config.getConfig("base"));
        }

        List<? extends Config> sources = config.getConfigList("source");

        for (Config conf : sources) {
            this.sources.add((BaseSource) parsePlugin(conf));
        }

        if (config.hasPath("transform")){
            List<? extends Config> transforms = config.getConfigList("transform");

            for (Config conf : transforms) {
                this.transforms.add((BaseTransform) parsePlugin(conf));
            }
        }

        List<? extends Config> sinks = config.getConfigList("sink");

        for (Config conf : sinks) {
            this.sinks.add((BaseSink) parsePlugin(conf));
        }
    }


    /**
     * 生成plugin
     *
     * @param config
     * @return
     * @throws ConfigError
     */
    private Plugin parsePlugin(Config config) throws ConfigError {
        String pluginCls = getPluginCls(config.getString("type"));

        try {
            Plugin plugin = (Plugin) Class.forName(pluginCls).getConstructors()[0].newInstance();
            plugin.setConfig(config);
            return plugin;

        } catch (ClassNotFoundException e) {
            throw new ConfigError("plugin type not found: " + pluginCls);
        } catch (Exception e) {
            throw new ConfigError("unknow error: " + ExceptionUtils.getStackTrace(e));
        }
    }


    /**
     * 获取全限定名
     *
     * @param type
     * @return
     */
    private String getPluginCls(String type) {
        String[] paths = type.split("\\.");
        int clzPos = paths.length - 1;
        String clzName = paths[clzPos];
        paths[clzPos] = StringUtils.capitalize(clzName);
        return String.join(".", paths);
    }


}