package io.github.interestinglab.waterdrop;


import io.github.interestinglab.waterdrop.apis.BaseSink;
import io.github.interestinglab.waterdrop.apis.BaseSource;
import io.github.interestinglab.waterdrop.apis.BaseTransform;
import io.github.interestinglab.waterdrop.config.ConfigParser;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.plugin.Plugin;

import java.io.File;
import java.util.List;

/**
 * @author mr_xiong
 * @date 2019-05-29 14:40
 * @description
 */
public class Application {

    public static void main(String[] args) throws ConfigParser.ConfigError {

        File file = new File(args[0]);

        ConfigParser configParser = new ConfigParser(file);

        configParser.parse();

        RuntimeEnv runtimeEnv = configParser.getRuntimeEnv();

        List<BaseSource> sources = configParser.getSources();

        List<BaseTransform> transforms = configParser.getTransforms();

        List<BaseSink> sinks = configParser.getSinks();

        prepare(runtimeEnv, sources, transforms, sinks);

        runtimeEnv.start(sources, transforms, sinks);
    }

    private static void prepare(RuntimeEnv runtimeEnv, List<? extends Plugin>... plugins) {

        runtimeEnv.prepare();

        for (List<? extends Plugin> list : plugins){
            list.forEach(plugin -> plugin.prepare());
        }
    }

}
