package io.github.interestinglab.waterdrop;

import io.github.interestinglab.waterdrop.apis.BaseSink;
import io.github.interestinglab.waterdrop.apis.BaseSource;
import io.github.interestinglab.waterdrop.apis.BaseTransform;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException;
import io.github.interestinglab.waterdrop.config.CommandLineArgs;
import io.github.interestinglab.waterdrop.config.CommandLineUtils;
import io.github.interestinglab.waterdrop.common.config.Common;
import io.github.interestinglab.waterdrop.config.ConfigBuilder;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.plugin.Plugin;
import io.github.interestinglab.waterdrop.utils.AsciiArtUtils;
import io.github.interestinglab.waterdrop.utils.Engine;
import io.github.interestinglab.waterdrop.utils.PluginType;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.core.fs.Path;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scopt.OptionParser;

import java.util.Arrays;
import java.util.List;

import static io.github.interestinglab.waterdrop.utils.Engine.SPARK;

/**
 * @author mr_xiong
 * @date 2019-12-29 17:20
 * @description
 */
public class Waterdrop {

    public static void main(String[] args) {
        OptionParser<CommandLineArgs> sparkParser = CommandLineUtils.sparkParser();
        run(sparkParser, SPARK, args);
    }

    public static void run(OptionParser<CommandLineArgs> parser,Engine engine,String[] args){
        Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(args).iterator()).asScala().toSeq();
        Option<CommandLineArgs> option = parser.parse(seq, new CommandLineArgs("client", "application.conf", false));
        if (option.isDefined()) {
            CommandLineArgs commandLineArgs = option.get();
            Common.setDeployMode(commandLineArgs.deployMode());
            String configFilePath = getConfigFilePath(commandLineArgs, engine);
            boolean testConfig = commandLineArgs.testConfig();
            if (testConfig) {
                new ConfigBuilder(configFilePath).checkConfig();
                System.out.println("config OK !");
            } else {
                try {
                    entryPoint(configFilePath, engine);
                } catch (ConfigRuntimeException e) {
                    showConfigError(e);
                }catch (Exception e){
                    showFatalError(e);
                }
            }
        }
    }

    private static String getConfigFilePath(CommandLineArgs cmdArgs, Engine engine) {
        String path = null;
        switch (engine) {
            case FLINK:
                path = cmdArgs.configFile();
                break;
            case SPARK:
                final Option<String> mode = Common.getDeployMode();
                if (mode.isDefined() && "cluster".equals(mode.get())) {
                    path = new Path(cmdArgs.configFile()).getName();
                } else {
                    path = cmdArgs.configFile();
                }
                break;
            default:
                break;
        }
        return path;
    }

    private static void entryPoint(String configFile, Engine engine) {

        ConfigBuilder configBuilder = new ConfigBuilder(configFile, engine);
        List<BaseSource> sources = configBuilder.createPlugins(PluginType.SOURCE);
        List<BaseTransform> transforms = configBuilder.createPlugins(PluginType.TRANSFORM);
        List<BaseSink> sinks = configBuilder.createPlugins(PluginType.SINK);
        Execution execution = configBuilder.createExecution();
        baseCheckConfig(sources, transforms, sinks);
        prepare(configBuilder.getEnv(), sources, transforms, sinks);
        showWaterdropAsciiLogo();

        execution.start(sources, transforms, sinks);
    }

    private static void baseCheckConfig(List<? extends Plugin>... plugins) {
        boolean configValid = true;
        for (List<? extends Plugin> pluginList : plugins) {
            for (Plugin plugin : pluginList) {
                CheckResult checkResult = null;
                try {
                    checkResult = plugin.checkConfig();
                } catch (Exception e) {
                    checkResult = new CheckResult(false, e.getMessage());
                }
                if (!checkResult.isSuccess()) {
                    configValid = false;
                    System.out.println(String.format("Plugin[%s] contains invalid config, error: %s\n"
                            , plugin.getClass().getName(), checkResult.getMsg()));
                }
                if (!configValid) {
                    System.exit(-1); // invalid configuration
                }
            }
        }
    }

    private static void prepare(RuntimeEnv env, List<? extends Plugin>... plugins) {
        for (List<? extends Plugin> pluginList : plugins) {
            pluginList.forEach(plugin -> plugin.prepare(env));
        }

    }

    private static void showWaterdropAsciiLogo() {
        AsciiArtUtils.printAsciiArt("Waterdrop");
    }

    private static void showConfigError(Throwable throwable) {
        System.out.println(
                "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        System.out.println("Config Error:\n");
        System.out.println("Reason: " + errorMsg + "\n");
        System.out.println(
                "\n===============================================================================\n\n\n");
    }

    private static void showFatalError(Throwable throwable) {
        System.out.println(
                "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        System.out.println("Fatal Error, \n");
        System.out.println(
                "Please contact garygaowork@gmail.com or issue a bug in https://github.com/InterestingLab/waterdrop/issues\n");
        System.out.println("Reason: " + errorMsg + "\n");
        System.out.println("Exception StackTrace: " + ExceptionUtils.getStackTrace(throwable));
        System.out.println(
                "\n===============================================================================\n\n\n");
    }
}
