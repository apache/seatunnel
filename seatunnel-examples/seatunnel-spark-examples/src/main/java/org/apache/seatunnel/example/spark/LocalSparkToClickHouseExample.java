package org.apache.seatunnel.example.spark;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.base.Seatunnel;
import org.apache.seatunnel.core.base.command.Command;
import org.apache.seatunnel.core.base.exception.CommandException;
import org.apache.seatunnel.core.spark.args.SparkCommandArgs;
import org.apache.seatunnel.core.spark.command.SparkCommandBuilder;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class LocalSparkToClickHouseExample {

    public static void main(String[] args) throws URISyntaxException, FileNotFoundException, CommandException {
        String configFile = getTestConfigFile("/examples/spark.batch.clickhouse.conf");
        SparkCommandArgs sparkArgs = new SparkCommandArgs();
        sparkArgs.setConfigFile(configFile);
        sparkArgs.setCheckConfig(false);
        sparkArgs.setVariables(null);
        sparkArgs.setDeployMode(DeployMode.CLIENT);
        Command<SparkCommandArgs> sparkCommand = new SparkCommandBuilder().buildCommand(sparkArgs);
        Seatunnel.run(sparkCommand);
    }

    public static String getTestConfigFile(String configFile) throws URISyntaxException, FileNotFoundException {
        URL resource = LocalSparkToClickHouseExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Could not find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}