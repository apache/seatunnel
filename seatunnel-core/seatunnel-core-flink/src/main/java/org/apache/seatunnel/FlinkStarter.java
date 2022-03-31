package org.apache.seatunnel;

import org.apache.seatunnel.command.FlinkCommandArgs;
import org.apache.seatunnel.common.config.Common;

import com.beust.jcommander.JCommander;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FlinkStarter implements Starter {

    private FlinkCommandArgs flinkCommandArgs;

    private List<String> flinkParams = new ArrayList<>();

    private static final String jar = Common.appLibDir().resolve("seatunnel-core-flink.jar").toString();

    private static final String appName = SeatunnelFlink.class.getName();

    public FlinkStarter(String[] args) {
        this.flinkCommandArgs = parseSeaTunnelArgs(args);
        this.flinkParams = parseFlinkArgs(args);
        setSystemProperties();
    }

    public static void main(String[] args) {
        FlinkStarter flinkStarter = new FlinkStarter(args);
        List<String> command = flinkStarter.buildCommands();
        System.out.println(String.join(" ", command));

    }

    /**
     * Parse seatunnel args.
     *
     * @param args args
     * @return FlinkCommandArgs
     */
    private FlinkCommandArgs parseSeaTunnelArgs(String[] args) {
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        JCommander jCommander = JCommander.newBuilder()
            .programName("start-seatunnel-flink.sh")
            .addObject(flinkCommandArgs)
            .args(args)
            .build();
        if (flinkCommandArgs.isHelp()) {
            jCommander.usage();
            System.exit(0);
        }
        return flinkCommandArgs;
    }

    /**
     * If args is not seatunnel args, add it to flink params.
     *
     * @param args args
     * @return flink params
     */
    private List<String> parseFlinkArgs(String[] args) {
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        JCommander jCommander = JCommander.newBuilder()
            .programName("start-seatunnel-flink.sh")
            .addObject(flinkCommandArgs)
            .build();
        jCommander.parse(args);
        return new ArrayList<>();
    }

    private void setSystemProperties() {
        flinkCommandArgs.getVariables()
            .stream()
            .filter(Objects::nonNull)
            .map(variable -> variable.split("=", 2))
            .filter(pair -> pair.length == 2)
            .forEach(pair -> System.setProperty(pair[0], pair[1]));
    }

    @Override
    public List<String> buildCommands() {
        List<String> command = new ArrayList<>();
        command.add("${FLINK_HOME}/bin/flink");
        command.add("run");
        // add flink params

        command.add("-c");
        command.add(appName);
        command.add(jar);
        command.add("--config");
        command.add(flinkCommandArgs.getConfigFile());
        return command;
    }


}
