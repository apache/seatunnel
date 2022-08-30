package org.apache.seatunnel.e2e.flink.local;

import org.apache.seatunnel.core.starter.Seatunnel;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.command.FlinkCommandBuilder;
import org.apache.seatunnel.core.starter.flink.config.FlinkRunMode;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created 2022/8/30
 */

@Builder
@AllArgsConstructor
public class FlinkLocalContainer {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkLocalContainer.class);

    private List<String> parameters = new ArrayList<>();

    public void executeSeaTunnelFlinkJob(String path) throws CommandException {
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setRunMode(FlinkRunMode.RUN);
        flinkCommandArgs.setConfigFile(getResource(path));
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(parameters);
        Command<FlinkCommandArgs> flinkCommandArgsCommand = new FlinkCommandBuilder()
            .buildCommand(flinkCommandArgs);
        Seatunnel.run(flinkCommandArgsCommand);
    }

    @SneakyThrows
    private String getResource(String confFile) {
        return FlinkLocalContainer.class.getClassLoader().getResource(confFile)
            .toURI().getPath();
    }
}
