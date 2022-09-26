package org.apache.seatunnel;

import org.apache.flink.table.runtime.functions.DateTimeFunctions;
import org.apache.seatunnel.core.base.Seatunnel;
import org.apache.seatunnel.core.base.command.Command;
import org.apache.seatunnel.core.base.exception.CommandException;
import org.apache.seatunnel.core.flink.FlinkStarter;
import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.flink.command.FlinkCommandBuilder;
import org.apache.seatunnel.core.flink.config.FlinkJobType;
import org.apache.seatunnel.core.flink.utils.CommandLineUtils;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormatter;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws CommandException {
        System.setProperty("java.io.tmpdir", "D:/tmp");
        FlinkCommandArgs flinkCommandArgs = CommandLineUtils.parseCommandArgs(args, FlinkJobType.JAR);
        Command<FlinkCommandArgs> flinkCommand = new FlinkCommandBuilder()
                .buildCommand(flinkCommandArgs);
        Seatunnel.run(flinkCommand);
//        FlinkStarter.main(args);
    }
}
