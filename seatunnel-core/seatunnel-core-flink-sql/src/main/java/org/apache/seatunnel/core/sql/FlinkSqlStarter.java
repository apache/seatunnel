package org.apache.seatunnel.core.sql;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.core.base.Starter;
import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.flink.config.FlinkJobStarter;
import org.apache.seatunnel.core.flink.utils.CommandLineUtils;

import java.util.List;

public class FlinkSqlStarter implements Starter {

    private static final String APP_JAR_NAME = "seatunnel-core-flink-sql.jar";
    private static final String CLASS_NAME = SeatunnelSql.class.getName();


    private final FlinkCommandArgs flinkCommandArgs;
    /**
     * SeaTunnel flink sql job jar.
     */
    private final String appJar;

    public FlinkSqlStarter(String[] args) {
        this.flinkCommandArgs = CommandLineUtils.parseCommandArgs(args, FlinkJobStarter.JAR);
        // set the deployment mode, used to get the job jar path.
        Common.setDeployMode(flinkCommandArgs.getDeployMode().getName());
        this.appJar = Common.appLibDir().resolve(APP_JAR_NAME).toString();
    }

    @Override
    public List<String> buildCommands() {
        return CommandLineUtils.buildFlinkCommand(flinkCommandArgs, CLASS_NAME, appJar);
    }

    @SuppressWarnings("checkstyle:RegexpSingleline")
    public static void main(String[] args) {
        FlinkSqlStarter flinkSqlStarter = new FlinkSqlStarter(args);
        System.out.println(String.join(" ", flinkSqlStarter.buildCommands()));
    }
}
