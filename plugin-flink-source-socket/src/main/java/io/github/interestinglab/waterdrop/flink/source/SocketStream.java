package io.github.interestinglab.waterdrop.flink.source;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class SocketStream implements FlinkStreamSource<Row> {

    private Config config;

    private static final String HOST = "host";
    private static final String PORT = "port";

    private String host = "localhost";

    private int port = 9999;


    @Override
    public DataStream<Row> getData(FlinkEnvironment env) {
        final StreamExecutionEnvironment environment = env.getStreamExecutionEnvironment();
        final SingleOutputStreamOperator<Row> operator = environment.socketTextStream(host, port)
                .map((MapFunction<String, Row>) value -> {
                    Row row = new Row(1);
                    row.setField(0, value);
                    return row;
                }).returns(new RowTypeInfo(Types.STRING()));
        return operator;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        if (config.hasPath(HOST)){
            host = config.getString(HOST);
        }
        if (config.hasPath(PORT)){
            port = config.getInt(PORT);
        }
    }
}
