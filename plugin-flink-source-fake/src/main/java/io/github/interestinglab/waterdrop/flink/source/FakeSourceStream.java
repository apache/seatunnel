package io.github.interestinglab.waterdrop.flink.source;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSource;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

public class FakeSourceStream extends RichParallelSourceFunction<Row> implements FlinkStreamSource<Row> {

    private volatile boolean running = true;

    private Config config;

    @Override
    public DataStream<Row> getData(FlinkEnvironment env) {
        return env.getStreamExecutionEnvironment()
                .addSource(this)
                .returns(new RowTypeInfo(STRING_TYPE_INFO,LONG_TYPE_INFO));
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
        return new CheckResult(true,"");
    }

    @Override
    public void prepare(FlinkEnvironment env) {}


    private static final String[] NAME_ARRAY = new String[]{"Gary", "Ricky Huo", "Kid Xiong"};

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        while (running) {
            int randomNum = (int) (1 + Math.random() * 3);
            Row row = Row.of(NAME_ARRAY[randomNum - 1], System.currentTimeMillis());
            ctx.collect(row);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
