package io.github.interestinglab.waterdrop.flink.stream.transform;

import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnv;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;


/**
 * @author mr_xiong
 * @date 2019-07-03 15:29
 * @description
 */
public class Watermark extends AbstractFlinkStreamTransform<Row,Row> {

    private long lateness;

    private String eventTimeFieldName;

    private int eventTimeFieldIndex = -1;

    @Override
    public DataStream<Row> process(DataStream<Row> dataStream, FlinkStreamEnv env) {

        int index;
        if (eventTimeFieldIndex > -1){
            index = eventTimeFieldIndex;
        }else {
            index = ((RowTypeInfo) dataStream.getType()).getFieldIndex(eventTimeFieldName);
        }

        SingleOutputStreamOperator<Row> addWatermark = dataStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(lateness)) {
            @Override
            public long extractTimestamp(Row element) {
                return (long) element.getField(index);
            }
        });
        return addWatermark;
    }

    @Override
    public CheckResult checkConfig() {
        return null;
    }

    @Override
    public void prepare() {
        if (config.hasPath("eventTime.lateness")){
            lateness = config.getLong("eventTime.lateness");
        }
        if (config.hasPath("eventTime.index")){
            eventTimeFieldIndex = config.getInt("eventTime.index");
        }else {
            eventTimeFieldName = config.getString("eventTime.name");
        }
    }
}
