package io.github.interestinglab.waterdrop.flink.stream.sink;

import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnv;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author mr_xiong
 * @date 2019-07-04 16:44
 * @description
 */
public class Kafka extends InternalFlinkStreamSink<String,String> {

    private String topic;
    private Properties kafkaParams = new Properties();

    private final String producerPrefix = "producer.";

    @Override
    public DataStreamSink<String> output(DataStream<String> dataStream, FlinkStreamEnv env) {
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(topic, new SimpleStringSchema(), kafkaParams);
        return dataStream.addSink(kafkaProducer);
    }

    @Override
    public CheckResult checkConfig() {
        return null;
    }

    @Override
    public void prepare() {
        topic = config.getString("topic");
        config.entrySet().forEach(entry -> {
            String key = entry.getKey();
            Object value = entry.getValue().unwrapped();
            if (key.startsWith(producerPrefix)) {
                kafkaParams.put(key.substring(producerPrefix.length()), value);
            }
        });
    }
}
