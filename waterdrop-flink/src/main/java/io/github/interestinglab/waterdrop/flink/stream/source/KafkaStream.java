package io.github.interestinglab.waterdrop.flink.stream.source;

import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnv;
import io.github.interestinglab.waterdrop.flink.utils.PropertiesUtil;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author mr_xiong
 * @date 2019-07-01 17:49
 * @description
 */
public class KafkaStream extends InternalFlinkStreamSource<Row> {

    private Properties kafkaParams = new Properties();

    private final String consumerPrefix = "consumer.";

    private String topic;

    @Override
    public DataStream<Row> getData(FlinkStreamEnv env) {
        FlinkKafkaConsumer<Row> consumer = new FlinkKafkaConsumer<>(topic, new RowDeserializationSchema(), kafkaParams);
        return env.getEnvironment().addSource(consumer);
    }

    @Override
    public CheckResult checkConfig() {
        return null;
    }

    @Override
    public void prepare() {

        topic = config.getString("topics");
        PropertiesUtil.setProperties(config, kafkaParams, consumerPrefix);
    }

    class RowDeserializationSchema implements KafkaDeserializationSchema<Row> {

        @Override
        public boolean isEndOfStream(Row nextElement) {
            return false;
        }

        @Override
        public Row deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

            return Row.of(record.topic(), new String(record.value(), StandardCharsets.UTF_8));
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            TypeInformation[] info = {BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
            String[] name = {"topic", "message"};
            return new RowTypeInfo(info, name);
        }
    }
}
