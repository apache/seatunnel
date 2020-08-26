package io.github.interestinglab.waterdrop.flink.sink;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.PropertiesUtil;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSink;
import io.github.interestinglab.waterdrop.flink.util.SchemaUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import java.util.Properties;

public class KafkaTable implements FlinkStreamSink<Row, Row> {

    private Config config;
    private Properties kafkaParams = new Properties();
    private String topic;


    @Override
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        Table table = tableEnvironment.fromDataStream(dataStream);
        insert(tableEnvironment,table);
        return null;
    }


    private void insert(TableEnvironment tableEnvironment,Table table){
        TypeInformation<?>[] types = table.getSchema().getFieldTypes();
        String[] fieldNames = table.getSchema().getFieldNames();
        Schema schema = getSchema(types, fieldNames);
        String uniqueTableName = SchemaUtil.getUniqueTableName();
        tableEnvironment.connect(getKafkaConnect())
                .withSchema(schema)
                .withFormat(setFormat())
                .inAppendMode()
                .registerTableSink(uniqueTableName);
        table.insertInto(uniqueTableName);
    }


    private Schema getSchema(TypeInformation<?>[] informations, String[] fieldNames) {
        Schema schema = new Schema();
        for (int i = 0; i < informations.length; i++) {
            schema.field(fieldNames[i], informations[i]);
        }
        return schema;
    }

    private Kafka getKafkaConnect() {

        Kafka kafka = new Kafka().version("universal");
        kafka.topic(topic);
        kafka.properties(kafkaParams);
        return kafka;
    }

    private FormatDescriptor setFormat() {
        return new Json().failOnMissingField(false).deriveSchema();
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
        return CheckConfigUtil.check(config,"topics");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        topic = config.getString("topics");
        String producerPrefix = "producer.";
        PropertiesUtil.setProperties(config, kafkaParams, producerPrefix, false);
        kafkaParams.put("key.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaParams.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
    }
}
