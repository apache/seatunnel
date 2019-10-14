package io.github.interestinglab.waterdrop.flink.sink;

import com.typesafe.config.waterdrop.Config;
import io.github.interestinglab.waterdrop.common.PropertiesUtil;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSink;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSink;
import io.github.interestinglab.waterdrop.flink.util.SchemaUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author mr_xiong
 * @date 2019-07-22 18:39
 * @description
 */
public class KafkaTable implements FlinkStreamSink<Row, Void>, FlinkBatchSink<Row, Void> {

    private Config config;
    private Properties kafkaParams = new Properties();
    private String topic;
    private final String producerPrefix = "producer.";


    @Override
    public DataStreamSink<Void> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        Table table = tableEnvironment.fromDataStream(dataStream);
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
        return null;
    }

    @Override
    public DataSink<Void> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        BatchTableEnvironment tableEnvironment = env.getBatchTableEnvironment();
        Table table = tableEnvironment.fromDataSet(dataSet);
        TypeInformation<?>[] types = table.getSchema().getFieldTypes();
        String[] fieldNames = table.getSchema().getFieldNames();
        Schema schema = getSchema(types, fieldNames);
        String uniqueTableName = SchemaUtil.getUniqueTableName();
        tableEnvironment.connect(getKafkaConnect())
                .withSchema(schema)
                .withFormat(setFormat())
                .registerTableSink(uniqueTableName);
        table.insertInto(uniqueTableName);
        return null;
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
    public void prepare() {
        topic = config.getString("topics");
        PropertiesUtil.setProperties(config, kafkaParams, producerPrefix, false);
    }
}
