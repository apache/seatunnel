package io.github.interestinglab.waterdrop.flink.stream.sink;

import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnv;
import io.github.interestinglab.waterdrop.flink.utils.PropertiesUtil;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import java.util.Properties;

/**
 * @author mr_xiong
 * @date 2019-07-22 18:39
 * @description
 */
public class KafkaTable extends InternalFlinkStreamSink<Void,Void> {

    private String tableName;
    private String sinkTableName;
    private Properties kafkaParams = new Properties();
    private String topic;
    private final String producerPrefix = "producer.";

    @Override
    public DataStreamSink<Void> output(DataStream<Void> dataStream, FlinkStreamEnv env) {
        StreamTableEnvironment tableEnvironment = env.getTableEnvironment();
        Table table = tableEnvironment.scan(tableName);
        TypeInformation<?>[] informations = table.getSchema().getFieldTypes();
        String[] fieldNames = table.getSchema().getFieldNames();
        Schema schema = getSchema(informations, fieldNames);
        tableEnvironment.connect(getKafkaConnect())
                .withSchema(schema)
                .withFormat(setFormat())
                .inAppendMode()
                .registerTableSink(sinkTableName);
        table.insertInto(sinkTableName);
        return null;
    }

    private Schema getSchema( TypeInformation<?>[] informations,String[] fieldNames){
        Schema schema = new Schema();
        for (int i = 0; i < informations.length; i++){
            schema.field(fieldNames[i],informations[i]);
        }
        return schema;
    }

    private Kafka getKafkaConnect(){

        org.apache.flink.table.descriptors.Kafka kafka = new Kafka().version("universal");
        kafka.topic(topic);
        kafka.properties(kafkaParams);
        return kafka;
    }

    private FormatDescriptor setFormat(){
        return new Json().failOnMissingField(false).deriveSchema();
    }

    @Override
    public CheckResult checkConfig() {
        return new CheckResult(true,"");
    }

    @Override
    public void prepare() {
        tableName = config.getString("table_name");
        sinkTableName = config.getString("sink_table_name");
        topic = config.getString("topics");
        PropertiesUtil.setProperties(config, kafkaParams, producerPrefix);
    }
}
