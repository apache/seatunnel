package io.github.interestinglab.waterdrop.flink.source;

import com.alibaba.fastjson.JSONObject;
import com.typesafe.config.waterdrop.Config;
import io.github.interestinglab.waterdrop.common.PropertiesUtil;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSource;
import io.github.interestinglab.waterdrop.flink.util.SchemaUtil;
import io.github.interestinglab.waterdrop.flink.util.TableUtil;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author mr_xiong
 * @date 2019-07-03 11:46
 * @description
 */
public class KafkaTableStream implements FlinkStreamSource<Row> {

    private Config config;

    private Properties kafkaParams = new Properties();
    private String topic;
    private Object schemaInfo;
    private String rowTimeField;
    private String tableName;
    private final String consumerPrefix = "consumer.";
    private long watermark;
    private String format;

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
        return null;
    }

    @Override
    public void prepare() {
        topic = config.getString("topics");
        PropertiesUtil.setProperties(config, kafkaParams, consumerPrefix,false);
        tableName = config.getString("result_table_name");
        if (config.hasPath("rowtime.field")){
            rowTimeField = config.getString("rowtime.field");
            if (config.hasPath("watermark")){
                watermark = config.getLong("watermark");
            }
        }
        String schemaContent = config.getString("schema");
        format = config.getString("source_format");
        schemaInfo = JSONObject.parse(schemaContent);
    }

    @Override
    public DataStream<Row> getData(FlinkEnvironment env) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        tableEnvironment
                .connect(getKafkaConnect())
                .withFormat(setFormat())
                .withSchema(getSchema())
                .inAppendMode()
                .registerTableSource(tableName);
        Table table = tableEnvironment.scan(tableName);
        return TableUtil.tableToDataStream(tableEnvironment,table,true);
    }

    private Schema getSchema() {
        Schema schema = new Schema();
        SchemaUtil.setSchema(schema, schemaInfo,format);
        if (StringUtils.isNotBlank(rowTimeField)){
            Rowtime rowtime = new Rowtime();
            rowtime.timestampsFromField(rowTimeField);
            rowtime.watermarksPeriodicBounded(watermark);
            schema.rowtime(rowtime);
        }
        return schema;
    }

    private Kafka getKafkaConnect(){
        Kafka kafka = new Kafka().version("universal");
        kafka.topic(topic);
        kafka.properties(kafkaParams);
        return kafka;
    }


    private FormatDescriptor setFormat(){
        try {
            return SchemaUtil.setFormat(format, config);
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new RuntimeException("format配置错误");
    }

}
