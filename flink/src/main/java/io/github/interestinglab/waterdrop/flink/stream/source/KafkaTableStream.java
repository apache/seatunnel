package io.github.interestinglab.waterdrop.flink.stream.source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnv;
import io.github.interestinglab.waterdrop.flink.utils.PropertiesUtil;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Properties;

/**
 * @author mr_xiong
 * @date 2019-07-03 11:46
 * @description
 */
public class KafkaTableStream extends InternalFlinkStreamSource<Void> {

    private Properties kafkaParams = new Properties();
    private String topic;
    private JSONObject jsonDemo;
    private String rowTimeField;
    private String tableName;
    private final String consumerPrefix = "consumer.";

    @Override
    public CheckResult checkConfig() {
        return null;
    }

    @Override
    public void prepare() {
        topic = config.getString("topics");
        PropertiesUtil.setProperties(config, kafkaParams, consumerPrefix);
        tableName = config.getString("table_name");
        String sourceContent = config.getString("source_content");
        if (config.hasPath("rowtime.field")){
            rowTimeField = config.getString("rowtime.field");
        }
        jsonDemo = JSONObject.parseObject(sourceContent);
    }

    @Override
    public DataStream<Void> getData(FlinkStreamEnv env) {
        env.getTableEnvironment()
                .connect(getKafkaConnect())
                .withFormat(setFormat())
                .withSchema(setSchema())
                .inAppendMode()
                .registerTableSink(tableName);
        return null;
    }

    private Schema setSchema() {
        Schema schema = new Schema();
        setSchema(schema,jsonDemo);
        if (StringUtils.isNotBlank(rowTimeField)){
            Rowtime rowtime = new Rowtime();
            rowtime.timestampsFromField(rowTimeField);
            rowtime.watermarksPeriodicAscending();
            schema.rowtime(rowtime);
        }
        return schema;
    }

    private Kafka getKafkaConnect(){

        Kafka kafka = new Kafka().version("0.10");
        kafka.topic(topic);
        kafka.properties(kafkaParams);
        return kafka;
    }
    private void setSchema(Schema schema, JSONObject json) {

        for (Map.Entry<String, Object> entry : json.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof String) {
                schema.field(key, Types.STRING());
            } else if (value instanceof Integer) {
                schema.field(key, Types.INT());
            } else if (value instanceof Long) {
                schema.field(key, Types.LONG());
            } else if (value instanceof BigDecimal) {
                schema.field(key, Types.JAVA_BIG_DEC());
            } else if (value instanceof JSONObject) {
                schema.field(key, getTypeInformation((JSONObject) value));
            }
        }
    }

    private  TypeInformation getTypeInformation(JSONObject json) {
        int size = json.size();
        String[] fields = new String[size];
        TypeInformation[] informations = new TypeInformation[size];
        int i = 0;
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            fields[i] = key;
            if (value instanceof String) {
                informations[i] = Types.STRING();
            } else if (value instanceof Integer) {
                informations[i] = Types.INT();
            } else if (value instanceof Long) {
                informations[i] = Types.LONG();
            } else if (value instanceof BigDecimal) {
                informations[i] = Types.JAVA_BIG_DEC();
            } else if (value instanceof JSONObject) {
                informations[i] = getTypeInformation((JSONObject) value);
            } else if (value instanceof JSONArray) {
                JSONObject demo = ((JSONArray) value).getJSONObject(0);
                informations[i] = ObjectArrayTypeInfo.getInfoFor(Row[].class, getTypeInformation(demo));
            }
            i++;
        }
        return new RowTypeInfo(informations, fields);
    }

    private FormatDescriptor setFormat(){
        return new Json().failOnMissingField(false).deriveSchema();
    }

}
