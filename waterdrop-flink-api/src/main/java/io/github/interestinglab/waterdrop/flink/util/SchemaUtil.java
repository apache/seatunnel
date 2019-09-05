package io.github.interestinglab.waterdrop.flink.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author mr_xiong
 * @date 2019-08-29 18:01
 * @description
 */
public class SchemaUtil {

    public static void setSchema(Schema schema, Object info, String format) {

        switch (format.toLowerCase()) {
            case "json":
                getJsonSchema(schema, (JSONObject) info);
                break;
            case "csv":
                getCsvSchema(schema, (List<Map<String, String>>) info);
                break;
            case "orc":
                getOrcSchema(schema, (JSONObject) info);
                break;
            case "avro":
                getAvroSchema(schema, (JSONObject) info);
                break;
            case "parquet":
                getParquetSchema(schema, (JSONObject) info);
            default:
        }
    }

    public static FormatDescriptor setFormat(String format, Config config) throws Exception{
        FormatDescriptor formatDescriptor = null;
        switch (format.toLowerCase()) {
            case "json":
                formatDescriptor = new Json().failOnMissingField(false).deriveSchema();
                break;
            case "csv":
                Csv csv = new Csv().deriveSchema();
                Field interPro = csv.getClass().getDeclaredField("internalProperties");
                interPro.setAccessible(true);
                Object desc = interPro.get(csv);
                Class<DescriptorProperties> descCls = DescriptorProperties.class;
                Method putMethod = descCls.getDeclaredMethod("put", String.class, String.class);
                putMethod.setAccessible(true);
                for (Map.Entry<String, ConfigValue> entry : config.entrySet()){
                    String key = entry.getKey();
                    if (key.startsWith("format.")){
                        String value = config.getString(key);
                        putMethod.invoke(desc,key,value);
                    }
                }
                formatDescriptor = csv;
                break;
            case "orc":
                break;
            case "avro":
                break;
            case "parquet":
                break;
            default:
        }
        return formatDescriptor;
    }

    private static void getJsonSchema(Schema schema, JSONObject json) {

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

    private static void getCsvSchema(Schema schema, List<Map<String, String>> schemaList) {

        for (Map<String, String> map : schemaList) {
            String field = map.get("field");
            String type = map.get("type").toUpperCase();
            schema.field(field, type);
        }
    }


    /**
     * todo
     * @param schema
     * @param json
     */
    private static void getOrcSchema(Schema schema, JSONObject json) {

    }


    /**
     * todo
     * @param schema
     * @param json
     */
    private static void getParquetSchema(Schema schema, JSONObject json) {

    }

    /**
     * todo
     * @param schema
     * @param json
     */
    private static void getAvroSchema(Schema schema, JSONObject json) {

    }

    private static TypeInformation getTypeInformation(JSONObject json) {
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
}
