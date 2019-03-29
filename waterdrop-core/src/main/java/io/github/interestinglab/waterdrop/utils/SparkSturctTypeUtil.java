package io.github.interestinglab.waterdrop.utils;


import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import io.github.interestinglab.waterdrop.config.ConfigRuntimeException;

import java.util.Map;


public class SparkSturctTypeUtil {

    public static StructType getStructType(StructType schema, JSONObject json) {
        StructType newSchema = schema.copy(schema.fields());
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            String field = entry.getKey();
            Object type = entry.getValue();
            if (type instanceof JSONObject) {
                StructType st = getStructType(new StructType(), (JSONObject) type);
                newSchema = newSchema.add(field, st);
            } else {
                newSchema = newSchema.add(field, getType(type.toString()));
            }
        }
        return newSchema;
    }

    private static DataType getType(String type) {
        DataType dataType = DataTypes.NullType;
        switch (type.toLowerCase()) {
            case "string":
                dataType = DataTypes.StringType;
                break;
            case "integer":
                dataType = DataTypes.IntegerType;
                break;
            case "long":
                dataType = DataTypes.LongType;
                break;
            case "double":
                dataType = DataTypes.DoubleType;
                break;
            case "float":
                dataType = DataTypes.FloatType;
                break;
            case "short":
                dataType = DataTypes.ShortType;
                break;
            case "date":
                dataType = DataTypes.DateType;
                break;
            case "timestamp":
                dataType = DataTypes.TimestampType;
                break;
            case "boolean":
                dataType = DataTypes.BooleanType;
                break;
            case "binary":
                dataType = DataTypes.BinaryType;
                break;
            case "byte":
                dataType = DataTypes.ByteType;
                break;
            default:
                throw new ConfigRuntimeException("Throw data type exception, unknown type: " + type);
        }
        return dataType;
    }
}
