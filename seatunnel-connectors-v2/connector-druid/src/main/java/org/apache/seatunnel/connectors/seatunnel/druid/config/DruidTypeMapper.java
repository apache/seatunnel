package org.apache.seatunnel.connectors.seatunnel.druid.config;


import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.util.HashMap;
import java.util.Map;

/**
 * guanbo
 */
public class DruidTypeMapper {
    public static final  Map<String, SeaTunnelDataType<?>> informationMapping = new HashMap<>();

    static{
            // https://druid.apache.org/docs/latest/querying/sql.html#data-types
            informationMapping.put("CHAR", BasicType.STRING_TYPE);
            informationMapping.put("VARCHAR", BasicType.STRING_TYPE);
            informationMapping.put("DECIMAL", BasicType.DOUBLE_TYPE);
            informationMapping.put("FLOAT", BasicType.FLOAT_TYPE);
            informationMapping.put("REAL", BasicType.DOUBLE_TYPE);
            informationMapping.put("DOUBLE", BasicType.DOUBLE_TYPE);
            informationMapping.put("BOOLEAN", BasicType.LONG_TYPE);
            informationMapping.put("TINYINT", BasicType.LONG_TYPE);
            informationMapping.put("SMALLINT", BasicType.LONG_TYPE);
            informationMapping.put("INTEGER", BasicType.LONG_TYPE);
            informationMapping.put("BIGINT", BasicType.LONG_TYPE);
            informationMapping.put("TIMESTAMP", LocalTimeType.LOCAL_DATE_TIME_TYPE);
            informationMapping.put("DATE", LocalTimeType.LOCAL_DATE_TYPE);
    }



}
