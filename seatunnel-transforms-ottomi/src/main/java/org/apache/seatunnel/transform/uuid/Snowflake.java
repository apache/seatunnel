package org.apache.seatunnel.transform.uuid;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.util.IdUtil;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class Snowflake implements ZetaUDF {
    // 雪花算法生成唯一ID
    private static final cn.hutool.core.lang.Snowflake snowflake = IdUtil.getSnowflake();

    @Override
    public String functionName() {
        return "SNOWFLAKE";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        return snowflake.nextIdStr();
    }
}
