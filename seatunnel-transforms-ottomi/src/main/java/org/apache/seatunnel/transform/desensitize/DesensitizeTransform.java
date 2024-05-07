package org.apache.seatunnel.transform.desensitize;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.util.DesensitizedUtil;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class DesensitizeTransform implements ZetaUDF {
    @Override
    public String functionName() {
        return "DESENSITIZED";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String data = (String) args.get(0);
        String desensitizedTypeName = (String) args.get(1);
        DesensitizedUtil.DesensitizedType desensitizedType =
                DesensitizedUtil.DesensitizedType.valueOf(desensitizedTypeName);
        return DesensitizedUtil.desensitized(data, desensitizedType);
    }
}
