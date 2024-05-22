package org.apache.seatunnel.transform.crypto.encrypt;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.codec.Base64;
import cn.hutool.core.util.StrUtil;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class Base64Encrypt implements ZetaUDF {

    @Override
    public String functionName() {
        return "BASE64_ENC";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String data = String.valueOf(args.get(0));
        if (StrUtil.isNotEmpty(data)) {
            return Base64.encode(data.getBytes());
        }
        return null;
    }
}
