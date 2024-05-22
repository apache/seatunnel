package org.apache.seatunnel.transform.crypto.encrypt;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.util.HexUtil;
import cn.hutool.core.util.StrUtil;
import com.google.auto.service.AutoService;

import java.nio.charset.StandardCharsets;
import java.util.List;

@AutoService(ZetaUDF.class)
public class HexEncrypt implements ZetaUDF {

    @Override
    public String functionName() {
        return "HEX_ENC";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String data = String.valueOf(args.get(0));
        if (StrUtil.isNotEmpty(data)) {
            return HexUtil.encodeHexStr(data.getBytes(StandardCharsets.UTF_8));
        }
        return null;
    }
}
