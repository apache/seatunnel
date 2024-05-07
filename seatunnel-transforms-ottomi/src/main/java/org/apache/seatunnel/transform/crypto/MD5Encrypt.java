package org.apache.seatunnel.transform.crypto;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class MD5Encrypt implements ZetaUDF {

    @Override
    public String functionName() {
        return "MD5";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String data = (String) args.get(0);
        if (StrUtil.isNotEmpty(data)) {
            return SecureUtil.md5(data);
        }
        return null;
    }
}
