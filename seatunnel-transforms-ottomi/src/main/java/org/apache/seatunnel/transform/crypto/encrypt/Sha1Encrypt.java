package org.apache.seatunnel.transform.crypto.encrypt;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class Sha1Encrypt implements ZetaUDF {

    @Override
    public String functionName() {
        return "SHA1_ENC";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String data = String.valueOf(args.get(0));
        if (StrUtil.isNotEmpty(data)) {
            return SecureUtil.sha1().digestHex(data);
        }
        return null;
    }
}
