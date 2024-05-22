package org.apache.seatunnel.transform.crypto.encrypt;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.symmetric.DES;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class DesEncrypt implements ZetaUDF {

    @Override
    public String functionName() {
        return "DES_ENC";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String data = String.valueOf(args.get(0));
        if (StrUtil.isNotEmpty(data)) {
            DES des;
            if (args.size() == 2) {
                des = SecureUtil.des(String.valueOf(args.get(1)).getBytes());
            } else {
                des = SecureUtil.des();
            }
            return des.encryptBase64(data);
        }
        return null;
    }
}
