package org.apache.seatunnel.transform.crypto.encrypt;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.crypto.Constants;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SmUtil;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class Sm4Encrypt implements ZetaUDF {

    @Override
    public String functionName() {
        return "SM4_ENC";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String data = String.valueOf(args.get(0));
        if (StrUtil.isNotEmpty(data)) {
            byte[] key;
            if (args.size() == 2) {
                key = ((String) args.get(1)).getBytes();
            } else {
                key = Constants.DEFAULT_DES_KEY;
            }
            return SmUtil.sm4(key).encryptBase64(data);
        }
        return null;
    }
}
