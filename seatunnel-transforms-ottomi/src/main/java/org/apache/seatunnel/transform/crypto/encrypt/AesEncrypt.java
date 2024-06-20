package org.apache.seatunnel.transform.crypto.encrypt;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.symmetric.AES;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class AesEncrypt implements ZetaUDF {

    @Override
    public String functionName() {
        return "AES_ENC";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String data = String.valueOf(args.get(0));
        if (StrUtil.isNotEmpty(data)) {
            AES aes;
            if (args.size() == 2) {
                aes = SecureUtil.aes(String.valueOf(args.get(1)).getBytes());
            } else {
                aes = SecureUtil.aes();
            }
            return aes.encryptBase64(data);
        }
        return null;
    }
}