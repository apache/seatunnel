package org.apache.seatunnel.transform.crypto.decrypt;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.crypto.Constants;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.google.auto.service.AutoService;

import java.nio.charset.Charset;
import java.util.List;

@AutoService(ZetaUDF.class)
public class AesDecrypt implements ZetaUDF {

    @Override
    public String functionName() {
        return "AES_DEC";
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
            return SecureUtil.aes(key).decryptStr(data, Charset.forName(Constants.CRYPTO_CHARSET));
        }
        return null;
    }
}
