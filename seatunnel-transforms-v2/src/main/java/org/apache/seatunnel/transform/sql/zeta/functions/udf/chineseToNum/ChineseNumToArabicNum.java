package org.apache.seatunnel.transform.sql.zeta.functions.udf.chineseToNum;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.util.List;

// 中文数字转阿拉伯数字
@AutoService(ZetaUDF.class)
public class ChineseNumToArabicNum implements ZetaUDF {

    @Override
    public String functionName() {
        return "CHINESE_TO_NUM";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.LONG_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String data = (String) args.get(0);
        if (StringUtils.isBlank(data)) {
            return data;
        }
        return CNToNumber.convertToArabic(data);
    }
}
