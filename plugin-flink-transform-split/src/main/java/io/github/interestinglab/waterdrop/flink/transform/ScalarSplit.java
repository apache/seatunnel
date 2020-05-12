package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class ScalarSplit extends ScalarFunction {

    private RowTypeInfo rowTypeInfo;
    private int num;
    private String separator;

    public ScalarSplit(RowTypeInfo rowTypeInfo, int num, String separator) {
        this.rowTypeInfo = rowTypeInfo;
        this.num = num;
        this.separator = separator;
    }

    public Row eval(String str) {
        Row row = new Row(num);
        int i = 0;
        for (String s : str.split(separator, num)) {
            row.setField(i++, s);
        }
        return row;
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return rowTypeInfo;
    }
}
