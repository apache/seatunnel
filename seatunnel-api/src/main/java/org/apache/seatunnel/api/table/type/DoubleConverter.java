
package org.apache.seatunnel.api.table.type;

public class DoubleConverter implements Converter<DoubleType> {
    @Override
    public DoubleType convert(String originValue) {
        return new DoubleType(Double.parseDouble(originValue));
    }
}
