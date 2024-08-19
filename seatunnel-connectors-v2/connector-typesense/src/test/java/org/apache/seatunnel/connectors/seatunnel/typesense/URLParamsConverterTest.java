package org.apache.seatunnel.connectors.seatunnel.typesense;

import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.typesense.util.URLParamsConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class URLParamsConverterTest {

    @Test
    public void convertParamsToJson() {
        String json = URLParamsConverter.convertParamsToJson("q=*&filter_by=num_employees:10");
        Assertions.assertEquals(json, "{\"q\":\"*\",\"filter_by\":\"num_employees:10\"}");
        Assertions.assertThrows(
                TypesenseConnectorException.class,
                () -> URLParamsConverter.convertParamsToJson("q=*&filter_by=num_employees:10&b"));
    }
}
