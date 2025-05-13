package org.apache.seatunnel.connectors.seatunnel.prometheus;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceParameter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PrometheusParamCheckTest {

    @Test
    public void checkTime(){
        final PrometheusSourceParameter prometheusSourceParameter = new PrometheusSourceParameter();
        Map<String,Object> map1 = new HashMap<>();
        map1.put("url","http://localhost:9090");
        map1.put("query","node_cpu_seconds_total");
        map1.put("query_type","Range");
        map1.put("start","2025-05-13T02:25:23Z");
        map1.put("end","2025-05-13T02:25:23.001Z");
        prometheusSourceParameter.buildWithConfig(ReadonlyConfig.fromMap(map1));

        Map<String,Object> map2 = new HashMap<>();
        map2.put("url","http://localhost:9090");
        map2.put("query","node_cpu_seconds_total");
        map2.put("query_type","Range");
        map2.put("start","2025-05-13T02:25:23Z");
        map2.put("end","2025-05-13T02:25:23.001");
        Assertions.assertThrows(Exception.class, ()-> prometheusSourceParameter.buildWithConfig(ReadonlyConfig.fromMap(map2)));

        Map<String,Object> map3 = new HashMap<>();
        map3.put("url","http://localhost:9090");
        map3.put("query","node_cpu_seconds_total");
        map3.put("query_type","Range");
        map3.put("start","1747103123.083");
        map3.put("end","1747106723");
        prometheusSourceParameter.buildWithConfig(ReadonlyConfig.fromMap(map3));

        Map<String,Object> map4 = new HashMap<>();
        map4.put("url","http://localhost:9090");
        map4.put("query","node_cpu_seconds_total");
        map4.put("query_type","Range");
        map4.put("start","CURRENT_TIMESTAMP");
        map4.put("end","CURRENT_TIMESTAMP");
        prometheusSourceParameter.buildWithConfig(ReadonlyConfig.fromMap(map4));
    }
}
