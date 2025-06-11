package org.apache.seatunnel.format.sensorsdata.record;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.sensorsdata.analytics.javasdk.SensorsConst.DEFAULT_LIB_DETAIL;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_DETAIL_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_METHOD_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_VERSION_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.SDK_VERSION;

public class SensorsDataLibInfo {

    public static final Map<String, String> LIB_INFO =
            ImmutableMap.<String, String>builder()
                    .put(LIB_SYSTEM_ATTR, LIB)
                    .put(LIB_VERSION_SYSTEM_ATTR, SDK_VERSION)
                    .put(LIB_METHOD_SYSTEM_ATTR, "code")
                    .put(LIB_DETAIL_SYSTEM_ATTR, DEFAULT_LIB_DETAIL)
                    .build();
}
