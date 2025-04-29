package org.apache.seatunnel.connectors.seatunnel.inlong.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class InlongUtil {
    private static final Logger LOG = LoggerFactory.getLogger(InlongUtil.class);

    public static void silenceSleepInMs(long millisecond) {
        try {
            TimeUnit.MILLISECONDS.sleep(millisecond);
        } catch (Exception e) {
            LOG.warn("error in silence sleep: ", e);
        }
    }
}
