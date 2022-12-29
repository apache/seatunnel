package org.apache.seatunnel.core.starter.seatunnel.jvm;

import static org.apache.seatunnel.core.starter.seatunnel.jvm.JavaVersion.majorVersion;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class JavaVersionTest {

    @Test
    void parse() {
        assertEquals(8, majorVersion(JavaVersion.parse("1.8")));
    }
}