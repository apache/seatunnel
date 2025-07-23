package org.apache.seatunnel.engine.server.rest;

import org.junit.jupiter.api.Test;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JsonWriterTest {

    @Test
    void testWriteJson() throws IOException {
        // 准备测试数据
        HttpServletResponse resp = mock(HttpServletResponse.class);
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        when(resp.getWriter()).thenReturn(printWriter);

        TestObject testObj = new TestObject("测试", 123);
        String expectedJson = "{\"name\":\"测试\",\"value\":123}";

        // 执行测试方法
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("application/json");
        resp.getWriter().write(new com.google.gson.Gson().toJson(testObj));

        // 验证结果
        verify(resp).setCharacterEncoding(StandardCharsets.UTF_8.name());
        verify(resp).setContentType("application/json");
        assertEquals(expectedJson, stringWriter.toString());
    }

    @Test
    void testOutputStreamChineseEncoding() throws Exception {
        HttpServletResponse resp = mock(HttpServletResponse.class);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        when(resp.getOutputStream()).thenReturn(new ServletOutputStream() {
            @Override
            public boolean isReady() {
                return false;
            }

            @Override
            public void setWriteListener(WriteListener writeListener) {

            }

            public void write(int b) {
                baos.write(b);
            }
        });

        TestObject testObj = new TestObject("测试", 123);
        String expectedJson = "{\"name\":\"测试\",\"value\":123}";
        byte[] expectedBytes = expectedJson.getBytes(StandardCharsets.UTF_8);

        // 执行测试
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("application/json");
        resp.getOutputStream().write(expectedBytes);

        // 验证字节级编码
        verify(resp).setCharacterEncoding(StandardCharsets.UTF_8.name());
        verify(resp).setContentType("application/json");
        assertArrayEquals(expectedBytes, baos.toByteArray());
    }

    // 测试用内部类
    private static class TestObject {
        private String name;
        private int value;

        public TestObject(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }
}

