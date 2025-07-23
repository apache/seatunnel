package org.apache.seatunnel.engine.server.rest;

import org.apache.seatunnel.engine.server.rest.servlet.BaseServlet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BaseServletTest {

    private BaseServlet baseServlet;
    private NodeEngineImpl mockNodeEngine;
    private HttpServletResponse mockResponse;
    private StringWriter stringWriter;

    @BeforeEach
    void setUp() throws IOException {
        mockNodeEngine = mock(NodeEngineImpl.class);
        baseServlet = new BaseServlet(mockNodeEngine);
        mockResponse = mock(HttpServletResponse.class);
        stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        when(mockResponse.getWriter()).thenReturn(printWriter);
    }

    private JsonObject createJsonObject() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("Jack", "li");
        return jsonObject;
    }

    private JsonArray createJsonArray() {
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(createJsonObject());
        return jsonArray;
    }

    @Test
    void testWriteJsonWithObject() throws IOException {
        JsonObject jsonObject = createJsonObject();
        baseServlet.writeJsonForTest(mockResponse, jsonObject, "1");

        verify(mockResponse).setCharacterEncoding("UTF-8");
        verify(mockResponse).setContentType("application/json; charset=UTF-8");
        assertEquals(new Gson().toJson(jsonObject), stringWriter.toString());
    }

    @Test
    void testWriteJsonWithObjectStatusCode() throws IOException {
        JsonObject jsonObject = createJsonObject();
        baseServlet.writeJsonForTest(mockResponse, jsonObject, "6");

        verify(mockResponse).setCharacterEncoding("UTF-8");
        verify(mockResponse).setContentType("application/json; charset=UTF-8");
        assertEquals(new Gson().toJson(jsonObject), stringWriter.toString());
    }

    @Test
    void testWriteJsonWithJsonArray() throws IOException {
        JsonArray jsonArray = createJsonArray();
        baseServlet.writeJsonForTest(mockResponse, jsonArray, "2");

        verify(mockResponse).setCharacterEncoding("UTF-8");
        verify(mockResponse).setContentType("application/json; charset=UTF-8");
        assertEquals(jsonArray.toString(), stringWriter.toString());
    }

    @Test
    void testWriteJsonWithJsonObject() throws IOException {
        JsonObject jsonObject = createJsonObject();
        baseServlet.writeJsonForTest(mockResponse, jsonObject, "3");

        verify(mockResponse).setCharacterEncoding("UTF-8");
        verify(mockResponse).setContentType("application/json; charset=UTF-8");
        assertEquals(jsonObject.toString(), stringWriter.toString());
    }

    @Test
    void testWriteJsonWithJsonArrayStatusCode() throws IOException {
        JsonArray jsonArray = createJsonArray();
        baseServlet.writeJsonForTest(mockResponse, jsonArray, "4");

        verify(mockResponse).setCharacterEncoding("UTF-8");
        verify(mockResponse).setContentType("application/json; charset=UTF-8");
        assertEquals(jsonArray.toString(), stringWriter.toString());
    }

    @Test
    void testWriteJsonWithJsonObjectStatusCode() throws IOException {
        JsonObject jsonObject = createJsonObject();
        baseServlet.writeJsonForTest(mockResponse, jsonObject, "5");

        verify(mockResponse).setCharacterEncoding("UTF-8");
        verify(mockResponse).setContentType("application/json; charset=UTF-8");
        assertEquals(jsonObject.toString(), stringWriter.toString());
    }

    @Test
    void testWriteTextPlainWithJsonObject() throws IOException {
        JsonObject jsonObject = createJsonObject();
        baseServlet.writeJsonForTest(mockResponse, jsonObject, "7");

        verify(mockResponse).setCharacterEncoding("UTF-8");
        verify(mockResponse).setContentType("text/plain; charset=UTF-8");
        assertEquals(jsonObject.toString(), stringWriter.toString());
    }

    @Test
    void testWriteHtmlWithJsonObject() throws IOException {
        JsonObject jsonObject = createJsonObject();
        baseServlet.writeJsonForTest(mockResponse, jsonObject, "8");

        verify(mockResponse).setCharacterEncoding("UTF-8");
        verify(mockResponse).setContentType("text/html; charset=UTF-8");
        assertEquals(jsonObject.toString(), stringWriter.toString());
    }
}
