package org.apache.seatunnel.connectors.seatunnel.wechat.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.sink.HttpSinkWriter;

import java.io.IOException;
import java.util.HashMap;

public class WeChatHttpSinkWriter extends HttpSinkWriter {
    private static final String WECHAT_SEND_MSG_SUPPORT_TYPE = "text";
    private static final String WECHAT_SEND_MSG_TYPE_KEY = "msgtype";
    private static final String WECHAT_SEND_MSG_CONTENT_KEY = "content";

    public WeChatHttpSinkWriter(HttpParameter httpParameter) {
        //new SeaTunnelRowType can match SeaTunnelRowWrapper fields sequence
        super(new SeaTunnelRowType(new String[]{WECHAT_SEND_MSG_TYPE_KEY, WECHAT_SEND_MSG_SUPPORT_TYPE}, new SeaTunnelDataType[]{BasicType.VOID_TYPE, BasicType.VOID_TYPE}), httpParameter);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        HashMap<Object, Object> objectMap = new HashMap<>();
        objectMap.put(WECHAT_SEND_MSG_CONTENT_KEY, element.toString());
        //SeaTunnelRowWrapper can used to post wechat web hook
        SeaTunnelRow wechatRowWrapper = new SeaTunnelRow(new Object[]{WECHAT_SEND_MSG_SUPPORT_TYPE, objectMap});
        super.write(wechatRowWrapper);
    }
}
