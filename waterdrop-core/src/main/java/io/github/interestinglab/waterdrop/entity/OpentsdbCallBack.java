package io.github.interestinglab.waterdrop.entity;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OpentsdbCallBack implements FutureCallback<HttpResponse> {

    private String postBody = "";

    private static Logger logger = LoggerFactory.getLogger(OpentsdbCallBack.class);

    public OpentsdbCallBack(String postBody) {
        this.postBody = postBody;
    }

    public void completed(HttpResponse response) {

        int status = response.getStatusLine().getStatusCode();
        String result = getHttpContent(response);
        if(status == 200)
        {
            logger.debug(String.format("[OpentsdbCallBack] Data send to opentsdb successfully, and response is：" + result));
        }else{
            logger.error(String.format("[OpentsdbCallBack] Data send to opentsdb failed, and response is：: %s,and source data is : %s",result,postBody));
        }
    }

    public void cancelled() {
        logger.info("Send to Opentsdb cancelled");
    }

    public void failed(Exception e) {

        logger.error(String.format("[OpentsdbCallBack] Data send to opentsdb failed, and response is : %s,and source data is :%s",e.getMessage(),postBody));
        e.printStackTrace();
    }


    protected String getHttpContent(HttpResponse response) {

        HttpEntity entity = response.getEntity();
        String body = null;

        if (entity == null) {
            return null;
        }
        try {
            body = EntityUtils.toString(entity, "utf-8");

        } catch (ParseException e) {

            logger.warn("the response's content inputstream is corrupt", e);
        } catch (IOException e) {

            logger.warn("the response's content inputstream is corrupt", e);
        }
        return body;
    }
}