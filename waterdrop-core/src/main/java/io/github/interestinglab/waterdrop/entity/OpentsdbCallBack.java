/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
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