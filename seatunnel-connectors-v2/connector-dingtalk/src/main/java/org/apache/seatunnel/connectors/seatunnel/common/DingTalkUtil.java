/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.common;

import com.aliyun.dingtalkoauth2_1_0.models.GetAccessTokenResponse;
import java.io.IOException;

/**
 * @description: Ding Talk util
 **/

public class DingTalkUtil {

    /**
     * @return Client
     */
    public static com.aliyun.dingtalkoauth2_1_0.Client createClient() throws Exception {
        com.aliyun.teaopenapi.models.Config config = new com.aliyun.teaopenapi.models.Config();
        config.protocol = "https";
        config.regionId = "central";
        return new com.aliyun.dingtalkoauth2_1_0.Client(config);
    }

    public static String getAppToken(String appKey, String appSecret) {
        String appToken = null;
        try {
            com.aliyun.dingtalkoauth2_1_0.Client client = DingTalkUtil.createClient();
            com.aliyun.dingtalkoauth2_1_0.models.GetAccessTokenRequest getAccessTokenRequest = new com.aliyun.dingtalkoauth2_1_0.models.GetAccessTokenRequest()
                .setAppKey(appKey).setAppSecret(appSecret);
            GetAccessTokenResponse res = client.getAccessToken(getAccessTokenRequest);
            appToken = res.getBody().getAccessToken();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return appToken;
    }
}
