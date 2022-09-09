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

package org.apache.seatunnel.app.controller;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.apache.seatunnel.app.WebMvcApplicationTest;
import org.apache.seatunnel.app.common.Result;
import org.apache.seatunnel.app.dal.dao.impl.UserDaoImpl;
import org.apache.seatunnel.app.domain.request.user.AddUserReq;
import org.apache.seatunnel.app.domain.response.user.AddUserRes;
import org.apache.seatunnel.common.utils.JsonUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;

@Disabled("todo:this test is not working, waiting fix")
public class UserControllerTest extends WebMvcApplicationTest {

    @MockBean
    private UserDaoImpl userDaoImpl;

    @Test
    public void testAdd() throws Exception {
        AddUserReq requestDTO = new AddUserReq();
        requestDTO.setUsername("admin");
        requestDTO.setPassword("123456");
        requestDTO.setStatus(new Byte("1"));
        requestDTO.setType(new Byte("1"));
        String url = "/api/v1/user/user";
        when(this.userDaoImpl.add(Mockito.any())).thenReturn(1);
        MvcResult mvcResult = mockMvc.perform(post(url).contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJsonString(requestDTO)))
                .andExpect(status().isOk())
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        String result = mvcResult.getResponse().getContentAsString();
        Result<AddUserRes> data = JsonUtils.parseObject(result, new TypeReference<Result<AddUserRes>>() {
        });
        Assertions.assertTrue(data.isSuccess());
        Assertions.assertNotNull(data.getData());
    }

}
