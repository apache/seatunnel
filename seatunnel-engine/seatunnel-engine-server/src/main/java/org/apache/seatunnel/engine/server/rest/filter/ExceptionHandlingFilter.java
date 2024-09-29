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

package org.apache.seatunnel.engine.server.rest.filter;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.engine.server.rest.ErrResponse;

import lombok.extern.slf4j.Slf4j;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

@Slf4j
public class ExceptionHandlingFilter implements Filter {

    private ObjectMapper objectMapper;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        objectMapper = new ObjectMapper();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        try {
            chain.doFilter(request, response);
        } catch (IllegalArgumentException e) {
            handleException(HttpServletResponse.SC_BAD_REQUEST, (HttpServletResponse) response, e);
        } catch (Exception e) {
            handleException(
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    (HttpServletResponse) response,
                    e);
        }
    }

    private void handleException(int status, HttpServletResponse response, Exception e)
            throws IOException {
        response.setStatus(status);
        response.setContentType("application/json;charset=UTF-8");

        ErrResponse errorResponse = new ErrResponse();
        errorResponse.setMessage(e.getMessage());
        errorResponse.setStatus("fail");

        String jsonResponse = objectMapper.writeValueAsString(errorResponse);
        response.getWriter().write(jsonResponse);

        e.printStackTrace();
    }

    @Override
    public void destroy() {}
}
