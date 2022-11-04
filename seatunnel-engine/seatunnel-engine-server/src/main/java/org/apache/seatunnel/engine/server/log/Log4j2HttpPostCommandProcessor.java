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

package org.apache.seatunnel.engine.server.log;

import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;

import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpPostCommand;
import com.hazelcast.internal.ascii.rest.HttpPostCommandProcessor;
import com.hazelcast.internal.json.JsonObject;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;

public class Log4j2HttpPostCommandProcessor extends HttpCommandProcessor<HttpPostCommand> {

    private final HttpPostCommandProcessor original;

    public Log4j2HttpPostCommandProcessor(TextCommandService textCommandService) {
        this(textCommandService, new HttpPostCommandProcessor(textCommandService));
    }

    public Log4j2HttpPostCommandProcessor(TextCommandService textCommandService,
                                          HttpPostCommandProcessor httpPostCommandProcessor) {
        super(textCommandService, textCommandService.getNode().getLogger(Log4j2HttpPostCommandProcessor.class));
        this.original = httpPostCommandProcessor;
    }

    @Override
    public void handleRejection(HttpPostCommand request) {
        handle(request);
    }

    @Override
    public void handle(HttpPostCommand request) {
        String uri = request.getURI();
        if (uri.startsWith(HttpCommandProcessor.URI_LOG_LEVEL)) {
            setLoggerLevel(request);
        } else if (uri.startsWith(HttpCommandProcessor.URI_LOG_LEVEL_RESET)) {
            prepareResponse(SC_500, request, "Reset logger level endpoint disabled!");
            textCommandService.sendResponse(request);
        } else {
            original.handle(request);
        }
    }

    /**
     * Request example:
     *
     * POST {@link HttpCommandProcessor#URI_LOG_LEVEL}
     *
     * Request Body(application/text):
     *
     * your_username&your_password&com.example.logger1&ERROR
     *
     */
    @SuppressWarnings("MagicNumber")
    private void setLoggerLevel(HttpPostCommand request) {
        try {
            String[] params = decodeParamsAndAuthenticate(request, 4);
            String logger = params[2];
            String level = params[3];
            if (LoggerConfig.ROOT.equals(logger)) {
                Configurator.setRootLevel(Level.getLevel(level));
            } else {
                Configurator.setLevel(logger, Level.getLevel(level));
            }
            prepareResponse(request, new JsonObject().add("status", "SUCCESS"));
        } catch (Throwable e) {
            prepareResponse(SC_500, request, exceptionResponse(e));
        }
        textCommandService.sendResponse(request);
    }
}
