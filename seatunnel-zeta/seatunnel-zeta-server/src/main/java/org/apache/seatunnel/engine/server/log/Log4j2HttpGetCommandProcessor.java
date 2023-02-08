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

import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpGetCommand;
import com.hazelcast.internal.ascii.rest.HttpGetCommandProcessor;
import com.hazelcast.internal.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.util.Map;

public class Log4j2HttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    private final HttpGetCommandProcessor original;

    public Log4j2HttpGetCommandProcessor(TextCommandService textCommandService) {
        this(textCommandService, new HttpGetCommandProcessor(textCommandService));
    }

    public Log4j2HttpGetCommandProcessor(TextCommandService textCommandService,
                                         HttpGetCommandProcessor httpGetCommandProcessor) {
        super(textCommandService, textCommandService.getNode().getLogger(Log4j2HttpGetCommandProcessor.class));
        this.original = httpGetCommandProcessor;
    }

    @Override
    public void handleRejection(HttpGetCommand request) {
        handle(request);
    }

    @Override
    public void handle(HttpGetCommand request) {
        String uri = request.getURI();
        if (uri.startsWith(HttpCommandProcessor.URI_LOG_LEVEL)) {
            outputAllLoggerLevel(request);
        } else {
            original.handle(request);
        }
    }

    /**
     * Request example:
     *
     * GET {@link HttpCommandProcessor#URI_LOG_LEVEL}
     *
     * Response Body(application/json):
     *
     * {
     *     "root": "INFO"
     *     "com.example.logger1": "ERROR"
     * }
     *
     */
    private void outputAllLoggerLevel(HttpGetCommand request) {
        JsonObject jsonObject = new JsonObject();

        LoggerContext loggerContext = LoggerContext.getContext(false);
        Map<String, LoggerConfig> loggers = loggerContext.getConfiguration().getLoggers();
        for (String logger : loggers.keySet()) {
            LoggerConfig config = loggers.get(logger);
            if (LogManager.ROOT_LOGGER_NAME.equals(logger)) {
                logger = LoggerConfig.ROOT;
            }
            jsonObject.set(logger, config.getLevel().name());
        }

        prepareResponse(request, jsonObject);
        textCommandService.sendResponse(request);
    }
}
