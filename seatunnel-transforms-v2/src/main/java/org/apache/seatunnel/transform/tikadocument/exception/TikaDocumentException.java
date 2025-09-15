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

package org.apache.seatunnel.transform.tikadocument.exception;

import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.tikadocument.TikaDocumentTransformErrorCode;

/** Exception for TikaDocument Transform errors */
public class TikaDocumentException extends TransformException {

    public TikaDocumentException(String message) {
        super(TikaDocumentTransformErrorCode.DOCUMENT_PARSING_FAILED, message);
    }

    public TikaDocumentException(String message, Throwable cause) {
        super(
                TikaDocumentTransformErrorCode.DOCUMENT_PARSING_FAILED,
                message + ": " + cause.getMessage());
    }

    public TikaDocumentException(TikaDocumentTransformErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    /** Exception for unsupported document formats */
    public static class UnsupportedFormatException extends TikaDocumentException {
        public UnsupportedFormatException(String format) {
            super(
                    TikaDocumentTransformErrorCode.UNSUPPORTED_DOCUMENT_FORMAT,
                    "Unsupported document format: " + format);
        }
    }

    /** Exception for document parsing errors */
    public static class ParseException extends TikaDocumentException {
        public ParseException(String message) {
            super(
                    TikaDocumentTransformErrorCode.DOCUMENT_PARSING_FAILED,
                    "Document parsing failed: " + message);
        }

        public ParseException(String message, Throwable cause) {
            super("Document parsing failed: " + message, cause);
        }
    }

    /** Exception for timeout errors */
    public static class TimeoutException extends TikaDocumentException {
        public TimeoutException(long timeoutMs) {
            super(
                    TikaDocumentTransformErrorCode.DOCUMENT_PROCESSING_TIMEOUT,
                    "Document processing timed out after " + timeoutMs + " ms");
        }
    }

    /** Exception for invalid configuration */
    public static class ConfigurationException extends TikaDocumentException {
        public ConfigurationException(String message) {
            super(
                    TikaDocumentTransformErrorCode.INVALID_CONFIGURATION,
                    "Invalid configuration: " + message);
        }
    }
}
