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
package org.apache.seatunnel.connectors.seatunnel.assemblyai;

/**
 * Identifies the transcript catalog and its three pre-recorded audio child resources.
 *
 * <p>The redacted audio resource uses the metadata response attached to a transcript ID.
 */
enum AssemblyAIResource {
    TRANSCRIPTS("transcripts", null),
    TRANSCRIPT_SENTENCES("transcript_sentences", "sentences"),
    PARAGRAPHS("paragraphs", "paragraphs"),
    REDACTED_AUDIO("redacted_audio", "redacted-audio");

    final String label;
    final String route;

    AssemblyAIResource(String label, String route) {
        this.label = label;
        this.route = route;
    }

    Failure failure(String message) {
        return new Failure("AssemblyAI " + label + ": " + message);
    }

    /**
     * Contains diagnostics with the logical resource and a fixed description.
     *
     * <p>Response text, requested media URLs and credentials are excluded.
     */
    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Failure(String message) {
            super(message);
        }
    }
}
