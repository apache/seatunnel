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
package org.apache.seatunnel.connectors.seatunnel.apptivo;

import org.apache.seatunnel.api.source.SourceSplit;

/**
 * Stores one bounded, credential-free Apptivo scan position.
 *
 * <p>The logical options, observed firm, resource and accepted row offset are enough to resume a
 * page, including a singleton cases configuration or an already completed scan.
 */
final class ApptivoSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    // Fingerprint of origin, configured firm, ordered resources and request page width.
    private final String scope;

    // Store the first provider firm ID independently of credential rotation and resource
    // boundaries.
    String firm;
    int resource;
    int offset;
    int row;
    int pageNumber;
    Long total;
    String digest;
    String preceding;

    ApptivoSplit(ApptivoConfig config) {
        this(config.scope, config.firm);
    }

    private ApptivoSplit(String scope, String firm) {
        this.scope = scope;
        this.firm = firm;
    }

    @Override
    public String splitId() {
        return "apptivo-catalog";
    }

    /**
     * Creates a stable snapshot separate from the reader's live scalar positions.
     *
     * <p>Credential material, response bodies and HTTP state are outside this type.
     */
    ApptivoSplit copy() {
        ApptivoSplit copy = new ApptivoSplit(scope, firm);
        copy.resource = resource;
        copy.offset = offset;
        copy.row = row;
        copy.pageNumber = pageNumber;
        copy.total = total;
        copy.digest = digest;
        copy.preceding = preceding;
        return copy;
    }

    boolean finished(ApptivoConfig config) {
        return resource == config.resources.size();
    }

    void validate(ApptivoConfig config) {
        if (!config.scope.equals(scope)
                || config.firm != null && !config.firm.equals(firm)
                || resource < 0
                || resource > config.resources.size()
                || offset < 0
                || row < 0
                || pageNumber < 0
                || row > config.pageSize
                || finished(config)
                        && (offset != 0 || row != 0 || digest != null || total != null)) {
            throw new IllegalStateException(
                    "Apptivo checkpoint has incompatible scan options or state");
        }
    }

    void nextResource() {
        ++resource;
        offset = 0;
        row = 0;
        pageNumber = 0;
        total = null;
        digest = null;
        preceding = null;
    }

    void nextPage(ApptivoResource active, int records, int maxPages, ApptivoApi.Page page) {
        if (records <= 0 || pageNumber + 1 >= maxPages) {
            throw active.failure("pagination exceeded max_pages_per_resource");
        }
        ++pageNumber;
        offset = Math.addExact(offset, records);
        row = 0;
        total = page.total;
        preceding = page.digest;
        digest = null;
    }
}
