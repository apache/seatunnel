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

package org.apache.seatunnel.transform.nlpmodel.embedding.multimodal;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.transform.nlpmodel.embedding.remote.AbstractModel;

import java.io.IOException;
import java.util.List;

/**
 * Abstract base class for multimodal embedding models that can handle text, image, and video data
 */
public abstract class MultimodalModel extends AbstractModel {

    public MultimodalModel(Integer vectorizedNumber) {
        super(vectorizedNumber);
    }

    @Override
    protected final List<List<Float>> vector(Object[] fields) throws IOException {
        if (isMultimodalFields(fields)) {
            return multimodalVector(fields);
        } else {
            return textVector(fields);
        }
    }

    protected abstract List<List<Float>> textVector(Object[] fields) throws IOException;

    protected abstract List<List<Float>> multimodalVector(Object[] fields) throws IOException;

    /** Check if the given fields contain multimodal data */
    @VisibleForTesting
    public boolean isMultimodalFields(Object[] fields) {
        if (fields == null || fields.length == 0) {
            return false;
        }
        if (fields[0] instanceof MultimodalFieldValue) {
            return true;
        }
        return false;
    }
}
