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
package org.apache.seatunnel.connectors.seatunnel.prometheus.sink.proto;

public final class Remote {
    private Remote() {}

    public static void registerAllExtensions(com.google.protobuf.ExtensionRegistryLite registry) {}

    public static void registerAllExtensions(com.google.protobuf.ExtensionRegistry registry) {
        registerAllExtensions((com.google.protobuf.ExtensionRegistryLite) registry);
    }

    public interface WriteRequestOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.WriteRequest)
            com.google.protobuf.MessageOrBuilder {

        /**
         * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
         * </code>
         */
        java.util.List<Types.TimeSeries> getTimeseriesList();

        /**
         * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
         * </code>
         */
        Types.TimeSeries getTimeseries(int index);

        /**
         * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
         * </code>
         */
        int getTimeseriesCount();

        /**
         * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
         * </code>
         */
        java.util.List<? extends Types.TimeSeriesOrBuilder> getTimeseriesOrBuilderList();

        /**
         * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
         * </code>
         */
        Types.TimeSeriesOrBuilder getTimeseriesOrBuilder(int index);

        /**
         * <code>repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
         * </code>
         */
        java.util.List<Types.MetricMetadata> getMetadataList();

        /**
         * <code>repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
         * </code>
         */
        Types.MetricMetadata getMetadata(int index);

        /**
         * <code>repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
         * </code>
         */
        int getMetadataCount();

        /**
         * <code>repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
         * </code>
         */
        java.util.List<? extends Types.MetricMetadataOrBuilder> getMetadataOrBuilderList();

        /**
         * <code>repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
         * </code>
         */
        Types.MetricMetadataOrBuilder getMetadataOrBuilder(int index);
    }

    /** Protobuf type {@code prometheus.WriteRequest} */
    public static final class WriteRequest extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.WriteRequest)
            WriteRequestOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use WriteRequest.newBuilder() to construct.
        private WriteRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private WriteRequest() {
            timeseries_ = java.util.Collections.emptyList();
            metadata_ = java.util.Collections.emptyList();
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new WriteRequest();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Remote.internal_static_prometheus_WriteRequest_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Remote.internal_static_prometheus_WriteRequest_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Remote.WriteRequest.class, Remote.WriteRequest.Builder.class);
        }

        public static final int TIMESERIES_FIELD_NUMBER = 1;

        @SuppressWarnings("serial")
        private java.util.List<Types.TimeSeries> timeseries_;

        /**
         * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public java.util.List<Types.TimeSeries> getTimeseriesList() {
            return timeseries_;
        }

        /**
         * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public java.util.List<? extends Types.TimeSeriesOrBuilder> getTimeseriesOrBuilderList() {
            return timeseries_;
        }

        /**
         * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public int getTimeseriesCount() {
            return timeseries_.size();
        }

        /**
         * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public Types.TimeSeries getTimeseries(int index) {
            return timeseries_.get(index);
        }

        /**
         * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public Types.TimeSeriesOrBuilder getTimeseriesOrBuilder(int index) {
            return timeseries_.get(index);
        }

        public static final int METADATA_FIELD_NUMBER = 3;

        @SuppressWarnings("serial")
        private java.util.List<Types.MetricMetadata> metadata_;

        /**
         * <code>repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public java.util.List<Types.MetricMetadata> getMetadataList() {
            return metadata_;
        }

        /**
         * <code>repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public java.util.List<? extends Types.MetricMetadataOrBuilder> getMetadataOrBuilderList() {
            return metadata_;
        }

        /**
         * <code>repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public int getMetadataCount() {
            return metadata_.size();
        }

        /**
         * <code>repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public Types.MetricMetadata getMetadata(int index) {
            return metadata_.get(index);
        }

        /**
         * <code>repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public Types.MetricMetadataOrBuilder getMetadataOrBuilder(int index) {
            return metadata_.get(index);
        }

        private byte memoizedIsInitialized = -1;

        @Override
        public final boolean isInitialized() {
            byte isInitialized = memoizedIsInitialized;
            if (isInitialized == 1) {
                return true;
            }
            if (isInitialized == 0) {
                return false;
            }

            memoizedIsInitialized = 1;
            return true;
        }

        @Override
        public void writeTo(com.google.protobuf.CodedOutputStream output)
                throws java.io.IOException {
            for (int i = 0; i < timeseries_.size(); i++) {
                output.writeMessage(1, timeseries_.get(i));
            }
            for (int i = 0; i < metadata_.size(); i++) {
                output.writeMessage(3, metadata_.get(i));
            }
            getUnknownFields().writeTo(output);
        }

        @Override
        public int getSerializedSize() {
            int size = memoizedSize;
            if (size != -1) {
                return size;
            }

            size = 0;
            for (int i = 0; i < timeseries_.size(); i++) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeMessageSize(
                                1, timeseries_.get(i));
            }
            for (int i = 0; i < metadata_.size(); i++) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeMessageSize(
                                3, metadata_.get(i));
            }
            size += getUnknownFields().getSerializedSize();
            memoizedSize = size;
            return size;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof Remote.WriteRequest)) {
                return super.equals(obj);
            }
            Remote.WriteRequest other = (Remote.WriteRequest) obj;

            if (!getTimeseriesList().equals(other.getTimeseriesList())) {
                return false;
            }
            if (!getMetadataList().equals(other.getMetadataList())) {
                return false;
            }
            if (!getUnknownFields().equals(other.getUnknownFields())) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            if (memoizedHashCode != 0) {
                return memoizedHashCode;
            }
            int hash = 41;
            hash = (19 * hash) + getDescriptor().hashCode();
            if (getTimeseriesCount() > 0) {
                hash = (37 * hash) + TIMESERIES_FIELD_NUMBER;
                hash = (53 * hash) + getTimeseriesList().hashCode();
            }
            if (getMetadataCount() > 0) {
                hash = (37 * hash) + METADATA_FIELD_NUMBER;
                hash = (53 * hash) + getMetadataList().hashCode();
            }
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Remote.WriteRequest parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.WriteRequest parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.WriteRequest parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.WriteRequest parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.WriteRequest parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.WriteRequest parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.WriteRequest parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Remote.WriteRequest parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Remote.WriteRequest parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Remote.WriteRequest parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Remote.WriteRequest parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Remote.WriteRequest parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        @Override
        public Builder newBuilderForType() {
            return newBuilder();
        }

        public static Builder newBuilder() {
            return DEFAULT_INSTANCE.toBuilder();
        }

        public static Builder newBuilder(Remote.WriteRequest prototype) {
            return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }

        @Override
        public Builder toBuilder() {
            return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
        }

        @Override
        protected Builder newBuilderForType(BuilderParent parent) {
            Builder builder = new Builder(parent);
            return builder;
        }

        /** Protobuf type {@code prometheus.WriteRequest} */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.WriteRequest)
                Remote.WriteRequestOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Remote.internal_static_prometheus_WriteRequest_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Remote.internal_static_prometheus_WriteRequest_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Remote.WriteRequest.class, Remote.WriteRequest.Builder.class);
            }

            // Construct using Remote.WriteRequest.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                if (timeseriesBuilder_ == null) {
                    timeseries_ = java.util.Collections.emptyList();
                } else {
                    timeseries_ = null;
                    timeseriesBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000001);
                if (metadataBuilder_ == null) {
                    metadata_ = java.util.Collections.emptyList();
                } else {
                    metadata_ = null;
                    metadataBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000002);
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Remote.internal_static_prometheus_WriteRequest_descriptor;
            }

            @Override
            public Remote.WriteRequest getDefaultInstanceForType() {
                return Remote.WriteRequest.getDefaultInstance();
            }

            @Override
            public Remote.WriteRequest build() {
                Remote.WriteRequest result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Remote.WriteRequest buildPartial() {
                Remote.WriteRequest result = new Remote.WriteRequest(this);
                buildPartialRepeatedFields(result);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartialRepeatedFields(Remote.WriteRequest result) {
                if (timeseriesBuilder_ == null) {
                    if (((bitField0_ & 0x00000001) != 0)) {
                        timeseries_ = java.util.Collections.unmodifiableList(timeseries_);
                        bitField0_ = (bitField0_ & ~0x00000001);
                    }
                    result.timeseries_ = timeseries_;
                } else {
                    result.timeseries_ = timeseriesBuilder_.build();
                }
                if (metadataBuilder_ == null) {
                    if (((bitField0_ & 0x00000002) != 0)) {
                        metadata_ = java.util.Collections.unmodifiableList(metadata_);
                        bitField0_ = (bitField0_ & ~0x00000002);
                    }
                    result.metadata_ = metadata_;
                } else {
                    result.metadata_ = metadataBuilder_.build();
                }
            }

            private void buildPartial0(Remote.WriteRequest result) {
                int from_bitField0_ = bitField0_;
            }

            @Override
            public Builder clone() {
                return super.clone();
            }

            @Override
            public Builder setField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, Object value) {
                return super.setField(field, value);
            }

            @Override
            public Builder clearField(com.google.protobuf.Descriptors.FieldDescriptor field) {
                return super.clearField(field);
            }

            @Override
            public Builder clearOneof(com.google.protobuf.Descriptors.OneofDescriptor oneof) {
                return super.clearOneof(oneof);
            }

            @Override
            public Builder setRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    int index,
                    Object value) {
                return super.setRepeatedField(field, index, value);
            }

            @Override
            public Builder addRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, Object value) {
                return super.addRepeatedField(field, value);
            }

            @Override
            public Builder mergeFrom(com.google.protobuf.Message other) {
                if (other instanceof Remote.WriteRequest) {
                    return mergeFrom((Remote.WriteRequest) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Remote.WriteRequest other) {
                if (other == Remote.WriteRequest.getDefaultInstance()) {
                    return this;
                }
                if (timeseriesBuilder_ == null) {
                    if (!other.timeseries_.isEmpty()) {
                        if (timeseries_.isEmpty()) {
                            timeseries_ = other.timeseries_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                        } else {
                            ensureTimeseriesIsMutable();
                            timeseries_.addAll(other.timeseries_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.timeseries_.isEmpty()) {
                        if (timeseriesBuilder_.isEmpty()) {
                            timeseriesBuilder_.dispose();
                            timeseriesBuilder_ = null;
                            timeseries_ = other.timeseries_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                            timeseriesBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getTimeseriesFieldBuilder()
                                            : null;
                        } else {
                            timeseriesBuilder_.addAllMessages(other.timeseries_);
                        }
                    }
                }
                if (metadataBuilder_ == null) {
                    if (!other.metadata_.isEmpty()) {
                        if (metadata_.isEmpty()) {
                            metadata_ = other.metadata_;
                            bitField0_ = (bitField0_ & ~0x00000002);
                        } else {
                            ensureMetadataIsMutable();
                            metadata_.addAll(other.metadata_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.metadata_.isEmpty()) {
                        if (metadataBuilder_.isEmpty()) {
                            metadataBuilder_.dispose();
                            metadataBuilder_ = null;
                            metadata_ = other.metadata_;
                            bitField0_ = (bitField0_ & ~0x00000002);
                            metadataBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getMetadataFieldBuilder()
                                            : null;
                        } else {
                            metadataBuilder_.addAllMessages(other.metadata_);
                        }
                    }
                }
                this.mergeUnknownFields(other.getUnknownFields());
                onChanged();
                return this;
            }

            @Override
            public final boolean isInitialized() {
                return true;
            }

            @Override
            public Builder mergeFrom(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException {
                if (extensionRegistry == null) {
                    throw new NullPointerException();
                }
                try {
                    boolean done = false;
                    while (!done) {
                        int tag = input.readTag();
                        switch (tag) {
                            case 0:
                                done = true;
                                break;
                            case 10:
                                {
                                    Types.TimeSeries m =
                                            input.readMessage(
                                                    Types.TimeSeries.parser(), extensionRegistry);
                                    if (timeseriesBuilder_ == null) {
                                        ensureTimeseriesIsMutable();
                                        timeseries_.add(m);
                                    } else {
                                        timeseriesBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 10
                            case 26:
                                {
                                    Types.MetricMetadata m =
                                            input.readMessage(
                                                    Types.MetricMetadata.parser(),
                                                    extensionRegistry);
                                    if (metadataBuilder_ == null) {
                                        ensureMetadataIsMutable();
                                        metadata_.add(m);
                                    } else {
                                        metadataBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 26
                            default:
                                {
                                    if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                                        done = true; // was an endgroup tag
                                    }
                                    break;
                                } // default:
                        } // switch (tag)
                    } // while (!done)
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    throw e.unwrapIOException();
                } finally {
                    onChanged();
                } // finally
                return this;
            }

            private int bitField0_;

            private java.util.List<Types.TimeSeries> timeseries_ =
                    java.util.Collections.emptyList();

            private void ensureTimeseriesIsMutable() {
                if (!((bitField0_ & 0x00000001) != 0)) {
                    timeseries_ = new java.util.ArrayList<Types.TimeSeries>(timeseries_);
                    bitField0_ |= 0x00000001;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.TimeSeries, Types.TimeSeries.Builder, Types.TimeSeriesOrBuilder>
                    timeseriesBuilder_;

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<Types.TimeSeries> getTimeseriesList() {
                if (timeseriesBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(timeseries_);
                } else {
                    return timeseriesBuilder_.getMessageList();
                }
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public int getTimeseriesCount() {
                if (timeseriesBuilder_ == null) {
                    return timeseries_.size();
                } else {
                    return timeseriesBuilder_.getCount();
                }
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.TimeSeries getTimeseries(int index) {
                if (timeseriesBuilder_ == null) {
                    return timeseries_.get(index);
                } else {
                    return timeseriesBuilder_.getMessage(index);
                }
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder setTimeseries(int index, Types.TimeSeries value) {
                if (timeseriesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureTimeseriesIsMutable();
                    timeseries_.set(index, value);
                    onChanged();
                } else {
                    timeseriesBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder setTimeseries(int index, Types.TimeSeries.Builder builderForValue) {
                if (timeseriesBuilder_ == null) {
                    ensureTimeseriesIsMutable();
                    timeseries_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    timeseriesBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addTimeseries(Types.TimeSeries value) {
                if (timeseriesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureTimeseriesIsMutable();
                    timeseries_.add(value);
                    onChanged();
                } else {
                    timeseriesBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addTimeseries(int index, Types.TimeSeries value) {
                if (timeseriesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureTimeseriesIsMutable();
                    timeseries_.add(index, value);
                    onChanged();
                } else {
                    timeseriesBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addTimeseries(Types.TimeSeries.Builder builderForValue) {
                if (timeseriesBuilder_ == null) {
                    ensureTimeseriesIsMutable();
                    timeseries_.add(builderForValue.build());
                    onChanged();
                } else {
                    timeseriesBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addTimeseries(int index, Types.TimeSeries.Builder builderForValue) {
                if (timeseriesBuilder_ == null) {
                    ensureTimeseriesIsMutable();
                    timeseries_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    timeseriesBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addAllTimeseries(Iterable<? extends Types.TimeSeries> values) {
                if (timeseriesBuilder_ == null) {
                    ensureTimeseriesIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, timeseries_);
                    onChanged();
                } else {
                    timeseriesBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder clearTimeseries() {
                if (timeseriesBuilder_ == null) {
                    timeseries_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000001);
                    onChanged();
                } else {
                    timeseriesBuilder_.clear();
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder removeTimeseries(int index) {
                if (timeseriesBuilder_ == null) {
                    ensureTimeseriesIsMutable();
                    timeseries_.remove(index);
                    onChanged();
                } else {
                    timeseriesBuilder_.remove(index);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.TimeSeries.Builder getTimeseriesBuilder(int index) {
                return getTimeseriesFieldBuilder().getBuilder(index);
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.TimeSeriesOrBuilder getTimeseriesOrBuilder(int index) {
                if (timeseriesBuilder_ == null) {
                    return timeseries_.get(index);
                } else {
                    return timeseriesBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<? extends Types.TimeSeriesOrBuilder>
                    getTimeseriesOrBuilderList() {
                if (timeseriesBuilder_ != null) {
                    return timeseriesBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(timeseries_);
                }
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.TimeSeries.Builder addTimeseriesBuilder() {
                return getTimeseriesFieldBuilder()
                        .addBuilder(Types.TimeSeries.getDefaultInstance());
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.TimeSeries.Builder addTimeseriesBuilder(int index) {
                return getTimeseriesFieldBuilder()
                        .addBuilder(index, Types.TimeSeries.getDefaultInstance());
            }

            /**
             * <code>repeated .prometheus.TimeSeries timeseries = 1 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<Types.TimeSeries.Builder> getTimeseriesBuilderList() {
                return getTimeseriesFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.TimeSeries, Types.TimeSeries.Builder, Types.TimeSeriesOrBuilder>
                    getTimeseriesFieldBuilder() {
                if (timeseriesBuilder_ == null) {
                    timeseriesBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.TimeSeries,
                                    Types.TimeSeries.Builder,
                                    Types.TimeSeriesOrBuilder>(
                                    timeseries_,
                                    ((bitField0_ & 0x00000001) != 0),
                                    getParentForChildren(),
                                    isClean());
                    timeseries_ = null;
                }
                return timeseriesBuilder_;
            }

            private java.util.List<Types.MetricMetadata> metadata_ =
                    java.util.Collections.emptyList();

            private void ensureMetadataIsMutable() {
                if (!((bitField0_ & 0x00000002) != 0)) {
                    metadata_ = new java.util.ArrayList<Types.MetricMetadata>(metadata_);
                    bitField0_ |= 0x00000002;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.MetricMetadata,
                            Types.MetricMetadata.Builder,
                            Types.MetricMetadataOrBuilder>
                    metadataBuilder_;

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<Types.MetricMetadata> getMetadataList() {
                if (metadataBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(metadata_);
                } else {
                    return metadataBuilder_.getMessageList();
                }
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public int getMetadataCount() {
                if (metadataBuilder_ == null) {
                    return metadata_.size();
                } else {
                    return metadataBuilder_.getCount();
                }
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.MetricMetadata getMetadata(int index) {
                if (metadataBuilder_ == null) {
                    return metadata_.get(index);
                } else {
                    return metadataBuilder_.getMessage(index);
                }
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder setMetadata(int index, Types.MetricMetadata value) {
                if (metadataBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureMetadataIsMutable();
                    metadata_.set(index, value);
                    onChanged();
                } else {
                    metadataBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder setMetadata(int index, Types.MetricMetadata.Builder builderForValue) {
                if (metadataBuilder_ == null) {
                    ensureMetadataIsMutable();
                    metadata_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    metadataBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addMetadata(Types.MetricMetadata value) {
                if (metadataBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureMetadataIsMutable();
                    metadata_.add(value);
                    onChanged();
                } else {
                    metadataBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addMetadata(int index, Types.MetricMetadata value) {
                if (metadataBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureMetadataIsMutable();
                    metadata_.add(index, value);
                    onChanged();
                } else {
                    metadataBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addMetadata(Types.MetricMetadata.Builder builderForValue) {
                if (metadataBuilder_ == null) {
                    ensureMetadataIsMutable();
                    metadata_.add(builderForValue.build());
                    onChanged();
                } else {
                    metadataBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addMetadata(int index, Types.MetricMetadata.Builder builderForValue) {
                if (metadataBuilder_ == null) {
                    ensureMetadataIsMutable();
                    metadata_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    metadataBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addAllMetadata(Iterable<? extends Types.MetricMetadata> values) {
                if (metadataBuilder_ == null) {
                    ensureMetadataIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, metadata_);
                    onChanged();
                } else {
                    metadataBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder clearMetadata() {
                if (metadataBuilder_ == null) {
                    metadata_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000002);
                    onChanged();
                } else {
                    metadataBuilder_.clear();
                }
                return this;
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder removeMetadata(int index) {
                if (metadataBuilder_ == null) {
                    ensureMetadataIsMutable();
                    metadata_.remove(index);
                    onChanged();
                } else {
                    metadataBuilder_.remove(index);
                }
                return this;
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.MetricMetadata.Builder getMetadataBuilder(int index) {
                return getMetadataFieldBuilder().getBuilder(index);
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.MetricMetadataOrBuilder getMetadataOrBuilder(int index) {
                if (metadataBuilder_ == null) {
                    return metadata_.get(index);
                } else {
                    return metadataBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<? extends Types.MetricMetadataOrBuilder>
                    getMetadataOrBuilderList() {
                if (metadataBuilder_ != null) {
                    return metadataBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(metadata_);
                }
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.MetricMetadata.Builder addMetadataBuilder() {
                return getMetadataFieldBuilder()
                        .addBuilder(Types.MetricMetadata.getDefaultInstance());
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.MetricMetadata.Builder addMetadataBuilder(int index) {
                return getMetadataFieldBuilder()
                        .addBuilder(index, Types.MetricMetadata.getDefaultInstance());
            }

            /**
             * <code>
             * repeated .prometheus.MetricMetadata metadata = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<Types.MetricMetadata.Builder> getMetadataBuilderList() {
                return getMetadataFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.MetricMetadata,
                            Types.MetricMetadata.Builder,
                            Types.MetricMetadataOrBuilder>
                    getMetadataFieldBuilder() {
                if (metadataBuilder_ == null) {
                    metadataBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.MetricMetadata,
                                    Types.MetricMetadata.Builder,
                                    Types.MetricMetadataOrBuilder>(
                                    metadata_,
                                    ((bitField0_ & 0x00000002) != 0),
                                    getParentForChildren(),
                                    isClean());
                    metadata_ = null;
                }
                return metadataBuilder_;
            }

            @Override
            public final Builder setUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.setUnknownFields(unknownFields);
            }

            @Override
            public final Builder mergeUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.mergeUnknownFields(unknownFields);
            }

            // @@protoc_insertion_point(builder_scope:prometheus.WriteRequest)
        }

        // @@protoc_insertion_point(class_scope:prometheus.WriteRequest)
        private static final Remote.WriteRequest DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Remote.WriteRequest();
        }

        public static Remote.WriteRequest getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<WriteRequest> PARSER =
                new com.google.protobuf.AbstractParser<WriteRequest>() {
                    @Override
                    public WriteRequest parsePartialFrom(
                            com.google.protobuf.CodedInputStream input,
                            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                            throws com.google.protobuf.InvalidProtocolBufferException {
                        Builder builder = newBuilder();
                        try {
                            builder.mergeFrom(input, extensionRegistry);
                        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                            throw e.setUnfinishedMessage(builder.buildPartial());
                        } catch (com.google.protobuf.UninitializedMessageException e) {
                            throw e.asInvalidProtocolBufferException()
                                    .setUnfinishedMessage(builder.buildPartial());
                        } catch (java.io.IOException e) {
                            throw new com.google.protobuf.InvalidProtocolBufferException(e)
                                    .setUnfinishedMessage(builder.buildPartial());
                        }
                        return builder.buildPartial();
                    }
                };

        public static com.google.protobuf.Parser<WriteRequest> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<WriteRequest> getParserForType() {
            return PARSER;
        }

        @Override
        public Remote.WriteRequest getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface ReadRequestOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.ReadRequest)
            com.google.protobuf.MessageOrBuilder {

        /** <code>repeated .prometheus.Query queries = 1;</code> */
        java.util.List<Remote.Query> getQueriesList();

        /** <code>repeated .prometheus.Query queries = 1;</code> */
        Remote.Query getQueries(int index);

        /** <code>repeated .prometheus.Query queries = 1;</code> */
        int getQueriesCount();

        /** <code>repeated .prometheus.Query queries = 1;</code> */
        java.util.List<? extends Remote.QueryOrBuilder> getQueriesOrBuilderList();

        /** <code>repeated .prometheus.Query queries = 1;</code> */
        Remote.QueryOrBuilder getQueriesOrBuilder(int index);

        /**
         *
         *
         * <pre>
         * accepted_response_types allows negotiating the content type of the response.
         *
         * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
         * implemented by server, error is returned.
         * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
         * </pre>
         *
         * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;</code>
         *
         * @return A list containing the acceptedResponseTypes.
         */
        java.util.List<Remote.ReadRequest.ResponseType> getAcceptedResponseTypesList();

        /**
         *
         *
         * <pre>
         * accepted_response_types allows negotiating the content type of the response.
         *
         * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
         * implemented by server, error is returned.
         * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
         * </pre>
         *
         * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;</code>
         *
         * @return The count of acceptedResponseTypes.
         */
        int getAcceptedResponseTypesCount();

        /**
         *
         *
         * <pre>
         * accepted_response_types allows negotiating the content type of the response.
         *
         * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
         * implemented by server, error is returned.
         * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
         * </pre>
         *
         * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;</code>
         *
         * @param index The index of the element to return.
         * @return The acceptedResponseTypes at the given index.
         */
        Remote.ReadRequest.ResponseType getAcceptedResponseTypes(int index);

        /**
         *
         *
         * <pre>
         * accepted_response_types allows negotiating the content type of the response.
         *
         * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
         * implemented by server, error is returned.
         * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
         * </pre>
         *
         * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;</code>
         *
         * @return A list containing the enum numeric values on the wire for acceptedResponseTypes.
         */
        java.util.List<Integer> getAcceptedResponseTypesValueList();

        /**
         *
         *
         * <pre>
         * accepted_response_types allows negotiating the content type of the response.
         *
         * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
         * implemented by server, error is returned.
         * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
         * </pre>
         *
         * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;</code>
         *
         * @param index The index of the value to return.
         * @return The enum numeric value on the wire of acceptedResponseTypes at the given index.
         */
        int getAcceptedResponseTypesValue(int index);
    }

    /**
     *
     *
     * <pre>
     * ReadRequest represents a remote read request.
     * </pre>
     *
     * <p>Protobuf type {@code prometheus.ReadRequest}
     */
    public static final class ReadRequest extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.ReadRequest)
            ReadRequestOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use ReadRequest.newBuilder() to construct.
        private ReadRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private ReadRequest() {
            queries_ = java.util.Collections.emptyList();
            acceptedResponseTypes_ = java.util.Collections.emptyList();
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new ReadRequest();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Remote.internal_static_prometheus_ReadRequest_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Remote.internal_static_prometheus_ReadRequest_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Remote.ReadRequest.class, Remote.ReadRequest.Builder.class);
        }

        /** Protobuf enum {@code prometheus.ReadRequest.ResponseType} */
        public enum ResponseType implements com.google.protobuf.ProtocolMessageEnum {
            /**
             *
             *
             * <pre>
             * Server will return a single ReadResponse message with matched series that includes list of raw samples.
             * It's recommended to use streamed response types instead.
             *
             * Response headers:
             * Content-Type: "application/x-protobuf"
             * Content-Encoding: "snappy"
             * </pre>
             *
             * <code>SAMPLES = 0;</code>
             */
            SAMPLES(0),
            /**
             *
             *
             * <pre>
             * Server will stream a delimited ChunkedReadResponse message that
             * contains XOR or HISTOGRAM(!) encoded chunks for a single series.
             * Each message is following varint size and fixed size bigendian
             * uint32 for CRC32 Castagnoli checksum.
             *
             * Response headers:
             * Content-Type: "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
             * Content-Encoding: ""
             * </pre>
             *
             * <code>STREAMED_XOR_CHUNKS = 1;</code>
             */
            STREAMED_XOR_CHUNKS(1),
            UNRECOGNIZED(-1),
            ;

            /**
             *
             *
             * <pre>
             * Server will return a single ReadResponse message with matched series that includes list of raw samples.
             * It's recommended to use streamed response types instead.
             *
             * Response headers:
             * Content-Type: "application/x-protobuf"
             * Content-Encoding: "snappy"
             * </pre>
             *
             * <code>SAMPLES = 0;</code>
             */
            public static final int SAMPLES_VALUE = 0;
            /**
             *
             *
             * <pre>
             * Server will stream a delimited ChunkedReadResponse message that
             * contains XOR or HISTOGRAM(!) encoded chunks for a single series.
             * Each message is following varint size and fixed size bigendian
             * uint32 for CRC32 Castagnoli checksum.
             *
             * Response headers:
             * Content-Type: "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
             * Content-Encoding: ""
             * </pre>
             *
             * <code>STREAMED_XOR_CHUNKS = 1;</code>
             */
            public static final int STREAMED_XOR_CHUNKS_VALUE = 1;

            public final int getNumber() {
                if (this == UNRECOGNIZED) {
                    throw new IllegalArgumentException(
                            "Can't get the number of an unknown enum value.");
                }
                return value;
            }

            /**
             * @param value The numeric wire value of the corresponding enum entry.
             * @return The enum associated with the given numeric wire value.
             * @deprecated Use {@link #forNumber(int)} instead.
             */
            @Deprecated
            public static ResponseType valueOf(int value) {
                return forNumber(value);
            }

            /**
             * @param value The numeric wire value of the corresponding enum entry.
             * @return The enum associated with the given numeric wire value.
             */
            public static ResponseType forNumber(int value) {
                switch (value) {
                    case 0:
                        return SAMPLES;
                    case 1:
                        return STREAMED_XOR_CHUNKS;
                    default:
                        return null;
                }
            }

            public static com.google.protobuf.Internal.EnumLiteMap<ResponseType>
                    internalGetValueMap() {
                return internalValueMap;
            }

            private static final com.google.protobuf.Internal.EnumLiteMap<ResponseType>
                    internalValueMap =
                            new com.google.protobuf.Internal.EnumLiteMap<ResponseType>() {
                                public ResponseType findValueByNumber(int number) {
                                    return ResponseType.forNumber(number);
                                }
                            };

            public final com.google.protobuf.Descriptors.EnumValueDescriptor getValueDescriptor() {
                if (this == UNRECOGNIZED) {
                    throw new IllegalStateException(
                            "Can't get the descriptor of an unrecognized enum value.");
                }
                return getDescriptor().getValues().get(ordinal());
            }

            public final com.google.protobuf.Descriptors.EnumDescriptor getDescriptorForType() {
                return getDescriptor();
            }

            public static final com.google.protobuf.Descriptors.EnumDescriptor getDescriptor() {
                return Remote.ReadRequest.getDescriptor().getEnumTypes().get(0);
            }

            private static final ResponseType[] VALUES = values();

            public static ResponseType valueOf(
                    com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
                if (desc.getType() != getDescriptor()) {
                    throw new IllegalArgumentException("EnumValueDescriptor is not for this type.");
                }
                if (desc.getIndex() == -1) {
                    return UNRECOGNIZED;
                }
                return VALUES[desc.getIndex()];
            }

            private final int value;

            private ResponseType(int value) {
                this.value = value;
            }

            // @@protoc_insertion_point(enum_scope:prometheus.ReadRequest.ResponseType)
        }

        public static final int QUERIES_FIELD_NUMBER = 1;

        @SuppressWarnings("serial")
        private java.util.List<Remote.Query> queries_;

        /** <code>repeated .prometheus.Query queries = 1;</code> */
        @Override
        public java.util.List<Remote.Query> getQueriesList() {
            return queries_;
        }

        /** <code>repeated .prometheus.Query queries = 1;</code> */
        @Override
        public java.util.List<? extends Remote.QueryOrBuilder> getQueriesOrBuilderList() {
            return queries_;
        }

        /** <code>repeated .prometheus.Query queries = 1;</code> */
        @Override
        public int getQueriesCount() {
            return queries_.size();
        }

        /** <code>repeated .prometheus.Query queries = 1;</code> */
        @Override
        public Remote.Query getQueries(int index) {
            return queries_.get(index);
        }

        /** <code>repeated .prometheus.Query queries = 1;</code> */
        @Override
        public Remote.QueryOrBuilder getQueriesOrBuilder(int index) {
            return queries_.get(index);
        }

        public static final int ACCEPTED_RESPONSE_TYPES_FIELD_NUMBER = 2;

        @SuppressWarnings("serial")
        private java.util.List<Integer> acceptedResponseTypes_;

        private static final com.google.protobuf.Internal.ListAdapter.Converter<
                        Integer, Remote.ReadRequest.ResponseType>
                acceptedResponseTypes_converter_ =
                        new com.google.protobuf.Internal.ListAdapter.Converter<
                                Integer, Remote.ReadRequest.ResponseType>() {
                            public Remote.ReadRequest.ResponseType convert(Integer from) {
                                Remote.ReadRequest.ResponseType result =
                                        Remote.ReadRequest.ResponseType.forNumber(from);
                                return result == null
                                        ? Remote.ReadRequest.ResponseType.UNRECOGNIZED
                                        : result;
                            }
                        };

        /**
         *
         *
         * <pre>
         * accepted_response_types allows negotiating the content type of the response.
         *
         * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
         * implemented by server, error is returned.
         * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
         * </pre>
         *
         * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;</code>
         *
         * @return A list containing the acceptedResponseTypes.
         */
        @Override
        public java.util.List<Remote.ReadRequest.ResponseType> getAcceptedResponseTypesList() {
            return new com.google.protobuf.Internal.ListAdapter<
                    Integer, Remote.ReadRequest.ResponseType>(
                    acceptedResponseTypes_, acceptedResponseTypes_converter_);
        }

        /**
         *
         *
         * <pre>
         * accepted_response_types allows negotiating the content type of the response.
         *
         * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
         * implemented by server, error is returned.
         * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
         * </pre>
         *
         * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;</code>
         *
         * @return The count of acceptedResponseTypes.
         */
        @Override
        public int getAcceptedResponseTypesCount() {
            return acceptedResponseTypes_.size();
        }

        /**
         *
         *
         * <pre>
         * accepted_response_types allows negotiating the content type of the response.
         *
         * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
         * implemented by server, error is returned.
         * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
         * </pre>
         *
         * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;</code>
         *
         * @param index The index of the element to return.
         * @return The acceptedResponseTypes at the given index.
         */
        @Override
        public Remote.ReadRequest.ResponseType getAcceptedResponseTypes(int index) {
            return acceptedResponseTypes_converter_.convert(acceptedResponseTypes_.get(index));
        }

        /**
         *
         *
         * <pre>
         * accepted_response_types allows negotiating the content type of the response.
         *
         * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
         * implemented by server, error is returned.
         * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
         * </pre>
         *
         * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;</code>
         *
         * @return A list containing the enum numeric values on the wire for acceptedResponseTypes.
         */
        @Override
        public java.util.List<Integer> getAcceptedResponseTypesValueList() {
            return acceptedResponseTypes_;
        }

        /**
         *
         *
         * <pre>
         * accepted_response_types allows negotiating the content type of the response.
         *
         * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
         * implemented by server, error is returned.
         * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
         * </pre>
         *
         * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;</code>
         *
         * @param index The index of the value to return.
         * @return The enum numeric value on the wire of acceptedResponseTypes at the given index.
         */
        @Override
        public int getAcceptedResponseTypesValue(int index) {
            return acceptedResponseTypes_.get(index);
        }

        private int acceptedResponseTypesMemoizedSerializedSize;

        private byte memoizedIsInitialized = -1;

        @Override
        public final boolean isInitialized() {
            byte isInitialized = memoizedIsInitialized;
            if (isInitialized == 1) {
                return true;
            }
            if (isInitialized == 0) {
                return false;
            }

            memoizedIsInitialized = 1;
            return true;
        }

        @Override
        public void writeTo(com.google.protobuf.CodedOutputStream output)
                throws java.io.IOException {
            getSerializedSize();
            for (int i = 0; i < queries_.size(); i++) {
                output.writeMessage(1, queries_.get(i));
            }
            if (getAcceptedResponseTypesList().size() > 0) {
                output.writeUInt32NoTag(18);
                output.writeUInt32NoTag(acceptedResponseTypesMemoizedSerializedSize);
            }
            for (int i = 0; i < acceptedResponseTypes_.size(); i++) {
                output.writeEnumNoTag(acceptedResponseTypes_.get(i));
            }
            getUnknownFields().writeTo(output);
        }

        @Override
        public int getSerializedSize() {
            int size = memoizedSize;
            if (size != -1) {
                return size;
            }

            size = 0;
            for (int i = 0; i < queries_.size(); i++) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeMessageSize(
                                1, queries_.get(i));
            }
            {
                int dataSize = 0;
                for (int i = 0; i < acceptedResponseTypes_.size(); i++) {
                    dataSize +=
                            com.google.protobuf.CodedOutputStream.computeEnumSizeNoTag(
                                    acceptedResponseTypes_.get(i));
                }
                size += dataSize;
                if (!getAcceptedResponseTypesList().isEmpty()) {
                    size += 1;
                    size += com.google.protobuf.CodedOutputStream.computeUInt32SizeNoTag(dataSize);
                }
                acceptedResponseTypesMemoizedSerializedSize = dataSize;
            }
            size += getUnknownFields().getSerializedSize();
            memoizedSize = size;
            return size;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof Remote.ReadRequest)) {
                return super.equals(obj);
            }
            Remote.ReadRequest other = (Remote.ReadRequest) obj;

            if (!getQueriesList().equals(other.getQueriesList())) {
                return false;
            }
            if (!acceptedResponseTypes_.equals(other.acceptedResponseTypes_)) {
                return false;
            }
            if (!getUnknownFields().equals(other.getUnknownFields())) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            if (memoizedHashCode != 0) {
                return memoizedHashCode;
            }
            int hash = 41;
            hash = (19 * hash) + getDescriptor().hashCode();
            if (getQueriesCount() > 0) {
                hash = (37 * hash) + QUERIES_FIELD_NUMBER;
                hash = (53 * hash) + getQueriesList().hashCode();
            }
            if (getAcceptedResponseTypesCount() > 0) {
                hash = (37 * hash) + ACCEPTED_RESPONSE_TYPES_FIELD_NUMBER;
                hash = (53 * hash) + acceptedResponseTypes_.hashCode();
            }
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Remote.ReadRequest parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.ReadRequest parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.ReadRequest parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.ReadRequest parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.ReadRequest parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.ReadRequest parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.ReadRequest parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Remote.ReadRequest parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Remote.ReadRequest parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Remote.ReadRequest parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Remote.ReadRequest parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Remote.ReadRequest parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        @Override
        public Builder newBuilderForType() {
            return newBuilder();
        }

        public static Builder newBuilder() {
            return DEFAULT_INSTANCE.toBuilder();
        }

        public static Builder newBuilder(Remote.ReadRequest prototype) {
            return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }

        @Override
        public Builder toBuilder() {
            return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
        }

        @Override
        protected Builder newBuilderForType(BuilderParent parent) {
            Builder builder = new Builder(parent);
            return builder;
        }

        /**
         *
         *
         * <pre>
         * ReadRequest represents a remote read request.
         * </pre>
         *
         * <p>Protobuf type {@code prometheus.ReadRequest}
         */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.ReadRequest)
                Remote.ReadRequestOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Remote.internal_static_prometheus_ReadRequest_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Remote.internal_static_prometheus_ReadRequest_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Remote.ReadRequest.class, Remote.ReadRequest.Builder.class);
            }

            // Construct using Remote.ReadRequest.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                if (queriesBuilder_ == null) {
                    queries_ = java.util.Collections.emptyList();
                } else {
                    queries_ = null;
                    queriesBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000001);
                acceptedResponseTypes_ = java.util.Collections.emptyList();
                bitField0_ = (bitField0_ & ~0x00000002);
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Remote.internal_static_prometheus_ReadRequest_descriptor;
            }

            @Override
            public Remote.ReadRequest getDefaultInstanceForType() {
                return Remote.ReadRequest.getDefaultInstance();
            }

            @Override
            public Remote.ReadRequest build() {
                Remote.ReadRequest result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Remote.ReadRequest buildPartial() {
                Remote.ReadRequest result = new Remote.ReadRequest(this);
                buildPartialRepeatedFields(result);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartialRepeatedFields(Remote.ReadRequest result) {
                if (queriesBuilder_ == null) {
                    if (((bitField0_ & 0x00000001) != 0)) {
                        queries_ = java.util.Collections.unmodifiableList(queries_);
                        bitField0_ = (bitField0_ & ~0x00000001);
                    }
                    result.queries_ = queries_;
                } else {
                    result.queries_ = queriesBuilder_.build();
                }
                if (((bitField0_ & 0x00000002) != 0)) {
                    acceptedResponseTypes_ =
                            java.util.Collections.unmodifiableList(acceptedResponseTypes_);
                    bitField0_ = (bitField0_ & ~0x00000002);
                }
                result.acceptedResponseTypes_ = acceptedResponseTypes_;
            }

            private void buildPartial0(Remote.ReadRequest result) {
                int from_bitField0_ = bitField0_;
            }

            @Override
            public Builder clone() {
                return super.clone();
            }

            @Override
            public Builder setField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, Object value) {
                return super.setField(field, value);
            }

            @Override
            public Builder clearField(com.google.protobuf.Descriptors.FieldDescriptor field) {
                return super.clearField(field);
            }

            @Override
            public Builder clearOneof(com.google.protobuf.Descriptors.OneofDescriptor oneof) {
                return super.clearOneof(oneof);
            }

            @Override
            public Builder setRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    int index,
                    Object value) {
                return super.setRepeatedField(field, index, value);
            }

            @Override
            public Builder addRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, Object value) {
                return super.addRepeatedField(field, value);
            }

            @Override
            public Builder mergeFrom(com.google.protobuf.Message other) {
                if (other instanceof Remote.ReadRequest) {
                    return mergeFrom((Remote.ReadRequest) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Remote.ReadRequest other) {
                if (other == Remote.ReadRequest.getDefaultInstance()) {
                    return this;
                }
                if (queriesBuilder_ == null) {
                    if (!other.queries_.isEmpty()) {
                        if (queries_.isEmpty()) {
                            queries_ = other.queries_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                        } else {
                            ensureQueriesIsMutable();
                            queries_.addAll(other.queries_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.queries_.isEmpty()) {
                        if (queriesBuilder_.isEmpty()) {
                            queriesBuilder_.dispose();
                            queriesBuilder_ = null;
                            queries_ = other.queries_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                            queriesBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getQueriesFieldBuilder()
                                            : null;
                        } else {
                            queriesBuilder_.addAllMessages(other.queries_);
                        }
                    }
                }
                if (!other.acceptedResponseTypes_.isEmpty()) {
                    if (acceptedResponseTypes_.isEmpty()) {
                        acceptedResponseTypes_ = other.acceptedResponseTypes_;
                        bitField0_ = (bitField0_ & ~0x00000002);
                    } else {
                        ensureAcceptedResponseTypesIsMutable();
                        acceptedResponseTypes_.addAll(other.acceptedResponseTypes_);
                    }
                    onChanged();
                }
                this.mergeUnknownFields(other.getUnknownFields());
                onChanged();
                return this;
            }

            @Override
            public final boolean isInitialized() {
                return true;
            }

            @Override
            public Builder mergeFrom(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException {
                if (extensionRegistry == null) {
                    throw new NullPointerException();
                }
                try {
                    boolean done = false;
                    while (!done) {
                        int tag = input.readTag();
                        switch (tag) {
                            case 0:
                                done = true;
                                break;
                            case 10:
                                {
                                    Remote.Query m =
                                            input.readMessage(
                                                    Remote.Query.parser(), extensionRegistry);
                                    if (queriesBuilder_ == null) {
                                        ensureQueriesIsMutable();
                                        queries_.add(m);
                                    } else {
                                        queriesBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 10
                            case 16:
                                {
                                    int tmpRaw = input.readEnum();
                                    ensureAcceptedResponseTypesIsMutable();
                                    acceptedResponseTypes_.add(tmpRaw);
                                    break;
                                } // case 16
                            case 18:
                                {
                                    int length = input.readRawVarint32();
                                    int oldLimit = input.pushLimit(length);
                                    while (input.getBytesUntilLimit() > 0) {
                                        int tmpRaw = input.readEnum();
                                        ensureAcceptedResponseTypesIsMutable();
                                        acceptedResponseTypes_.add(tmpRaw);
                                    }
                                    input.popLimit(oldLimit);
                                    break;
                                } // case 18
                            default:
                                {
                                    if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                                        done = true; // was an endgroup tag
                                    }
                                    break;
                                } // default:
                        } // switch (tag)
                    } // while (!done)
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    throw e.unwrapIOException();
                } finally {
                    onChanged();
                } // finally
                return this;
            }

            private int bitField0_;

            private java.util.List<Remote.Query> queries_ = java.util.Collections.emptyList();

            private void ensureQueriesIsMutable() {
                if (!((bitField0_ & 0x00000001) != 0)) {
                    queries_ = new java.util.ArrayList<Remote.Query>(queries_);
                    bitField0_ |= 0x00000001;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Remote.Query, Remote.Query.Builder, Remote.QueryOrBuilder>
                    queriesBuilder_;

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public java.util.List<Remote.Query> getQueriesList() {
                if (queriesBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(queries_);
                } else {
                    return queriesBuilder_.getMessageList();
                }
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public int getQueriesCount() {
                if (queriesBuilder_ == null) {
                    return queries_.size();
                } else {
                    return queriesBuilder_.getCount();
                }
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Remote.Query getQueries(int index) {
                if (queriesBuilder_ == null) {
                    return queries_.get(index);
                } else {
                    return queriesBuilder_.getMessage(index);
                }
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Builder setQueries(int index, Remote.Query value) {
                if (queriesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureQueriesIsMutable();
                    queries_.set(index, value);
                    onChanged();
                } else {
                    queriesBuilder_.setMessage(index, value);
                }
                return this;
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Builder setQueries(int index, Remote.Query.Builder builderForValue) {
                if (queriesBuilder_ == null) {
                    ensureQueriesIsMutable();
                    queries_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    queriesBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Builder addQueries(Remote.Query value) {
                if (queriesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureQueriesIsMutable();
                    queries_.add(value);
                    onChanged();
                } else {
                    queriesBuilder_.addMessage(value);
                }
                return this;
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Builder addQueries(int index, Remote.Query value) {
                if (queriesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureQueriesIsMutable();
                    queries_.add(index, value);
                    onChanged();
                } else {
                    queriesBuilder_.addMessage(index, value);
                }
                return this;
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Builder addQueries(Remote.Query.Builder builderForValue) {
                if (queriesBuilder_ == null) {
                    ensureQueriesIsMutable();
                    queries_.add(builderForValue.build());
                    onChanged();
                } else {
                    queriesBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Builder addQueries(int index, Remote.Query.Builder builderForValue) {
                if (queriesBuilder_ == null) {
                    ensureQueriesIsMutable();
                    queries_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    queriesBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Builder addAllQueries(Iterable<? extends Remote.Query> values) {
                if (queriesBuilder_ == null) {
                    ensureQueriesIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, queries_);
                    onChanged();
                } else {
                    queriesBuilder_.addAllMessages(values);
                }
                return this;
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Builder clearQueries() {
                if (queriesBuilder_ == null) {
                    queries_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000001);
                    onChanged();
                } else {
                    queriesBuilder_.clear();
                }
                return this;
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Builder removeQueries(int index) {
                if (queriesBuilder_ == null) {
                    ensureQueriesIsMutable();
                    queries_.remove(index);
                    onChanged();
                } else {
                    queriesBuilder_.remove(index);
                }
                return this;
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Remote.Query.Builder getQueriesBuilder(int index) {
                return getQueriesFieldBuilder().getBuilder(index);
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Remote.QueryOrBuilder getQueriesOrBuilder(int index) {
                if (queriesBuilder_ == null) {
                    return queries_.get(index);
                } else {
                    return queriesBuilder_.getMessageOrBuilder(index);
                }
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public java.util.List<? extends Remote.QueryOrBuilder> getQueriesOrBuilderList() {
                if (queriesBuilder_ != null) {
                    return queriesBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(queries_);
                }
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Remote.Query.Builder addQueriesBuilder() {
                return getQueriesFieldBuilder().addBuilder(Remote.Query.getDefaultInstance());
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public Remote.Query.Builder addQueriesBuilder(int index) {
                return getQueriesFieldBuilder()
                        .addBuilder(index, Remote.Query.getDefaultInstance());
            }

            /** <code>repeated .prometheus.Query queries = 1;</code> */
            public java.util.List<Remote.Query.Builder> getQueriesBuilderList() {
                return getQueriesFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Remote.Query, Remote.Query.Builder, Remote.QueryOrBuilder>
                    getQueriesFieldBuilder() {
                if (queriesBuilder_ == null) {
                    queriesBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Remote.Query, Remote.Query.Builder, Remote.QueryOrBuilder>(
                                    queries_,
                                    ((bitField0_ & 0x00000001) != 0),
                                    getParentForChildren(),
                                    isClean());
                    queries_ = null;
                }
                return queriesBuilder_;
            }

            private java.util.List<Integer> acceptedResponseTypes_ =
                    java.util.Collections.emptyList();

            private void ensureAcceptedResponseTypesIsMutable() {
                if (!((bitField0_ & 0x00000002) != 0)) {
                    acceptedResponseTypes_ =
                            new java.util.ArrayList<Integer>(acceptedResponseTypes_);
                    bitField0_ |= 0x00000002;
                }
            }

            /**
             *
             *
             * <pre>
             * accepted_response_types allows negotiating the content type of the response.
             *
             * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
             * implemented by server, error is returned.
             * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
             * </pre>
             *
             * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;
             * </code>
             *
             * @return A list containing the acceptedResponseTypes.
             */
            public java.util.List<Remote.ReadRequest.ResponseType> getAcceptedResponseTypesList() {
                return new com.google.protobuf.Internal.ListAdapter<
                        Integer, Remote.ReadRequest.ResponseType>(
                        acceptedResponseTypes_, acceptedResponseTypes_converter_);
            }

            /**
             *
             *
             * <pre>
             * accepted_response_types allows negotiating the content type of the response.
             *
             * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
             * implemented by server, error is returned.
             * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
             * </pre>
             *
             * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;
             * </code>
             *
             * @return The count of acceptedResponseTypes.
             */
            public int getAcceptedResponseTypesCount() {
                return acceptedResponseTypes_.size();
            }

            /**
             *
             *
             * <pre>
             * accepted_response_types allows negotiating the content type of the response.
             *
             * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
             * implemented by server, error is returned.
             * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
             * </pre>
             *
             * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;
             * </code>
             *
             * @param index The index of the element to return.
             * @return The acceptedResponseTypes at the given index.
             */
            public Remote.ReadRequest.ResponseType getAcceptedResponseTypes(int index) {
                return acceptedResponseTypes_converter_.convert(acceptedResponseTypes_.get(index));
            }

            /**
             *
             *
             * <pre>
             * accepted_response_types allows negotiating the content type of the response.
             *
             * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
             * implemented by server, error is returned.
             * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
             * </pre>
             *
             * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;
             * </code>
             *
             * @param index The index to set the value at.
             * @param value The acceptedResponseTypes to set.
             * @return This builder for chaining.
             */
            public Builder setAcceptedResponseTypes(
                    int index, Remote.ReadRequest.ResponseType value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                ensureAcceptedResponseTypesIsMutable();
                acceptedResponseTypes_.set(index, value.getNumber());
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * accepted_response_types allows negotiating the content type of the response.
             *
             * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
             * implemented by server, error is returned.
             * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
             * </pre>
             *
             * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;
             * </code>
             *
             * @param value The acceptedResponseTypes to add.
             * @return This builder for chaining.
             */
            public Builder addAcceptedResponseTypes(Remote.ReadRequest.ResponseType value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                ensureAcceptedResponseTypesIsMutable();
                acceptedResponseTypes_.add(value.getNumber());
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * accepted_response_types allows negotiating the content type of the response.
             *
             * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
             * implemented by server, error is returned.
             * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
             * </pre>
             *
             * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;
             * </code>
             *
             * @param values The acceptedResponseTypes to add.
             * @return This builder for chaining.
             */
            public Builder addAllAcceptedResponseTypes(
                    Iterable<? extends Remote.ReadRequest.ResponseType> values) {
                ensureAcceptedResponseTypesIsMutable();
                for (Remote.ReadRequest.ResponseType value : values) {
                    acceptedResponseTypes_.add(value.getNumber());
                }
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * accepted_response_types allows negotiating the content type of the response.
             *
             * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
             * implemented by server, error is returned.
             * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
             * </pre>
             *
             * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;
             * </code>
             *
             * @return This builder for chaining.
             */
            public Builder clearAcceptedResponseTypes() {
                acceptedResponseTypes_ = java.util.Collections.emptyList();
                bitField0_ = (bitField0_ & ~0x00000002);
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * accepted_response_types allows negotiating the content type of the response.
             *
             * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
             * implemented by server, error is returned.
             * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
             * </pre>
             *
             * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;
             * </code>
             *
             * @return A list containing the enum numeric values on the wire for
             *     acceptedResponseTypes.
             */
            public java.util.List<Integer> getAcceptedResponseTypesValueList() {
                return java.util.Collections.unmodifiableList(acceptedResponseTypes_);
            }

            /**
             *
             *
             * <pre>
             * accepted_response_types allows negotiating the content type of the response.
             *
             * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
             * implemented by server, error is returned.
             * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
             * </pre>
             *
             * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;
             * </code>
             *
             * @param index The index of the value to return.
             * @return The enum numeric value on the wire of acceptedResponseTypes at the given
             *     index.
             */
            public int getAcceptedResponseTypesValue(int index) {
                return acceptedResponseTypes_.get(index);
            }

            /**
             *
             *
             * <pre>
             * accepted_response_types allows negotiating the content type of the response.
             *
             * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
             * implemented by server, error is returned.
             * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
             * </pre>
             *
             * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;
             * </code>
             *
             * @param index The index to set the value at.
             * @param value The enum numeric value on the wire for acceptedResponseTypes to set.
             * @return This builder for chaining.
             */
            public Builder setAcceptedResponseTypesValue(int index, int value) {
                ensureAcceptedResponseTypesIsMutable();
                acceptedResponseTypes_.set(index, value);
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * accepted_response_types allows negotiating the content type of the response.
             *
             * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
             * implemented by server, error is returned.
             * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
             * </pre>
             *
             * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;
             * </code>
             *
             * @param value The enum numeric value on the wire for acceptedResponseTypes to add.
             * @return This builder for chaining.
             */
            public Builder addAcceptedResponseTypesValue(int value) {
                ensureAcceptedResponseTypesIsMutable();
                acceptedResponseTypes_.add(value);
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * accepted_response_types allows negotiating the content type of the response.
             *
             * Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
             * implemented by server, error is returned.
             * For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
             * </pre>
             *
             * <code>repeated .prometheus.ReadRequest.ResponseType accepted_response_types = 2;
             * </code>
             *
             * @param values The enum numeric values on the wire for acceptedResponseTypes to add.
             * @return This builder for chaining.
             */
            public Builder addAllAcceptedResponseTypesValue(Iterable<Integer> values) {
                ensureAcceptedResponseTypesIsMutable();
                for (int value : values) {
                    acceptedResponseTypes_.add(value);
                }
                onChanged();
                return this;
            }

            @Override
            public final Builder setUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.setUnknownFields(unknownFields);
            }

            @Override
            public final Builder mergeUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.mergeUnknownFields(unknownFields);
            }

            // @@protoc_insertion_point(builder_scope:prometheus.ReadRequest)
        }

        // @@protoc_insertion_point(class_scope:prometheus.ReadRequest)
        private static final Remote.ReadRequest DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Remote.ReadRequest();
        }

        public static Remote.ReadRequest getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<ReadRequest> PARSER =
                new com.google.protobuf.AbstractParser<ReadRequest>() {
                    @Override
                    public ReadRequest parsePartialFrom(
                            com.google.protobuf.CodedInputStream input,
                            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                            throws com.google.protobuf.InvalidProtocolBufferException {
                        Builder builder = newBuilder();
                        try {
                            builder.mergeFrom(input, extensionRegistry);
                        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                            throw e.setUnfinishedMessage(builder.buildPartial());
                        } catch (com.google.protobuf.UninitializedMessageException e) {
                            throw e.asInvalidProtocolBufferException()
                                    .setUnfinishedMessage(builder.buildPartial());
                        } catch (java.io.IOException e) {
                            throw new com.google.protobuf.InvalidProtocolBufferException(e)
                                    .setUnfinishedMessage(builder.buildPartial());
                        }
                        return builder.buildPartial();
                    }
                };

        public static com.google.protobuf.Parser<ReadRequest> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<ReadRequest> getParserForType() {
            return PARSER;
        }

        @Override
        public Remote.ReadRequest getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface ReadResponseOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.ReadResponse)
            com.google.protobuf.MessageOrBuilder {

        /**
         *
         *
         * <pre>
         * In same order as the request's queries.
         * </pre>
         *
         * <code>repeated .prometheus.QueryResult results = 1;</code>
         */
        java.util.List<Remote.QueryResult> getResultsList();

        /**
         *
         *
         * <pre>
         * In same order as the request's queries.
         * </pre>
         *
         * <code>repeated .prometheus.QueryResult results = 1;</code>
         */
        Remote.QueryResult getResults(int index);

        /**
         *
         *
         * <pre>
         * In same order as the request's queries.
         * </pre>
         *
         * <code>repeated .prometheus.QueryResult results = 1;</code>
         */
        int getResultsCount();

        /**
         *
         *
         * <pre>
         * In same order as the request's queries.
         * </pre>
         *
         * <code>repeated .prometheus.QueryResult results = 1;</code>
         */
        java.util.List<? extends Remote.QueryResultOrBuilder> getResultsOrBuilderList();

        /**
         *
         *
         * <pre>
         * In same order as the request's queries.
         * </pre>
         *
         * <code>repeated .prometheus.QueryResult results = 1;</code>
         */
        Remote.QueryResultOrBuilder getResultsOrBuilder(int index);
    }

    /**
     *
     *
     * <pre>
     * ReadResponse is a response when response_type equals SAMPLES.
     * </pre>
     *
     * <p>Protobuf type {@code prometheus.ReadResponse}
     */
    public static final class ReadResponse extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.ReadResponse)
            ReadResponseOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use ReadResponse.newBuilder() to construct.
        private ReadResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private ReadResponse() {
            results_ = java.util.Collections.emptyList();
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new ReadResponse();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Remote.internal_static_prometheus_ReadResponse_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Remote.internal_static_prometheus_ReadResponse_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Remote.ReadResponse.class, Remote.ReadResponse.Builder.class);
        }

        public static final int RESULTS_FIELD_NUMBER = 1;

        @SuppressWarnings("serial")
        private java.util.List<Remote.QueryResult> results_;

        /**
         *
         *
         * <pre>
         * In same order as the request's queries.
         * </pre>
         *
         * <code>repeated .prometheus.QueryResult results = 1;</code>
         */
        @Override
        public java.util.List<Remote.QueryResult> getResultsList() {
            return results_;
        }

        /**
         *
         *
         * <pre>
         * In same order as the request's queries.
         * </pre>
         *
         * <code>repeated .prometheus.QueryResult results = 1;</code>
         */
        @Override
        public java.util.List<? extends Remote.QueryResultOrBuilder> getResultsOrBuilderList() {
            return results_;
        }

        /**
         *
         *
         * <pre>
         * In same order as the request's queries.
         * </pre>
         *
         * <code>repeated .prometheus.QueryResult results = 1;</code>
         */
        @Override
        public int getResultsCount() {
            return results_.size();
        }

        /**
         *
         *
         * <pre>
         * In same order as the request's queries.
         * </pre>
         *
         * <code>repeated .prometheus.QueryResult results = 1;</code>
         */
        @Override
        public Remote.QueryResult getResults(int index) {
            return results_.get(index);
        }

        /**
         *
         *
         * <pre>
         * In same order as the request's queries.
         * </pre>
         *
         * <code>repeated .prometheus.QueryResult results = 1;</code>
         */
        @Override
        public Remote.QueryResultOrBuilder getResultsOrBuilder(int index) {
            return results_.get(index);
        }

        private byte memoizedIsInitialized = -1;

        @Override
        public final boolean isInitialized() {
            byte isInitialized = memoizedIsInitialized;
            if (isInitialized == 1) {
                return true;
            }
            if (isInitialized == 0) {
                return false;
            }

            memoizedIsInitialized = 1;
            return true;
        }

        @Override
        public void writeTo(com.google.protobuf.CodedOutputStream output)
                throws java.io.IOException {
            for (int i = 0; i < results_.size(); i++) {
                output.writeMessage(1, results_.get(i));
            }
            getUnknownFields().writeTo(output);
        }

        @Override
        public int getSerializedSize() {
            int size = memoizedSize;
            if (size != -1) {
                return size;
            }

            size = 0;
            for (int i = 0; i < results_.size(); i++) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeMessageSize(
                                1, results_.get(i));
            }
            size += getUnknownFields().getSerializedSize();
            memoizedSize = size;
            return size;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof Remote.ReadResponse)) {
                return super.equals(obj);
            }
            Remote.ReadResponse other = (Remote.ReadResponse) obj;

            if (!getResultsList().equals(other.getResultsList())) {
                return false;
            }
            if (!getUnknownFields().equals(other.getUnknownFields())) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            if (memoizedHashCode != 0) {
                return memoizedHashCode;
            }
            int hash = 41;
            hash = (19 * hash) + getDescriptor().hashCode();
            if (getResultsCount() > 0) {
                hash = (37 * hash) + RESULTS_FIELD_NUMBER;
                hash = (53 * hash) + getResultsList().hashCode();
            }
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Remote.ReadResponse parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.ReadResponse parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.ReadResponse parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.ReadResponse parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.ReadResponse parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.ReadResponse parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.ReadResponse parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Remote.ReadResponse parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Remote.ReadResponse parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Remote.ReadResponse parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Remote.ReadResponse parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Remote.ReadResponse parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        @Override
        public Builder newBuilderForType() {
            return newBuilder();
        }

        public static Builder newBuilder() {
            return DEFAULT_INSTANCE.toBuilder();
        }

        public static Builder newBuilder(Remote.ReadResponse prototype) {
            return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }

        @Override
        public Builder toBuilder() {
            return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
        }

        @Override
        protected Builder newBuilderForType(BuilderParent parent) {
            Builder builder = new Builder(parent);
            return builder;
        }

        /**
         *
         *
         * <pre>
         * ReadResponse is a response when response_type equals SAMPLES.
         * </pre>
         *
         * <p>Protobuf type {@code prometheus.ReadResponse}
         */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.ReadResponse)
                Remote.ReadResponseOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Remote.internal_static_prometheus_ReadResponse_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Remote.internal_static_prometheus_ReadResponse_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Remote.ReadResponse.class, Remote.ReadResponse.Builder.class);
            }

            // Construct using Remote.ReadResponse.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                if (resultsBuilder_ == null) {
                    results_ = java.util.Collections.emptyList();
                } else {
                    results_ = null;
                    resultsBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000001);
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Remote.internal_static_prometheus_ReadResponse_descriptor;
            }

            @Override
            public Remote.ReadResponse getDefaultInstanceForType() {
                return Remote.ReadResponse.getDefaultInstance();
            }

            @Override
            public Remote.ReadResponse build() {
                Remote.ReadResponse result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Remote.ReadResponse buildPartial() {
                Remote.ReadResponse result = new Remote.ReadResponse(this);
                buildPartialRepeatedFields(result);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartialRepeatedFields(Remote.ReadResponse result) {
                if (resultsBuilder_ == null) {
                    if (((bitField0_ & 0x00000001) != 0)) {
                        results_ = java.util.Collections.unmodifiableList(results_);
                        bitField0_ = (bitField0_ & ~0x00000001);
                    }
                    result.results_ = results_;
                } else {
                    result.results_ = resultsBuilder_.build();
                }
            }

            private void buildPartial0(Remote.ReadResponse result) {
                int from_bitField0_ = bitField0_;
            }

            @Override
            public Builder clone() {
                return super.clone();
            }

            @Override
            public Builder setField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, Object value) {
                return super.setField(field, value);
            }

            @Override
            public Builder clearField(com.google.protobuf.Descriptors.FieldDescriptor field) {
                return super.clearField(field);
            }

            @Override
            public Builder clearOneof(com.google.protobuf.Descriptors.OneofDescriptor oneof) {
                return super.clearOneof(oneof);
            }

            @Override
            public Builder setRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    int index,
                    Object value) {
                return super.setRepeatedField(field, index, value);
            }

            @Override
            public Builder addRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, Object value) {
                return super.addRepeatedField(field, value);
            }

            @Override
            public Builder mergeFrom(com.google.protobuf.Message other) {
                if (other instanceof Remote.ReadResponse) {
                    return mergeFrom((Remote.ReadResponse) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Remote.ReadResponse other) {
                if (other == Remote.ReadResponse.getDefaultInstance()) {
                    return this;
                }
                if (resultsBuilder_ == null) {
                    if (!other.results_.isEmpty()) {
                        if (results_.isEmpty()) {
                            results_ = other.results_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                        } else {
                            ensureResultsIsMutable();
                            results_.addAll(other.results_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.results_.isEmpty()) {
                        if (resultsBuilder_.isEmpty()) {
                            resultsBuilder_.dispose();
                            resultsBuilder_ = null;
                            results_ = other.results_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                            resultsBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getResultsFieldBuilder()
                                            : null;
                        } else {
                            resultsBuilder_.addAllMessages(other.results_);
                        }
                    }
                }
                this.mergeUnknownFields(other.getUnknownFields());
                onChanged();
                return this;
            }

            @Override
            public final boolean isInitialized() {
                return true;
            }

            @Override
            public Builder mergeFrom(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException {
                if (extensionRegistry == null) {
                    throw new NullPointerException();
                }
                try {
                    boolean done = false;
                    while (!done) {
                        int tag = input.readTag();
                        switch (tag) {
                            case 0:
                                done = true;
                                break;
                            case 10:
                                {
                                    Remote.QueryResult m =
                                            input.readMessage(
                                                    Remote.QueryResult.parser(), extensionRegistry);
                                    if (resultsBuilder_ == null) {
                                        ensureResultsIsMutable();
                                        results_.add(m);
                                    } else {
                                        resultsBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 10
                            default:
                                {
                                    if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                                        done = true; // was an endgroup tag
                                    }
                                    break;
                                } // default:
                        } // switch (tag)
                    } // while (!done)
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    throw e.unwrapIOException();
                } finally {
                    onChanged();
                } // finally
                return this;
            }

            private int bitField0_;

            private java.util.List<Remote.QueryResult> results_ = java.util.Collections.emptyList();

            private void ensureResultsIsMutable() {
                if (!((bitField0_ & 0x00000001) != 0)) {
                    results_ = new java.util.ArrayList<Remote.QueryResult>(results_);
                    bitField0_ |= 0x00000001;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Remote.QueryResult,
                            Remote.QueryResult.Builder,
                            Remote.QueryResultOrBuilder>
                    resultsBuilder_;

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public java.util.List<Remote.QueryResult> getResultsList() {
                if (resultsBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(results_);
                } else {
                    return resultsBuilder_.getMessageList();
                }
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public int getResultsCount() {
                if (resultsBuilder_ == null) {
                    return results_.size();
                } else {
                    return resultsBuilder_.getCount();
                }
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Remote.QueryResult getResults(int index) {
                if (resultsBuilder_ == null) {
                    return results_.get(index);
                } else {
                    return resultsBuilder_.getMessage(index);
                }
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Builder setResults(int index, Remote.QueryResult value) {
                if (resultsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureResultsIsMutable();
                    results_.set(index, value);
                    onChanged();
                } else {
                    resultsBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Builder setResults(int index, Remote.QueryResult.Builder builderForValue) {
                if (resultsBuilder_ == null) {
                    ensureResultsIsMutable();
                    results_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    resultsBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Builder addResults(Remote.QueryResult value) {
                if (resultsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureResultsIsMutable();
                    results_.add(value);
                    onChanged();
                } else {
                    resultsBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Builder addResults(int index, Remote.QueryResult value) {
                if (resultsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureResultsIsMutable();
                    results_.add(index, value);
                    onChanged();
                } else {
                    resultsBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Builder addResults(Remote.QueryResult.Builder builderForValue) {
                if (resultsBuilder_ == null) {
                    ensureResultsIsMutable();
                    results_.add(builderForValue.build());
                    onChanged();
                } else {
                    resultsBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Builder addResults(int index, Remote.QueryResult.Builder builderForValue) {
                if (resultsBuilder_ == null) {
                    ensureResultsIsMutable();
                    results_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    resultsBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Builder addAllResults(Iterable<? extends Remote.QueryResult> values) {
                if (resultsBuilder_ == null) {
                    ensureResultsIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, results_);
                    onChanged();
                } else {
                    resultsBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Builder clearResults() {
                if (resultsBuilder_ == null) {
                    results_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000001);
                    onChanged();
                } else {
                    resultsBuilder_.clear();
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Builder removeResults(int index) {
                if (resultsBuilder_ == null) {
                    ensureResultsIsMutable();
                    results_.remove(index);
                    onChanged();
                } else {
                    resultsBuilder_.remove(index);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Remote.QueryResult.Builder getResultsBuilder(int index) {
                return getResultsFieldBuilder().getBuilder(index);
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Remote.QueryResultOrBuilder getResultsOrBuilder(int index) {
                if (resultsBuilder_ == null) {
                    return results_.get(index);
                } else {
                    return resultsBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public java.util.List<? extends Remote.QueryResultOrBuilder> getResultsOrBuilderList() {
                if (resultsBuilder_ != null) {
                    return resultsBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(results_);
                }
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Remote.QueryResult.Builder addResultsBuilder() {
                return getResultsFieldBuilder().addBuilder(Remote.QueryResult.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public Remote.QueryResult.Builder addResultsBuilder(int index) {
                return getResultsFieldBuilder()
                        .addBuilder(index, Remote.QueryResult.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * In same order as the request's queries.
             * </pre>
             *
             * <code>repeated .prometheus.QueryResult results = 1;</code>
             */
            public java.util.List<Remote.QueryResult.Builder> getResultsBuilderList() {
                return getResultsFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Remote.QueryResult,
                            Remote.QueryResult.Builder,
                            Remote.QueryResultOrBuilder>
                    getResultsFieldBuilder() {
                if (resultsBuilder_ == null) {
                    resultsBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Remote.QueryResult,
                                    Remote.QueryResult.Builder,
                                    Remote.QueryResultOrBuilder>(
                                    results_,
                                    ((bitField0_ & 0x00000001) != 0),
                                    getParentForChildren(),
                                    isClean());
                    results_ = null;
                }
                return resultsBuilder_;
            }

            @Override
            public final Builder setUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.setUnknownFields(unknownFields);
            }

            @Override
            public final Builder mergeUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.mergeUnknownFields(unknownFields);
            }

            // @@protoc_insertion_point(builder_scope:prometheus.ReadResponse)
        }

        // @@protoc_insertion_point(class_scope:prometheus.ReadResponse)
        private static final Remote.ReadResponse DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Remote.ReadResponse();
        }

        public static Remote.ReadResponse getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<ReadResponse> PARSER =
                new com.google.protobuf.AbstractParser<ReadResponse>() {
                    @Override
                    public ReadResponse parsePartialFrom(
                            com.google.protobuf.CodedInputStream input,
                            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                            throws com.google.protobuf.InvalidProtocolBufferException {
                        Builder builder = newBuilder();
                        try {
                            builder.mergeFrom(input, extensionRegistry);
                        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                            throw e.setUnfinishedMessage(builder.buildPartial());
                        } catch (com.google.protobuf.UninitializedMessageException e) {
                            throw e.asInvalidProtocolBufferException()
                                    .setUnfinishedMessage(builder.buildPartial());
                        } catch (java.io.IOException e) {
                            throw new com.google.protobuf.InvalidProtocolBufferException(e)
                                    .setUnfinishedMessage(builder.buildPartial());
                        }
                        return builder.buildPartial();
                    }
                };

        public static com.google.protobuf.Parser<ReadResponse> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<ReadResponse> getParserForType() {
            return PARSER;
        }

        @Override
        public Remote.ReadResponse getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface QueryOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.Query)
            com.google.protobuf.MessageOrBuilder {

        /**
         * <code>int64 start_timestamp_ms = 1;</code>
         *
         * @return The startTimestampMs.
         */
        long getStartTimestampMs();

        /**
         * <code>int64 end_timestamp_ms = 2;</code>
         *
         * @return The endTimestampMs.
         */
        long getEndTimestampMs();

        /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
        java.util.List<Types.LabelMatcher> getMatchersList();

        /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
        Types.LabelMatcher getMatchers(int index);

        /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
        int getMatchersCount();

        /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
        java.util.List<? extends Types.LabelMatcherOrBuilder> getMatchersOrBuilderList();

        /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
        Types.LabelMatcherOrBuilder getMatchersOrBuilder(int index);

        /**
         * <code>.prometheus.ReadHints hints = 4;</code>
         *
         * @return Whether the hints field is set.
         */
        boolean hasHints();

        /**
         * <code>.prometheus.ReadHints hints = 4;</code>
         *
         * @return The hints.
         */
        Types.ReadHints getHints();

        /** <code>.prometheus.ReadHints hints = 4;</code> */
        Types.ReadHintsOrBuilder getHintsOrBuilder();
    }

    /** Protobuf type {@code prometheus.Query} */
    public static final class Query extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.Query)
            QueryOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use Query.newBuilder() to construct.
        private Query(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private Query() {
            matchers_ = java.util.Collections.emptyList();
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new Query();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Remote.internal_static_prometheus_Query_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Remote.internal_static_prometheus_Query_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Remote.Query.class, Remote.Query.Builder.class);
        }

        private int bitField0_;
        public static final int START_TIMESTAMP_MS_FIELD_NUMBER = 1;
        private long startTimestampMs_ = 0L;

        /**
         * <code>int64 start_timestamp_ms = 1;</code>
         *
         * @return The startTimestampMs.
         */
        @Override
        public long getStartTimestampMs() {
            return startTimestampMs_;
        }

        public static final int END_TIMESTAMP_MS_FIELD_NUMBER = 2;
        private long endTimestampMs_ = 0L;

        /**
         * <code>int64 end_timestamp_ms = 2;</code>
         *
         * @return The endTimestampMs.
         */
        @Override
        public long getEndTimestampMs() {
            return endTimestampMs_;
        }

        public static final int MATCHERS_FIELD_NUMBER = 3;

        @SuppressWarnings("serial")
        private java.util.List<Types.LabelMatcher> matchers_;

        /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
        @Override
        public java.util.List<Types.LabelMatcher> getMatchersList() {
            return matchers_;
        }

        /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
        @Override
        public java.util.List<? extends Types.LabelMatcherOrBuilder> getMatchersOrBuilderList() {
            return matchers_;
        }

        /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
        @Override
        public int getMatchersCount() {
            return matchers_.size();
        }

        /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
        @Override
        public Types.LabelMatcher getMatchers(int index) {
            return matchers_.get(index);
        }

        /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
        @Override
        public Types.LabelMatcherOrBuilder getMatchersOrBuilder(int index) {
            return matchers_.get(index);
        }

        public static final int HINTS_FIELD_NUMBER = 4;
        private Types.ReadHints hints_;

        /**
         * <code>.prometheus.ReadHints hints = 4;</code>
         *
         * @return Whether the hints field is set.
         */
        @Override
        public boolean hasHints() {
            return ((bitField0_ & 0x00000001) != 0);
        }

        /**
         * <code>.prometheus.ReadHints hints = 4;</code>
         *
         * @return The hints.
         */
        @Override
        public Types.ReadHints getHints() {
            return hints_ == null ? Types.ReadHints.getDefaultInstance() : hints_;
        }

        /** <code>.prometheus.ReadHints hints = 4;</code> */
        @Override
        public Types.ReadHintsOrBuilder getHintsOrBuilder() {
            return hints_ == null ? Types.ReadHints.getDefaultInstance() : hints_;
        }

        private byte memoizedIsInitialized = -1;

        @Override
        public final boolean isInitialized() {
            byte isInitialized = memoizedIsInitialized;
            if (isInitialized == 1) {
                return true;
            }
            if (isInitialized == 0) {
                return false;
            }

            memoizedIsInitialized = 1;
            return true;
        }

        @Override
        public void writeTo(com.google.protobuf.CodedOutputStream output)
                throws java.io.IOException {
            if (startTimestampMs_ != 0L) {
                output.writeInt64(1, startTimestampMs_);
            }
            if (endTimestampMs_ != 0L) {
                output.writeInt64(2, endTimestampMs_);
            }
            for (int i = 0; i < matchers_.size(); i++) {
                output.writeMessage(3, matchers_.get(i));
            }
            if (((bitField0_ & 0x00000001) != 0)) {
                output.writeMessage(4, getHints());
            }
            getUnknownFields().writeTo(output);
        }

        @Override
        public int getSerializedSize() {
            int size = memoizedSize;
            if (size != -1) {
                return size;
            }

            size = 0;
            if (startTimestampMs_ != 0L) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeInt64Size(
                                1, startTimestampMs_);
            }
            if (endTimestampMs_ != 0L) {
                size += com.google.protobuf.CodedOutputStream.computeInt64Size(2, endTimestampMs_);
            }
            for (int i = 0; i < matchers_.size(); i++) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeMessageSize(
                                3, matchers_.get(i));
            }
            if (((bitField0_ & 0x00000001) != 0)) {
                size += com.google.protobuf.CodedOutputStream.computeMessageSize(4, getHints());
            }
            size += getUnknownFields().getSerializedSize();
            memoizedSize = size;
            return size;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof Remote.Query)) {
                return super.equals(obj);
            }
            Remote.Query other = (Remote.Query) obj;

            if (getStartTimestampMs() != other.getStartTimestampMs()) {
                return false;
            }
            if (getEndTimestampMs() != other.getEndTimestampMs()) {
                return false;
            }
            if (!getMatchersList().equals(other.getMatchersList())) {
                return false;
            }
            if (hasHints() != other.hasHints()) {
                return false;
            }
            if (hasHints()) {
                if (!getHints().equals(other.getHints())) {
                    return false;
                }
            }
            if (!getUnknownFields().equals(other.getUnknownFields())) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            if (memoizedHashCode != 0) {
                return memoizedHashCode;
            }
            int hash = 41;
            hash = (19 * hash) + getDescriptor().hashCode();
            hash = (37 * hash) + START_TIMESTAMP_MS_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getStartTimestampMs());
            hash = (37 * hash) + END_TIMESTAMP_MS_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getEndTimestampMs());
            if (getMatchersCount() > 0) {
                hash = (37 * hash) + MATCHERS_FIELD_NUMBER;
                hash = (53 * hash) + getMatchersList().hashCode();
            }
            if (hasHints()) {
                hash = (37 * hash) + HINTS_FIELD_NUMBER;
                hash = (53 * hash) + getHints().hashCode();
            }
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Remote.Query parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.Query parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.Query parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.Query parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.Query parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.Query parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.Query parseFrom(java.io.InputStream input) throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Remote.Query parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Remote.Query parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Remote.Query parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Remote.Query parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Remote.Query parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        @Override
        public Builder newBuilderForType() {
            return newBuilder();
        }

        public static Builder newBuilder() {
            return DEFAULT_INSTANCE.toBuilder();
        }

        public static Builder newBuilder(Remote.Query prototype) {
            return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }

        @Override
        public Builder toBuilder() {
            return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
        }

        @Override
        protected Builder newBuilderForType(BuilderParent parent) {
            Builder builder = new Builder(parent);
            return builder;
        }

        /** Protobuf type {@code prometheus.Query} */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.Query)
                Remote.QueryOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Remote.internal_static_prometheus_Query_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Remote.internal_static_prometheus_Query_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Remote.Query.class, Remote.Query.Builder.class);
            }

            // Construct using Remote.Query.newBuilder()
            private Builder() {
                maybeForceBuilderInitialization();
            }

            private Builder(BuilderParent parent) {
                super(parent);
                maybeForceBuilderInitialization();
            }

            private void maybeForceBuilderInitialization() {
                if (com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders) {
                    getMatchersFieldBuilder();
                    getHintsFieldBuilder();
                }
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                startTimestampMs_ = 0L;
                endTimestampMs_ = 0L;
                if (matchersBuilder_ == null) {
                    matchers_ = java.util.Collections.emptyList();
                } else {
                    matchers_ = null;
                    matchersBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000004);
                hints_ = null;
                if (hintsBuilder_ != null) {
                    hintsBuilder_.dispose();
                    hintsBuilder_ = null;
                }
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Remote.internal_static_prometheus_Query_descriptor;
            }

            @Override
            public Remote.Query getDefaultInstanceForType() {
                return Remote.Query.getDefaultInstance();
            }

            @Override
            public Remote.Query build() {
                Remote.Query result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Remote.Query buildPartial() {
                Remote.Query result = new Remote.Query(this);
                buildPartialRepeatedFields(result);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartialRepeatedFields(Remote.Query result) {
                if (matchersBuilder_ == null) {
                    if (((bitField0_ & 0x00000004) != 0)) {
                        matchers_ = java.util.Collections.unmodifiableList(matchers_);
                        bitField0_ = (bitField0_ & ~0x00000004);
                    }
                    result.matchers_ = matchers_;
                } else {
                    result.matchers_ = matchersBuilder_.build();
                }
            }

            private void buildPartial0(Remote.Query result) {
                int from_bitField0_ = bitField0_;
                if (((from_bitField0_ & 0x00000001) != 0)) {
                    result.startTimestampMs_ = startTimestampMs_;
                }
                if (((from_bitField0_ & 0x00000002) != 0)) {
                    result.endTimestampMs_ = endTimestampMs_;
                }
                int to_bitField0_ = 0;
                if (((from_bitField0_ & 0x00000008) != 0)) {
                    result.hints_ = hintsBuilder_ == null ? hints_ : hintsBuilder_.build();
                    to_bitField0_ |= 0x00000001;
                }
                result.bitField0_ |= to_bitField0_;
            }

            @Override
            public Builder clone() {
                return super.clone();
            }

            @Override
            public Builder setField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, Object value) {
                return super.setField(field, value);
            }

            @Override
            public Builder clearField(com.google.protobuf.Descriptors.FieldDescriptor field) {
                return super.clearField(field);
            }

            @Override
            public Builder clearOneof(com.google.protobuf.Descriptors.OneofDescriptor oneof) {
                return super.clearOneof(oneof);
            }

            @Override
            public Builder setRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    int index,
                    Object value) {
                return super.setRepeatedField(field, index, value);
            }

            @Override
            public Builder addRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, Object value) {
                return super.addRepeatedField(field, value);
            }

            @Override
            public Builder mergeFrom(com.google.protobuf.Message other) {
                if (other instanceof Remote.Query) {
                    return mergeFrom((Remote.Query) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Remote.Query other) {
                if (other == Remote.Query.getDefaultInstance()) {
                    return this;
                }
                if (other.getStartTimestampMs() != 0L) {
                    setStartTimestampMs(other.getStartTimestampMs());
                }
                if (other.getEndTimestampMs() != 0L) {
                    setEndTimestampMs(other.getEndTimestampMs());
                }
                if (matchersBuilder_ == null) {
                    if (!other.matchers_.isEmpty()) {
                        if (matchers_.isEmpty()) {
                            matchers_ = other.matchers_;
                            bitField0_ = (bitField0_ & ~0x00000004);
                        } else {
                            ensureMatchersIsMutable();
                            matchers_.addAll(other.matchers_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.matchers_.isEmpty()) {
                        if (matchersBuilder_.isEmpty()) {
                            matchersBuilder_.dispose();
                            matchersBuilder_ = null;
                            matchers_ = other.matchers_;
                            bitField0_ = (bitField0_ & ~0x00000004);
                            matchersBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getMatchersFieldBuilder()
                                            : null;
                        } else {
                            matchersBuilder_.addAllMessages(other.matchers_);
                        }
                    }
                }
                if (other.hasHints()) {
                    mergeHints(other.getHints());
                }
                this.mergeUnknownFields(other.getUnknownFields());
                onChanged();
                return this;
            }

            @Override
            public final boolean isInitialized() {
                return true;
            }

            @Override
            public Builder mergeFrom(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException {
                if (extensionRegistry == null) {
                    throw new NullPointerException();
                }
                try {
                    boolean done = false;
                    while (!done) {
                        int tag = input.readTag();
                        switch (tag) {
                            case 0:
                                done = true;
                                break;
                            case 8:
                                {
                                    startTimestampMs_ = input.readInt64();
                                    bitField0_ |= 0x00000001;
                                    break;
                                } // case 8
                            case 16:
                                {
                                    endTimestampMs_ = input.readInt64();
                                    bitField0_ |= 0x00000002;
                                    break;
                                } // case 16
                            case 26:
                                {
                                    Types.LabelMatcher m =
                                            input.readMessage(
                                                    Types.LabelMatcher.parser(), extensionRegistry);
                                    if (matchersBuilder_ == null) {
                                        ensureMatchersIsMutable();
                                        matchers_.add(m);
                                    } else {
                                        matchersBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 26
                            case 34:
                                {
                                    input.readMessage(
                                            getHintsFieldBuilder().getBuilder(), extensionRegistry);
                                    bitField0_ |= 0x00000008;
                                    break;
                                } // case 34
                            default:
                                {
                                    if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                                        done = true; // was an endgroup tag
                                    }
                                    break;
                                } // default:
                        } // switch (tag)
                    } // while (!done)
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    throw e.unwrapIOException();
                } finally {
                    onChanged();
                } // finally
                return this;
            }

            private int bitField0_;

            private long startTimestampMs_;

            /**
             * <code>int64 start_timestamp_ms = 1;</code>
             *
             * @return The startTimestampMs.
             */
            @Override
            public long getStartTimestampMs() {
                return startTimestampMs_;
            }

            /**
             * <code>int64 start_timestamp_ms = 1;</code>
             *
             * @param value The startTimestampMs to set.
             * @return This builder for chaining.
             */
            public Builder setStartTimestampMs(long value) {

                startTimestampMs_ = value;
                bitField0_ |= 0x00000001;
                onChanged();
                return this;
            }

            /**
             * <code>int64 start_timestamp_ms = 1;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearStartTimestampMs() {
                bitField0_ = (bitField0_ & ~0x00000001);
                startTimestampMs_ = 0L;
                onChanged();
                return this;
            }

            private long endTimestampMs_;

            /**
             * <code>int64 end_timestamp_ms = 2;</code>
             *
             * @return The endTimestampMs.
             */
            @Override
            public long getEndTimestampMs() {
                return endTimestampMs_;
            }

            /**
             * <code>int64 end_timestamp_ms = 2;</code>
             *
             * @param value The endTimestampMs to set.
             * @return This builder for chaining.
             */
            public Builder setEndTimestampMs(long value) {

                endTimestampMs_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            /**
             * <code>int64 end_timestamp_ms = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearEndTimestampMs() {
                bitField0_ = (bitField0_ & ~0x00000002);
                endTimestampMs_ = 0L;
                onChanged();
                return this;
            }

            private java.util.List<Types.LabelMatcher> matchers_ =
                    java.util.Collections.emptyList();

            private void ensureMatchersIsMutable() {
                if (!((bitField0_ & 0x00000004) != 0)) {
                    matchers_ = new java.util.ArrayList<Types.LabelMatcher>(matchers_);
                    bitField0_ |= 0x00000004;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.LabelMatcher,
                            Types.LabelMatcher.Builder,
                            Types.LabelMatcherOrBuilder>
                    matchersBuilder_;

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public java.util.List<Types.LabelMatcher> getMatchersList() {
                if (matchersBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(matchers_);
                } else {
                    return matchersBuilder_.getMessageList();
                }
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public int getMatchersCount() {
                if (matchersBuilder_ == null) {
                    return matchers_.size();
                } else {
                    return matchersBuilder_.getCount();
                }
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Types.LabelMatcher getMatchers(int index) {
                if (matchersBuilder_ == null) {
                    return matchers_.get(index);
                } else {
                    return matchersBuilder_.getMessage(index);
                }
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Builder setMatchers(int index, Types.LabelMatcher value) {
                if (matchersBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureMatchersIsMutable();
                    matchers_.set(index, value);
                    onChanged();
                } else {
                    matchersBuilder_.setMessage(index, value);
                }
                return this;
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Builder setMatchers(int index, Types.LabelMatcher.Builder builderForValue) {
                if (matchersBuilder_ == null) {
                    ensureMatchersIsMutable();
                    matchers_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    matchersBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Builder addMatchers(Types.LabelMatcher value) {
                if (matchersBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureMatchersIsMutable();
                    matchers_.add(value);
                    onChanged();
                } else {
                    matchersBuilder_.addMessage(value);
                }
                return this;
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Builder addMatchers(int index, Types.LabelMatcher value) {
                if (matchersBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureMatchersIsMutable();
                    matchers_.add(index, value);
                    onChanged();
                } else {
                    matchersBuilder_.addMessage(index, value);
                }
                return this;
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Builder addMatchers(Types.LabelMatcher.Builder builderForValue) {
                if (matchersBuilder_ == null) {
                    ensureMatchersIsMutable();
                    matchers_.add(builderForValue.build());
                    onChanged();
                } else {
                    matchersBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Builder addMatchers(int index, Types.LabelMatcher.Builder builderForValue) {
                if (matchersBuilder_ == null) {
                    ensureMatchersIsMutable();
                    matchers_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    matchersBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Builder addAllMatchers(Iterable<? extends Types.LabelMatcher> values) {
                if (matchersBuilder_ == null) {
                    ensureMatchersIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, matchers_);
                    onChanged();
                } else {
                    matchersBuilder_.addAllMessages(values);
                }
                return this;
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Builder clearMatchers() {
                if (matchersBuilder_ == null) {
                    matchers_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000004);
                    onChanged();
                } else {
                    matchersBuilder_.clear();
                }
                return this;
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Builder removeMatchers(int index) {
                if (matchersBuilder_ == null) {
                    ensureMatchersIsMutable();
                    matchers_.remove(index);
                    onChanged();
                } else {
                    matchersBuilder_.remove(index);
                }
                return this;
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Types.LabelMatcher.Builder getMatchersBuilder(int index) {
                return getMatchersFieldBuilder().getBuilder(index);
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Types.LabelMatcherOrBuilder getMatchersOrBuilder(int index) {
                if (matchersBuilder_ == null) {
                    return matchers_.get(index);
                } else {
                    return matchersBuilder_.getMessageOrBuilder(index);
                }
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public java.util.List<? extends Types.LabelMatcherOrBuilder>
                    getMatchersOrBuilderList() {
                if (matchersBuilder_ != null) {
                    return matchersBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(matchers_);
                }
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Types.LabelMatcher.Builder addMatchersBuilder() {
                return getMatchersFieldBuilder()
                        .addBuilder(Types.LabelMatcher.getDefaultInstance());
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public Types.LabelMatcher.Builder addMatchersBuilder(int index) {
                return getMatchersFieldBuilder()
                        .addBuilder(index, Types.LabelMatcher.getDefaultInstance());
            }

            /** <code>repeated .prometheus.LabelMatcher matchers = 3;</code> */
            public java.util.List<Types.LabelMatcher.Builder> getMatchersBuilderList() {
                return getMatchersFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.LabelMatcher,
                            Types.LabelMatcher.Builder,
                            Types.LabelMatcherOrBuilder>
                    getMatchersFieldBuilder() {
                if (matchersBuilder_ == null) {
                    matchersBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.LabelMatcher,
                                    Types.LabelMatcher.Builder,
                                    Types.LabelMatcherOrBuilder>(
                                    matchers_,
                                    ((bitField0_ & 0x00000004) != 0),
                                    getParentForChildren(),
                                    isClean());
                    matchers_ = null;
                }
                return matchersBuilder_;
            }

            private Types.ReadHints hints_;
            private com.google.protobuf.SingleFieldBuilderV3<
                            Types.ReadHints, Types.ReadHints.Builder, Types.ReadHintsOrBuilder>
                    hintsBuilder_;

            /**
             * <code>.prometheus.ReadHints hints = 4;</code>
             *
             * @return Whether the hints field is set.
             */
            public boolean hasHints() {
                return ((bitField0_ & 0x00000008) != 0);
            }

            /**
             * <code>.prometheus.ReadHints hints = 4;</code>
             *
             * @return The hints.
             */
            public Types.ReadHints getHints() {
                if (hintsBuilder_ == null) {
                    return hints_ == null ? Types.ReadHints.getDefaultInstance() : hints_;
                } else {
                    return hintsBuilder_.getMessage();
                }
            }

            /** <code>.prometheus.ReadHints hints = 4;</code> */
            public Builder setHints(Types.ReadHints value) {
                if (hintsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    hints_ = value;
                } else {
                    hintsBuilder_.setMessage(value);
                }
                bitField0_ |= 0x00000008;
                onChanged();
                return this;
            }

            /** <code>.prometheus.ReadHints hints = 4;</code> */
            public Builder setHints(Types.ReadHints.Builder builderForValue) {
                if (hintsBuilder_ == null) {
                    hints_ = builderForValue.build();
                } else {
                    hintsBuilder_.setMessage(builderForValue.build());
                }
                bitField0_ |= 0x00000008;
                onChanged();
                return this;
            }

            /** <code>.prometheus.ReadHints hints = 4;</code> */
            public Builder mergeHints(Types.ReadHints value) {
                if (hintsBuilder_ == null) {
                    if (((bitField0_ & 0x00000008) != 0)
                            && hints_ != null
                            && hints_ != Types.ReadHints.getDefaultInstance()) {
                        getHintsBuilder().mergeFrom(value);
                    } else {
                        hints_ = value;
                    }
                } else {
                    hintsBuilder_.mergeFrom(value);
                }
                if (hints_ != null) {
                    bitField0_ |= 0x00000008;
                    onChanged();
                }
                return this;
            }

            /** <code>.prometheus.ReadHints hints = 4;</code> */
            public Builder clearHints() {
                bitField0_ = (bitField0_ & ~0x00000008);
                hints_ = null;
                if (hintsBuilder_ != null) {
                    hintsBuilder_.dispose();
                    hintsBuilder_ = null;
                }
                onChanged();
                return this;
            }

            /** <code>.prometheus.ReadHints hints = 4;</code> */
            public Types.ReadHints.Builder getHintsBuilder() {
                bitField0_ |= 0x00000008;
                onChanged();
                return getHintsFieldBuilder().getBuilder();
            }

            /** <code>.prometheus.ReadHints hints = 4;</code> */
            public Types.ReadHintsOrBuilder getHintsOrBuilder() {
                if (hintsBuilder_ != null) {
                    return hintsBuilder_.getMessageOrBuilder();
                } else {
                    return hints_ == null ? Types.ReadHints.getDefaultInstance() : hints_;
                }
            }

            /** <code>.prometheus.ReadHints hints = 4;</code> */
            private com.google.protobuf.SingleFieldBuilderV3<
                            Types.ReadHints, Types.ReadHints.Builder, Types.ReadHintsOrBuilder>
                    getHintsFieldBuilder() {
                if (hintsBuilder_ == null) {
                    hintsBuilder_ =
                            new com.google.protobuf.SingleFieldBuilderV3<
                                    Types.ReadHints,
                                    Types.ReadHints.Builder,
                                    Types.ReadHintsOrBuilder>(
                                    getHints(), getParentForChildren(), isClean());
                    hints_ = null;
                }
                return hintsBuilder_;
            }

            @Override
            public final Builder setUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.setUnknownFields(unknownFields);
            }

            @Override
            public final Builder mergeUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.mergeUnknownFields(unknownFields);
            }

            // @@protoc_insertion_point(builder_scope:prometheus.Query)
        }

        // @@protoc_insertion_point(class_scope:prometheus.Query)
        private static final Remote.Query DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Remote.Query();
        }

        public static Remote.Query getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<Query> PARSER =
                new com.google.protobuf.AbstractParser<Query>() {
                    @Override
                    public Query parsePartialFrom(
                            com.google.protobuf.CodedInputStream input,
                            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                            throws com.google.protobuf.InvalidProtocolBufferException {
                        Builder builder = newBuilder();
                        try {
                            builder.mergeFrom(input, extensionRegistry);
                        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                            throw e.setUnfinishedMessage(builder.buildPartial());
                        } catch (com.google.protobuf.UninitializedMessageException e) {
                            throw e.asInvalidProtocolBufferException()
                                    .setUnfinishedMessage(builder.buildPartial());
                        } catch (java.io.IOException e) {
                            throw new com.google.protobuf.InvalidProtocolBufferException(e)
                                    .setUnfinishedMessage(builder.buildPartial());
                        }
                        return builder.buildPartial();
                    }
                };

        public static com.google.protobuf.Parser<Query> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<Query> getParserForType() {
            return PARSER;
        }

        @Override
        public Remote.Query getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface QueryResultOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.QueryResult)
            com.google.protobuf.MessageOrBuilder {

        /**
         *
         *
         * <pre>
         * Samples within a time series must be ordered by time.
         * </pre>
         *
         * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
         */
        java.util.List<Types.TimeSeries> getTimeseriesList();

        /**
         *
         *
         * <pre>
         * Samples within a time series must be ordered by time.
         * </pre>
         *
         * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
         */
        Types.TimeSeries getTimeseries(int index);

        /**
         *
         *
         * <pre>
         * Samples within a time series must be ordered by time.
         * </pre>
         *
         * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
         */
        int getTimeseriesCount();

        /**
         *
         *
         * <pre>
         * Samples within a time series must be ordered by time.
         * </pre>
         *
         * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
         */
        java.util.List<? extends Types.TimeSeriesOrBuilder> getTimeseriesOrBuilderList();

        /**
         *
         *
         * <pre>
         * Samples within a time series must be ordered by time.
         * </pre>
         *
         * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
         */
        Types.TimeSeriesOrBuilder getTimeseriesOrBuilder(int index);
    }

    /** Protobuf type {@code prometheus.QueryResult} */
    public static final class QueryResult extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.QueryResult)
            QueryResultOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use QueryResult.newBuilder() to construct.
        private QueryResult(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private QueryResult() {
            timeseries_ = java.util.Collections.emptyList();
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new QueryResult();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Remote.internal_static_prometheus_QueryResult_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Remote.internal_static_prometheus_QueryResult_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Remote.QueryResult.class, Remote.QueryResult.Builder.class);
        }

        public static final int TIMESERIES_FIELD_NUMBER = 1;

        @SuppressWarnings("serial")
        private java.util.List<Types.TimeSeries> timeseries_;

        /**
         *
         *
         * <pre>
         * Samples within a time series must be ordered by time.
         * </pre>
         *
         * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
         */
        @Override
        public java.util.List<Types.TimeSeries> getTimeseriesList() {
            return timeseries_;
        }

        /**
         *
         *
         * <pre>
         * Samples within a time series must be ordered by time.
         * </pre>
         *
         * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
         */
        @Override
        public java.util.List<? extends Types.TimeSeriesOrBuilder> getTimeseriesOrBuilderList() {
            return timeseries_;
        }

        /**
         *
         *
         * <pre>
         * Samples within a time series must be ordered by time.
         * </pre>
         *
         * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
         */
        @Override
        public int getTimeseriesCount() {
            return timeseries_.size();
        }

        /**
         *
         *
         * <pre>
         * Samples within a time series must be ordered by time.
         * </pre>
         *
         * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
         */
        @Override
        public Types.TimeSeries getTimeseries(int index) {
            return timeseries_.get(index);
        }

        /**
         *
         *
         * <pre>
         * Samples within a time series must be ordered by time.
         * </pre>
         *
         * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
         */
        @Override
        public Types.TimeSeriesOrBuilder getTimeseriesOrBuilder(int index) {
            return timeseries_.get(index);
        }

        private byte memoizedIsInitialized = -1;

        @Override
        public final boolean isInitialized() {
            byte isInitialized = memoizedIsInitialized;
            if (isInitialized == 1) {
                return true;
            }
            if (isInitialized == 0) {
                return false;
            }

            memoizedIsInitialized = 1;
            return true;
        }

        @Override
        public void writeTo(com.google.protobuf.CodedOutputStream output)
                throws java.io.IOException {
            for (int i = 0; i < timeseries_.size(); i++) {
                output.writeMessage(1, timeseries_.get(i));
            }
            getUnknownFields().writeTo(output);
        }

        @Override
        public int getSerializedSize() {
            int size = memoizedSize;
            if (size != -1) {
                return size;
            }

            size = 0;
            for (int i = 0; i < timeseries_.size(); i++) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeMessageSize(
                                1, timeseries_.get(i));
            }
            size += getUnknownFields().getSerializedSize();
            memoizedSize = size;
            return size;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof Remote.QueryResult)) {
                return super.equals(obj);
            }
            Remote.QueryResult other = (Remote.QueryResult) obj;

            if (!getTimeseriesList().equals(other.getTimeseriesList())) {
                return false;
            }
            if (!getUnknownFields().equals(other.getUnknownFields())) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            if (memoizedHashCode != 0) {
                return memoizedHashCode;
            }
            int hash = 41;
            hash = (19 * hash) + getDescriptor().hashCode();
            if (getTimeseriesCount() > 0) {
                hash = (37 * hash) + TIMESERIES_FIELD_NUMBER;
                hash = (53 * hash) + getTimeseriesList().hashCode();
            }
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Remote.QueryResult parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.QueryResult parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.QueryResult parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.QueryResult parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.QueryResult parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.QueryResult parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.QueryResult parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Remote.QueryResult parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Remote.QueryResult parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Remote.QueryResult parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Remote.QueryResult parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Remote.QueryResult parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        @Override
        public Builder newBuilderForType() {
            return newBuilder();
        }

        public static Builder newBuilder() {
            return DEFAULT_INSTANCE.toBuilder();
        }

        public static Builder newBuilder(Remote.QueryResult prototype) {
            return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }

        @Override
        public Builder toBuilder() {
            return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
        }

        @Override
        protected Builder newBuilderForType(BuilderParent parent) {
            Builder builder = new Builder(parent);
            return builder;
        }

        /** Protobuf type {@code prometheus.QueryResult} */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.QueryResult)
                Remote.QueryResultOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Remote.internal_static_prometheus_QueryResult_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Remote.internal_static_prometheus_QueryResult_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Remote.QueryResult.class, Remote.QueryResult.Builder.class);
            }

            // Construct using Remote.QueryResult.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                if (timeseriesBuilder_ == null) {
                    timeseries_ = java.util.Collections.emptyList();
                } else {
                    timeseries_ = null;
                    timeseriesBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000001);
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Remote.internal_static_prometheus_QueryResult_descriptor;
            }

            @Override
            public Remote.QueryResult getDefaultInstanceForType() {
                return Remote.QueryResult.getDefaultInstance();
            }

            @Override
            public Remote.QueryResult build() {
                Remote.QueryResult result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Remote.QueryResult buildPartial() {
                Remote.QueryResult result = new Remote.QueryResult(this);
                buildPartialRepeatedFields(result);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartialRepeatedFields(Remote.QueryResult result) {
                if (timeseriesBuilder_ == null) {
                    if (((bitField0_ & 0x00000001) != 0)) {
                        timeseries_ = java.util.Collections.unmodifiableList(timeseries_);
                        bitField0_ = (bitField0_ & ~0x00000001);
                    }
                    result.timeseries_ = timeseries_;
                } else {
                    result.timeseries_ = timeseriesBuilder_.build();
                }
            }

            private void buildPartial0(Remote.QueryResult result) {
                int from_bitField0_ = bitField0_;
            }

            @Override
            public Builder clone() {
                return super.clone();
            }

            @Override
            public Builder setField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, Object value) {
                return super.setField(field, value);
            }

            @Override
            public Builder clearField(com.google.protobuf.Descriptors.FieldDescriptor field) {
                return super.clearField(field);
            }

            @Override
            public Builder clearOneof(com.google.protobuf.Descriptors.OneofDescriptor oneof) {
                return super.clearOneof(oneof);
            }

            @Override
            public Builder setRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    int index,
                    Object value) {
                return super.setRepeatedField(field, index, value);
            }

            @Override
            public Builder addRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, Object value) {
                return super.addRepeatedField(field, value);
            }

            @Override
            public Builder mergeFrom(com.google.protobuf.Message other) {
                if (other instanceof Remote.QueryResult) {
                    return mergeFrom((Remote.QueryResult) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Remote.QueryResult other) {
                if (other == Remote.QueryResult.getDefaultInstance()) {
                    return this;
                }
                if (timeseriesBuilder_ == null) {
                    if (!other.timeseries_.isEmpty()) {
                        if (timeseries_.isEmpty()) {
                            timeseries_ = other.timeseries_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                        } else {
                            ensureTimeseriesIsMutable();
                            timeseries_.addAll(other.timeseries_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.timeseries_.isEmpty()) {
                        if (timeseriesBuilder_.isEmpty()) {
                            timeseriesBuilder_.dispose();
                            timeseriesBuilder_ = null;
                            timeseries_ = other.timeseries_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                            timeseriesBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getTimeseriesFieldBuilder()
                                            : null;
                        } else {
                            timeseriesBuilder_.addAllMessages(other.timeseries_);
                        }
                    }
                }
                this.mergeUnknownFields(other.getUnknownFields());
                onChanged();
                return this;
            }

            @Override
            public final boolean isInitialized() {
                return true;
            }

            @Override
            public Builder mergeFrom(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException {
                if (extensionRegistry == null) {
                    throw new NullPointerException();
                }
                try {
                    boolean done = false;
                    while (!done) {
                        int tag = input.readTag();
                        switch (tag) {
                            case 0:
                                done = true;
                                break;
                            case 10:
                                {
                                    Types.TimeSeries m =
                                            input.readMessage(
                                                    Types.TimeSeries.parser(), extensionRegistry);
                                    if (timeseriesBuilder_ == null) {
                                        ensureTimeseriesIsMutable();
                                        timeseries_.add(m);
                                    } else {
                                        timeseriesBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 10
                            default:
                                {
                                    if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                                        done = true; // was an endgroup tag
                                    }
                                    break;
                                } // default:
                        } // switch (tag)
                    } // while (!done)
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    throw e.unwrapIOException();
                } finally {
                    onChanged();
                } // finally
                return this;
            }

            private int bitField0_;

            private java.util.List<Types.TimeSeries> timeseries_ =
                    java.util.Collections.emptyList();

            private void ensureTimeseriesIsMutable() {
                if (!((bitField0_ & 0x00000001) != 0)) {
                    timeseries_ = new java.util.ArrayList<Types.TimeSeries>(timeseries_);
                    bitField0_ |= 0x00000001;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.TimeSeries, Types.TimeSeries.Builder, Types.TimeSeriesOrBuilder>
                    timeseriesBuilder_;

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public java.util.List<Types.TimeSeries> getTimeseriesList() {
                if (timeseriesBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(timeseries_);
                } else {
                    return timeseriesBuilder_.getMessageList();
                }
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public int getTimeseriesCount() {
                if (timeseriesBuilder_ == null) {
                    return timeseries_.size();
                } else {
                    return timeseriesBuilder_.getCount();
                }
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Types.TimeSeries getTimeseries(int index) {
                if (timeseriesBuilder_ == null) {
                    return timeseries_.get(index);
                } else {
                    return timeseriesBuilder_.getMessage(index);
                }
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Builder setTimeseries(int index, Types.TimeSeries value) {
                if (timeseriesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureTimeseriesIsMutable();
                    timeseries_.set(index, value);
                    onChanged();
                } else {
                    timeseriesBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Builder setTimeseries(int index, Types.TimeSeries.Builder builderForValue) {
                if (timeseriesBuilder_ == null) {
                    ensureTimeseriesIsMutable();
                    timeseries_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    timeseriesBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Builder addTimeseries(Types.TimeSeries value) {
                if (timeseriesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureTimeseriesIsMutable();
                    timeseries_.add(value);
                    onChanged();
                } else {
                    timeseriesBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Builder addTimeseries(int index, Types.TimeSeries value) {
                if (timeseriesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureTimeseriesIsMutable();
                    timeseries_.add(index, value);
                    onChanged();
                } else {
                    timeseriesBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Builder addTimeseries(Types.TimeSeries.Builder builderForValue) {
                if (timeseriesBuilder_ == null) {
                    ensureTimeseriesIsMutable();
                    timeseries_.add(builderForValue.build());
                    onChanged();
                } else {
                    timeseriesBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Builder addTimeseries(int index, Types.TimeSeries.Builder builderForValue) {
                if (timeseriesBuilder_ == null) {
                    ensureTimeseriesIsMutable();
                    timeseries_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    timeseriesBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Builder addAllTimeseries(Iterable<? extends Types.TimeSeries> values) {
                if (timeseriesBuilder_ == null) {
                    ensureTimeseriesIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, timeseries_);
                    onChanged();
                } else {
                    timeseriesBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Builder clearTimeseries() {
                if (timeseriesBuilder_ == null) {
                    timeseries_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000001);
                    onChanged();
                } else {
                    timeseriesBuilder_.clear();
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Builder removeTimeseries(int index) {
                if (timeseriesBuilder_ == null) {
                    ensureTimeseriesIsMutable();
                    timeseries_.remove(index);
                    onChanged();
                } else {
                    timeseriesBuilder_.remove(index);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Types.TimeSeries.Builder getTimeseriesBuilder(int index) {
                return getTimeseriesFieldBuilder().getBuilder(index);
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Types.TimeSeriesOrBuilder getTimeseriesOrBuilder(int index) {
                if (timeseriesBuilder_ == null) {
                    return timeseries_.get(index);
                } else {
                    return timeseriesBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public java.util.List<? extends Types.TimeSeriesOrBuilder>
                    getTimeseriesOrBuilderList() {
                if (timeseriesBuilder_ != null) {
                    return timeseriesBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(timeseries_);
                }
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Types.TimeSeries.Builder addTimeseriesBuilder() {
                return getTimeseriesFieldBuilder()
                        .addBuilder(Types.TimeSeries.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public Types.TimeSeries.Builder addTimeseriesBuilder(int index) {
                return getTimeseriesFieldBuilder()
                        .addBuilder(index, Types.TimeSeries.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * Samples within a time series must be ordered by time.
             * </pre>
             *
             * <code>repeated .prometheus.TimeSeries timeseries = 1;</code>
             */
            public java.util.List<Types.TimeSeries.Builder> getTimeseriesBuilderList() {
                return getTimeseriesFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.TimeSeries, Types.TimeSeries.Builder, Types.TimeSeriesOrBuilder>
                    getTimeseriesFieldBuilder() {
                if (timeseriesBuilder_ == null) {
                    timeseriesBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.TimeSeries,
                                    Types.TimeSeries.Builder,
                                    Types.TimeSeriesOrBuilder>(
                                    timeseries_,
                                    ((bitField0_ & 0x00000001) != 0),
                                    getParentForChildren(),
                                    isClean());
                    timeseries_ = null;
                }
                return timeseriesBuilder_;
            }

            @Override
            public final Builder setUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.setUnknownFields(unknownFields);
            }

            @Override
            public final Builder mergeUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.mergeUnknownFields(unknownFields);
            }

            // @@protoc_insertion_point(builder_scope:prometheus.QueryResult)
        }

        // @@protoc_insertion_point(class_scope:prometheus.QueryResult)
        private static final Remote.QueryResult DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Remote.QueryResult();
        }

        public static Remote.QueryResult getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<QueryResult> PARSER =
                new com.google.protobuf.AbstractParser<QueryResult>() {
                    @Override
                    public QueryResult parsePartialFrom(
                            com.google.protobuf.CodedInputStream input,
                            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                            throws com.google.protobuf.InvalidProtocolBufferException {
                        Builder builder = newBuilder();
                        try {
                            builder.mergeFrom(input, extensionRegistry);
                        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                            throw e.setUnfinishedMessage(builder.buildPartial());
                        } catch (com.google.protobuf.UninitializedMessageException e) {
                            throw e.asInvalidProtocolBufferException()
                                    .setUnfinishedMessage(builder.buildPartial());
                        } catch (java.io.IOException e) {
                            throw new com.google.protobuf.InvalidProtocolBufferException(e)
                                    .setUnfinishedMessage(builder.buildPartial());
                        }
                        return builder.buildPartial();
                    }
                };

        public static com.google.protobuf.Parser<QueryResult> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<QueryResult> getParserForType() {
            return PARSER;
        }

        @Override
        public Remote.QueryResult getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface ChunkedReadResponseOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.ChunkedReadResponse)
            com.google.protobuf.MessageOrBuilder {

        /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
        java.util.List<Types.ChunkedSeries> getChunkedSeriesList();

        /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
        Types.ChunkedSeries getChunkedSeries(int index);

        /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
        int getChunkedSeriesCount();

        /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
        java.util.List<? extends Types.ChunkedSeriesOrBuilder> getChunkedSeriesOrBuilderList();

        /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
        Types.ChunkedSeriesOrBuilder getChunkedSeriesOrBuilder(int index);

        /**
         *
         *
         * <pre>
         * query_index represents an index of the query from ReadRequest.queries these chunks relates to.
         * </pre>
         *
         * <code>int64 query_index = 2;</code>
         *
         * @return The queryIndex.
         */
        long getQueryIndex();
    }

    /**
     *
     *
     * <pre>
     * ChunkedReadResponse is a response when response_type equals STREAMED_XOR_CHUNKS.
     * We strictly stream full series after series, optionally split by time. This means that a single frame can contain
     * partition of the single series, but once a new series is started to be streamed it means that no more chunks will
     * be sent for previous one. Series are returned sorted in the same way TSDB block are internally.
     * </pre>
     *
     * <p>Protobuf type {@code prometheus.ChunkedReadResponse}
     */
    public static final class ChunkedReadResponse extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.ChunkedReadResponse)
            ChunkedReadResponseOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use ChunkedReadResponse.newBuilder() to construct.
        private ChunkedReadResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private ChunkedReadResponse() {
            chunkedSeries_ = java.util.Collections.emptyList();
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new ChunkedReadResponse();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Remote.internal_static_prometheus_ChunkedReadResponse_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Remote.internal_static_prometheus_ChunkedReadResponse_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Remote.ChunkedReadResponse.class,
                            Remote.ChunkedReadResponse.Builder.class);
        }

        public static final int CHUNKED_SERIES_FIELD_NUMBER = 1;

        @SuppressWarnings("serial")
        private java.util.List<Types.ChunkedSeries> chunkedSeries_;

        /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
        @Override
        public java.util.List<Types.ChunkedSeries> getChunkedSeriesList() {
            return chunkedSeries_;
        }

        /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
        @Override
        public java.util.List<? extends Types.ChunkedSeriesOrBuilder>
                getChunkedSeriesOrBuilderList() {
            return chunkedSeries_;
        }

        /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
        @Override
        public int getChunkedSeriesCount() {
            return chunkedSeries_.size();
        }

        /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
        @Override
        public Types.ChunkedSeries getChunkedSeries(int index) {
            return chunkedSeries_.get(index);
        }

        /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
        @Override
        public Types.ChunkedSeriesOrBuilder getChunkedSeriesOrBuilder(int index) {
            return chunkedSeries_.get(index);
        }

        public static final int QUERY_INDEX_FIELD_NUMBER = 2;
        private long queryIndex_ = 0L;

        /**
         *
         *
         * <pre>
         * query_index represents an index of the query from ReadRequest.queries these chunks relates to.
         * </pre>
         *
         * <code>int64 query_index = 2;</code>
         *
         * @return The queryIndex.
         */
        @Override
        public long getQueryIndex() {
            return queryIndex_;
        }

        private byte memoizedIsInitialized = -1;

        @Override
        public final boolean isInitialized() {
            byte isInitialized = memoizedIsInitialized;
            if (isInitialized == 1) {
                return true;
            }
            if (isInitialized == 0) {
                return false;
            }

            memoizedIsInitialized = 1;
            return true;
        }

        @Override
        public void writeTo(com.google.protobuf.CodedOutputStream output)
                throws java.io.IOException {
            for (int i = 0; i < chunkedSeries_.size(); i++) {
                output.writeMessage(1, chunkedSeries_.get(i));
            }
            if (queryIndex_ != 0L) {
                output.writeInt64(2, queryIndex_);
            }
            getUnknownFields().writeTo(output);
        }

        @Override
        public int getSerializedSize() {
            int size = memoizedSize;
            if (size != -1) {
                return size;
            }

            size = 0;
            for (int i = 0; i < chunkedSeries_.size(); i++) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeMessageSize(
                                1, chunkedSeries_.get(i));
            }
            if (queryIndex_ != 0L) {
                size += com.google.protobuf.CodedOutputStream.computeInt64Size(2, queryIndex_);
            }
            size += getUnknownFields().getSerializedSize();
            memoizedSize = size;
            return size;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof Remote.ChunkedReadResponse)) {
                return super.equals(obj);
            }
            Remote.ChunkedReadResponse other = (Remote.ChunkedReadResponse) obj;

            if (!getChunkedSeriesList().equals(other.getChunkedSeriesList())) {
                return false;
            }
            if (getQueryIndex() != other.getQueryIndex()) {
                return false;
            }
            if (!getUnknownFields().equals(other.getUnknownFields())) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            if (memoizedHashCode != 0) {
                return memoizedHashCode;
            }
            int hash = 41;
            hash = (19 * hash) + getDescriptor().hashCode();
            if (getChunkedSeriesCount() > 0) {
                hash = (37 * hash) + CHUNKED_SERIES_FIELD_NUMBER;
                hash = (53 * hash) + getChunkedSeriesList().hashCode();
            }
            hash = (37 * hash) + QUERY_INDEX_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getQueryIndex());
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Remote.ChunkedReadResponse parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.ChunkedReadResponse parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.ChunkedReadResponse parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.ChunkedReadResponse parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.ChunkedReadResponse parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Remote.ChunkedReadResponse parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Remote.ChunkedReadResponse parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Remote.ChunkedReadResponse parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Remote.ChunkedReadResponse parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Remote.ChunkedReadResponse parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Remote.ChunkedReadResponse parseFrom(
                com.google.protobuf.CodedInputStream input) throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Remote.ChunkedReadResponse parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        @Override
        public Builder newBuilderForType() {
            return newBuilder();
        }

        public static Builder newBuilder() {
            return DEFAULT_INSTANCE.toBuilder();
        }

        public static Builder newBuilder(Remote.ChunkedReadResponse prototype) {
            return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }

        @Override
        public Builder toBuilder() {
            return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
        }

        @Override
        protected Builder newBuilderForType(BuilderParent parent) {
            Builder builder = new Builder(parent);
            return builder;
        }

        /**
         *
         *
         * <pre>
         * ChunkedReadResponse is a response when response_type equals STREAMED_XOR_CHUNKS.
         * We strictly stream full series after series, optionally split by time. This means that a single frame can contain
         * partition of the single series, but once a new series is started to be streamed it means that no more chunks will
         * be sent for previous one. Series are returned sorted in the same way TSDB block are internally.
         * </pre>
         *
         * <p>Protobuf type {@code prometheus.ChunkedReadResponse}
         */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.ChunkedReadResponse)
                Remote.ChunkedReadResponseOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Remote.internal_static_prometheus_ChunkedReadResponse_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Remote.internal_static_prometheus_ChunkedReadResponse_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Remote.ChunkedReadResponse.class,
                                Remote.ChunkedReadResponse.Builder.class);
            }

            // Construct using Remote.ChunkedReadResponse.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                if (chunkedSeriesBuilder_ == null) {
                    chunkedSeries_ = java.util.Collections.emptyList();
                } else {
                    chunkedSeries_ = null;
                    chunkedSeriesBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000001);
                queryIndex_ = 0L;
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Remote.internal_static_prometheus_ChunkedReadResponse_descriptor;
            }

            @Override
            public Remote.ChunkedReadResponse getDefaultInstanceForType() {
                return Remote.ChunkedReadResponse.getDefaultInstance();
            }

            @Override
            public Remote.ChunkedReadResponse build() {
                Remote.ChunkedReadResponse result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Remote.ChunkedReadResponse buildPartial() {
                Remote.ChunkedReadResponse result = new Remote.ChunkedReadResponse(this);
                buildPartialRepeatedFields(result);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartialRepeatedFields(Remote.ChunkedReadResponse result) {
                if (chunkedSeriesBuilder_ == null) {
                    if (((bitField0_ & 0x00000001) != 0)) {
                        chunkedSeries_ = java.util.Collections.unmodifiableList(chunkedSeries_);
                        bitField0_ = (bitField0_ & ~0x00000001);
                    }
                    result.chunkedSeries_ = chunkedSeries_;
                } else {
                    result.chunkedSeries_ = chunkedSeriesBuilder_.build();
                }
            }

            private void buildPartial0(Remote.ChunkedReadResponse result) {
                int from_bitField0_ = bitField0_;
                if (((from_bitField0_ & 0x00000002) != 0)) {
                    result.queryIndex_ = queryIndex_;
                }
            }

            @Override
            public Builder clone() {
                return super.clone();
            }

            @Override
            public Builder setField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, Object value) {
                return super.setField(field, value);
            }

            @Override
            public Builder clearField(com.google.protobuf.Descriptors.FieldDescriptor field) {
                return super.clearField(field);
            }

            @Override
            public Builder clearOneof(com.google.protobuf.Descriptors.OneofDescriptor oneof) {
                return super.clearOneof(oneof);
            }

            @Override
            public Builder setRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    int index,
                    Object value) {
                return super.setRepeatedField(field, index, value);
            }

            @Override
            public Builder addRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, Object value) {
                return super.addRepeatedField(field, value);
            }

            @Override
            public Builder mergeFrom(com.google.protobuf.Message other) {
                if (other instanceof Remote.ChunkedReadResponse) {
                    return mergeFrom((Remote.ChunkedReadResponse) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Remote.ChunkedReadResponse other) {
                if (other == Remote.ChunkedReadResponse.getDefaultInstance()) {
                    return this;
                }
                if (chunkedSeriesBuilder_ == null) {
                    if (!other.chunkedSeries_.isEmpty()) {
                        if (chunkedSeries_.isEmpty()) {
                            chunkedSeries_ = other.chunkedSeries_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                        } else {
                            ensureChunkedSeriesIsMutable();
                            chunkedSeries_.addAll(other.chunkedSeries_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.chunkedSeries_.isEmpty()) {
                        if (chunkedSeriesBuilder_.isEmpty()) {
                            chunkedSeriesBuilder_.dispose();
                            chunkedSeriesBuilder_ = null;
                            chunkedSeries_ = other.chunkedSeries_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                            chunkedSeriesBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getChunkedSeriesFieldBuilder()
                                            : null;
                        } else {
                            chunkedSeriesBuilder_.addAllMessages(other.chunkedSeries_);
                        }
                    }
                }
                if (other.getQueryIndex() != 0L) {
                    setQueryIndex(other.getQueryIndex());
                }
                this.mergeUnknownFields(other.getUnknownFields());
                onChanged();
                return this;
            }

            @Override
            public final boolean isInitialized() {
                return true;
            }

            @Override
            public Builder mergeFrom(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException {
                if (extensionRegistry == null) {
                    throw new NullPointerException();
                }
                try {
                    boolean done = false;
                    while (!done) {
                        int tag = input.readTag();
                        switch (tag) {
                            case 0:
                                done = true;
                                break;
                            case 10:
                                {
                                    Types.ChunkedSeries m =
                                            input.readMessage(
                                                    Types.ChunkedSeries.parser(),
                                                    extensionRegistry);
                                    if (chunkedSeriesBuilder_ == null) {
                                        ensureChunkedSeriesIsMutable();
                                        chunkedSeries_.add(m);
                                    } else {
                                        chunkedSeriesBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 10
                            case 16:
                                {
                                    queryIndex_ = input.readInt64();
                                    bitField0_ |= 0x00000002;
                                    break;
                                } // case 16
                            default:
                                {
                                    if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                                        done = true; // was an endgroup tag
                                    }
                                    break;
                                } // default:
                        } // switch (tag)
                    } // while (!done)
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    throw e.unwrapIOException();
                } finally {
                    onChanged();
                } // finally
                return this;
            }

            private int bitField0_;

            private java.util.List<Types.ChunkedSeries> chunkedSeries_ =
                    java.util.Collections.emptyList();

            private void ensureChunkedSeriesIsMutable() {
                if (!((bitField0_ & 0x00000001) != 0)) {
                    chunkedSeries_ = new java.util.ArrayList<Types.ChunkedSeries>(chunkedSeries_);
                    bitField0_ |= 0x00000001;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.ChunkedSeries,
                            Types.ChunkedSeries.Builder,
                            Types.ChunkedSeriesOrBuilder>
                    chunkedSeriesBuilder_;

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public java.util.List<Types.ChunkedSeries> getChunkedSeriesList() {
                if (chunkedSeriesBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(chunkedSeries_);
                } else {
                    return chunkedSeriesBuilder_.getMessageList();
                }
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public int getChunkedSeriesCount() {
                if (chunkedSeriesBuilder_ == null) {
                    return chunkedSeries_.size();
                } else {
                    return chunkedSeriesBuilder_.getCount();
                }
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Types.ChunkedSeries getChunkedSeries(int index) {
                if (chunkedSeriesBuilder_ == null) {
                    return chunkedSeries_.get(index);
                } else {
                    return chunkedSeriesBuilder_.getMessage(index);
                }
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Builder setChunkedSeries(int index, Types.ChunkedSeries value) {
                if (chunkedSeriesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureChunkedSeriesIsMutable();
                    chunkedSeries_.set(index, value);
                    onChanged();
                } else {
                    chunkedSeriesBuilder_.setMessage(index, value);
                }
                return this;
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Builder setChunkedSeries(
                    int index, Types.ChunkedSeries.Builder builderForValue) {
                if (chunkedSeriesBuilder_ == null) {
                    ensureChunkedSeriesIsMutable();
                    chunkedSeries_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    chunkedSeriesBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Builder addChunkedSeries(Types.ChunkedSeries value) {
                if (chunkedSeriesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureChunkedSeriesIsMutable();
                    chunkedSeries_.add(value);
                    onChanged();
                } else {
                    chunkedSeriesBuilder_.addMessage(value);
                }
                return this;
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Builder addChunkedSeries(int index, Types.ChunkedSeries value) {
                if (chunkedSeriesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureChunkedSeriesIsMutable();
                    chunkedSeries_.add(index, value);
                    onChanged();
                } else {
                    chunkedSeriesBuilder_.addMessage(index, value);
                }
                return this;
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Builder addChunkedSeries(Types.ChunkedSeries.Builder builderForValue) {
                if (chunkedSeriesBuilder_ == null) {
                    ensureChunkedSeriesIsMutable();
                    chunkedSeries_.add(builderForValue.build());
                    onChanged();
                } else {
                    chunkedSeriesBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Builder addChunkedSeries(
                    int index, Types.ChunkedSeries.Builder builderForValue) {
                if (chunkedSeriesBuilder_ == null) {
                    ensureChunkedSeriesIsMutable();
                    chunkedSeries_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    chunkedSeriesBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Builder addAllChunkedSeries(Iterable<? extends Types.ChunkedSeries> values) {
                if (chunkedSeriesBuilder_ == null) {
                    ensureChunkedSeriesIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, chunkedSeries_);
                    onChanged();
                } else {
                    chunkedSeriesBuilder_.addAllMessages(values);
                }
                return this;
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Builder clearChunkedSeries() {
                if (chunkedSeriesBuilder_ == null) {
                    chunkedSeries_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000001);
                    onChanged();
                } else {
                    chunkedSeriesBuilder_.clear();
                }
                return this;
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Builder removeChunkedSeries(int index) {
                if (chunkedSeriesBuilder_ == null) {
                    ensureChunkedSeriesIsMutable();
                    chunkedSeries_.remove(index);
                    onChanged();
                } else {
                    chunkedSeriesBuilder_.remove(index);
                }
                return this;
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Types.ChunkedSeries.Builder getChunkedSeriesBuilder(int index) {
                return getChunkedSeriesFieldBuilder().getBuilder(index);
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Types.ChunkedSeriesOrBuilder getChunkedSeriesOrBuilder(int index) {
                if (chunkedSeriesBuilder_ == null) {
                    return chunkedSeries_.get(index);
                } else {
                    return chunkedSeriesBuilder_.getMessageOrBuilder(index);
                }
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public java.util.List<? extends Types.ChunkedSeriesOrBuilder>
                    getChunkedSeriesOrBuilderList() {
                if (chunkedSeriesBuilder_ != null) {
                    return chunkedSeriesBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(chunkedSeries_);
                }
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Types.ChunkedSeries.Builder addChunkedSeriesBuilder() {
                return getChunkedSeriesFieldBuilder()
                        .addBuilder(Types.ChunkedSeries.getDefaultInstance());
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public Types.ChunkedSeries.Builder addChunkedSeriesBuilder(int index) {
                return getChunkedSeriesFieldBuilder()
                        .addBuilder(index, Types.ChunkedSeries.getDefaultInstance());
            }

            /** <code>repeated .prometheus.ChunkedSeries chunked_series = 1;</code> */
            public java.util.List<Types.ChunkedSeries.Builder> getChunkedSeriesBuilderList() {
                return getChunkedSeriesFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.ChunkedSeries,
                            Types.ChunkedSeries.Builder,
                            Types.ChunkedSeriesOrBuilder>
                    getChunkedSeriesFieldBuilder() {
                if (chunkedSeriesBuilder_ == null) {
                    chunkedSeriesBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.ChunkedSeries,
                                    Types.ChunkedSeries.Builder,
                                    Types.ChunkedSeriesOrBuilder>(
                                    chunkedSeries_,
                                    ((bitField0_ & 0x00000001) != 0),
                                    getParentForChildren(),
                                    isClean());
                    chunkedSeries_ = null;
                }
                return chunkedSeriesBuilder_;
            }

            private long queryIndex_;

            /**
             *
             *
             * <pre>
             * query_index represents an index of the query from ReadRequest.queries these chunks relates to.
             * </pre>
             *
             * <code>int64 query_index = 2;</code>
             *
             * @return The queryIndex.
             */
            @Override
            public long getQueryIndex() {
                return queryIndex_;
            }

            /**
             *
             *
             * <pre>
             * query_index represents an index of the query from ReadRequest.queries these chunks relates to.
             * </pre>
             *
             * <code>int64 query_index = 2;</code>
             *
             * @param value The queryIndex to set.
             * @return This builder for chaining.
             */
            public Builder setQueryIndex(long value) {

                queryIndex_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * query_index represents an index of the query from ReadRequest.queries these chunks relates to.
             * </pre>
             *
             * <code>int64 query_index = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearQueryIndex() {
                bitField0_ = (bitField0_ & ~0x00000002);
                queryIndex_ = 0L;
                onChanged();
                return this;
            }

            @Override
            public final Builder setUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.setUnknownFields(unknownFields);
            }

            @Override
            public final Builder mergeUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.mergeUnknownFields(unknownFields);
            }

            // @@protoc_insertion_point(builder_scope:prometheus.ChunkedReadResponse)
        }

        // @@protoc_insertion_point(class_scope:prometheus.ChunkedReadResponse)
        private static final Remote.ChunkedReadResponse DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Remote.ChunkedReadResponse();
        }

        public static Remote.ChunkedReadResponse getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<ChunkedReadResponse> PARSER =
                new com.google.protobuf.AbstractParser<ChunkedReadResponse>() {
                    @Override
                    public ChunkedReadResponse parsePartialFrom(
                            com.google.protobuf.CodedInputStream input,
                            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                            throws com.google.protobuf.InvalidProtocolBufferException {
                        Builder builder = newBuilder();
                        try {
                            builder.mergeFrom(input, extensionRegistry);
                        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                            throw e.setUnfinishedMessage(builder.buildPartial());
                        } catch (com.google.protobuf.UninitializedMessageException e) {
                            throw e.asInvalidProtocolBufferException()
                                    .setUnfinishedMessage(builder.buildPartial());
                        } catch (java.io.IOException e) {
                            throw new com.google.protobuf.InvalidProtocolBufferException(e)
                                    .setUnfinishedMessage(builder.buildPartial());
                        }
                        return builder.buildPartial();
                    }
                };

        public static com.google.protobuf.Parser<ChunkedReadResponse> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<ChunkedReadResponse> getParserForType() {
            return PARSER;
        }

        @Override
        public Remote.ChunkedReadResponse getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_WriteRequest_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_WriteRequest_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_ReadRequest_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_ReadRequest_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_ReadResponse_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_ReadResponse_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_Query_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_Query_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_QueryResult_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_QueryResult_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_ChunkedReadResponse_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_ChunkedReadResponse_fieldAccessorTable;

    public static com.google.protobuf.Descriptors.FileDescriptor getDescriptor() {
        return descriptor;
    }

    private static com.google.protobuf.Descriptors.FileDescriptor descriptor;

    static {
        String[] descriptorData = {
            "\n\014remote.proto\022\nprometheus\032\013types.proto\032"
                    + "\ngogo.proto\"z\n\014WriteRequest\0220\n\ntimeserie"
                    + "s\030\001 \003(\0132\026.prometheus.TimeSeriesB\004\310\336\037\000\0222\n"
                    + "\010metadata\030\003 \003(\0132\032.prometheus.MetricMetad"
                    + "ataB\004\310\336\037\000J\004\010\002\020\003\"\256\001\n\013ReadRequest\022\"\n\007queri"
                    + "es\030\001 \003(\0132\021.prometheus.Query\022E\n\027accepted_"
                    + "response_types\030\002 \003(\0162$.prometheus.ReadRe"
                    + "quest.ResponseType\"4\n\014ResponseType\022\013\n\007SA"
                    + "MPLES\020\000\022\027\n\023STREAMED_XOR_CHUNKS\020\001\"8\n\014Read"
                    + "Response\022(\n\007results\030\001 \003(\0132\027.prometheus.Q"
                    + "ueryResult\"\217\001\n\005Query\022\032\n\022start_timestamp_"
                    + "ms\030\001 \001(\003\022\030\n\020end_timestamp_ms\030\002 \001(\003\022*\n\010ma"
                    + "tchers\030\003 \003(\0132\030.prometheus.LabelMatcher\022$"
                    + "\n\005hints\030\004 \001(\0132\025.prometheus.ReadHints\"9\n\013"
                    + "QueryResult\022*\n\ntimeseries\030\001 \003(\0132\026.promet"
                    + "heus.TimeSeries\"]\n\023ChunkedReadResponse\0221"
                    + "\n\016chunked_series\030\001 \003(\0132\031.prometheus.Chun"
                    + "kedSeries\022\023\n\013query_index\030\002 \001(\003B\010Z\006prompb"
                    + "b\006proto3"
        };
        descriptor =
                com.google.protobuf.Descriptors.FileDescriptor.internalBuildGeneratedFileFrom(
                        descriptorData,
                        new com.google.protobuf.Descriptors.FileDescriptor[] {
                            Types.getDescriptor(), GoGoProtos.getDescriptor(),
                        });
        internal_static_prometheus_WriteRequest_descriptor =
                getDescriptor().getMessageTypes().get(0);
        internal_static_prometheus_WriteRequest_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_WriteRequest_descriptor,
                        new String[] {
                            "Timeseries", "Metadata",
                        });
        internal_static_prometheus_ReadRequest_descriptor =
                getDescriptor().getMessageTypes().get(1);
        internal_static_prometheus_ReadRequest_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_ReadRequest_descriptor,
                        new String[] {
                            "Queries", "AcceptedResponseTypes",
                        });
        internal_static_prometheus_ReadResponse_descriptor =
                getDescriptor().getMessageTypes().get(2);
        internal_static_prometheus_ReadResponse_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_ReadResponse_descriptor,
                        new String[] {
                            "Results",
                        });
        internal_static_prometheus_Query_descriptor = getDescriptor().getMessageTypes().get(3);
        internal_static_prometheus_Query_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_Query_descriptor,
                        new String[] {
                            "StartTimestampMs", "EndTimestampMs", "Matchers", "Hints",
                        });
        internal_static_prometheus_QueryResult_descriptor =
                getDescriptor().getMessageTypes().get(4);
        internal_static_prometheus_QueryResult_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_QueryResult_descriptor,
                        new String[] {
                            "Timeseries",
                        });
        internal_static_prometheus_ChunkedReadResponse_descriptor =
                getDescriptor().getMessageTypes().get(5);
        internal_static_prometheus_ChunkedReadResponse_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_ChunkedReadResponse_descriptor,
                        new String[] {
                            "ChunkedSeries", "QueryIndex",
                        });
        com.google.protobuf.ExtensionRegistry registry =
                com.google.protobuf.ExtensionRegistry.newInstance();
        registry.add(GoGoProtos.nullable);
        com.google.protobuf.Descriptors.FileDescriptor.internalUpdateFileDescriptor(
                descriptor, registry);
        Types.getDescriptor();
        GoGoProtos.getDescriptor();
    }

    // @@protoc_insertion_point(outer_class_scope)
}
