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

public final class Types {
    private Types() {}

    public static void registerAllExtensions(com.google.protobuf.ExtensionRegistryLite registry) {}

    public static void registerAllExtensions(com.google.protobuf.ExtensionRegistry registry) {
        registerAllExtensions((com.google.protobuf.ExtensionRegistryLite) registry);
    }

    public interface MetricMetadataOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.MetricMetadata)
            com.google.protobuf.MessageOrBuilder {

        /**
         *
         *
         * <pre>
         * Represents the metric type, these match the set from Prometheus.
         * Refer to github.com/prometheus/common/model/metadata.go for details.
         * </pre>
         *
         * <code>.prometheus.MetricMetadata.MetricType type = 1;</code>
         *
         * @return The enum numeric value on the wire for type.
         */
        int getTypeValue();

        /**
         *
         *
         * <pre>
         * Represents the metric type, these match the set from Prometheus.
         * Refer to github.com/prometheus/common/model/metadata.go for details.
         * </pre>
         *
         * <code>.prometheus.MetricMetadata.MetricType type = 1;</code>
         *
         * @return The type.
         */
        Types.MetricMetadata.MetricType getType();

        /**
         * <code>string metric_family_name = 2;</code>
         *
         * @return The metricFamilyName.
         */
        String getMetricFamilyName();

        /**
         * <code>string metric_family_name = 2;</code>
         *
         * @return The bytes for metricFamilyName.
         */
        com.google.protobuf.ByteString getMetricFamilyNameBytes();

        /**
         * <code>string help = 4;</code>
         *
         * @return The help.
         */
        String getHelp();

        /**
         * <code>string help = 4;</code>
         *
         * @return The bytes for help.
         */
        com.google.protobuf.ByteString getHelpBytes();

        /**
         * <code>string unit = 5;</code>
         *
         * @return The unit.
         */
        String getUnit();

        /**
         * <code>string unit = 5;</code>
         *
         * @return The bytes for unit.
         */
        com.google.protobuf.ByteString getUnitBytes();
    }

    /** Protobuf type {@code prometheus.MetricMetadata} */
    public static final class MetricMetadata extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.MetricMetadata)
            MetricMetadataOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use MetricMetadata.newBuilder() to construct.
        private MetricMetadata(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private MetricMetadata() {
            type_ = 0;
            metricFamilyName_ = "";
            help_ = "";
            unit_ = "";
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new MetricMetadata();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Types.internal_static_prometheus_MetricMetadata_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Types.internal_static_prometheus_MetricMetadata_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Types.MetricMetadata.class, Types.MetricMetadata.Builder.class);
        }

        /** Protobuf enum {@code prometheus.MetricMetadata.MetricType} */
        public enum MetricType implements com.google.protobuf.ProtocolMessageEnum {
            /** <code>UNKNOWN = 0;</code> */
            UNKNOWN(0),
            /** <code>COUNTER = 1;</code> */
            COUNTER(1),
            /** <code>GAUGE = 2;</code> */
            GAUGE(2),
            /** <code>HISTOGRAM = 3;</code> */
            HISTOGRAM(3),
            /** <code>GAUGEHISTOGRAM = 4;</code> */
            GAUGEHISTOGRAM(4),
            /** <code>SUMMARY = 5;</code> */
            SUMMARY(5),
            /** <code>INFO = 6;</code> */
            INFO(6),
            /** <code>STATESET = 7;</code> */
            STATESET(7),
            UNRECOGNIZED(-1),
            ;

            /** <code>UNKNOWN = 0;</code> */
            public static final int UNKNOWN_VALUE = 0;
            /** <code>COUNTER = 1;</code> */
            public static final int COUNTER_VALUE = 1;
            /** <code>GAUGE = 2;</code> */
            public static final int GAUGE_VALUE = 2;
            /** <code>HISTOGRAM = 3;</code> */
            public static final int HISTOGRAM_VALUE = 3;
            /** <code>GAUGEHISTOGRAM = 4;</code> */
            public static final int GAUGEHISTOGRAM_VALUE = 4;
            /** <code>SUMMARY = 5;</code> */
            public static final int SUMMARY_VALUE = 5;
            /** <code>INFO = 6;</code> */
            public static final int INFO_VALUE = 6;
            /** <code>STATESET = 7;</code> */
            public static final int STATESET_VALUE = 7;

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
            public static MetricType valueOf(int value) {
                return forNumber(value);
            }

            /**
             * @param value The numeric wire value of the corresponding enum entry.
             * @return The enum associated with the given numeric wire value.
             */
            public static MetricType forNumber(int value) {
                switch (value) {
                    case 0:
                        return UNKNOWN;
                    case 1:
                        return COUNTER;
                    case 2:
                        return GAUGE;
                    case 3:
                        return HISTOGRAM;
                    case 4:
                        return GAUGEHISTOGRAM;
                    case 5:
                        return SUMMARY;
                    case 6:
                        return INFO;
                    case 7:
                        return STATESET;
                    default:
                        return null;
                }
            }

            public static com.google.protobuf.Internal.EnumLiteMap<MetricType>
                    internalGetValueMap() {
                return internalValueMap;
            }

            private static final com.google.protobuf.Internal.EnumLiteMap<MetricType>
                    internalValueMap =
                            new com.google.protobuf.Internal.EnumLiteMap<MetricType>() {
                                public MetricType findValueByNumber(int number) {
                                    return MetricType.forNumber(number);
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
                return Types.MetricMetadata.getDescriptor().getEnumTypes().get(0);
            }

            private static final MetricType[] VALUES = values();

            public static MetricType valueOf(
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

            private MetricType(int value) {
                this.value = value;
            }

            // @@protoc_insertion_point(enum_scope:prometheus.MetricMetadata.MetricType)
        }

        public static final int TYPE_FIELD_NUMBER = 1;
        private int type_ = 0;

        /**
         *
         *
         * <pre>
         * Represents the metric type, these match the set from Prometheus.
         * Refer to github.com/prometheus/common/model/metadata.go for details.
         * </pre>
         *
         * <code>.prometheus.MetricMetadata.MetricType type = 1;</code>
         *
         * @return The enum numeric value on the wire for type.
         */
        @Override
        public int getTypeValue() {
            return type_;
        }

        /**
         *
         *
         * <pre>
         * Represents the metric type, these match the set from Prometheus.
         * Refer to github.com/prometheus/common/model/metadata.go for details.
         * </pre>
         *
         * <code>.prometheus.MetricMetadata.MetricType type = 1;</code>
         *
         * @return The type.
         */
        @Override
        public Types.MetricMetadata.MetricType getType() {
            Types.MetricMetadata.MetricType result =
                    Types.MetricMetadata.MetricType.forNumber(type_);
            return result == null ? Types.MetricMetadata.MetricType.UNRECOGNIZED : result;
        }

        public static final int METRIC_FAMILY_NAME_FIELD_NUMBER = 2;

        @SuppressWarnings("serial")
        private volatile Object metricFamilyName_ = "";

        /**
         * <code>string metric_family_name = 2;</code>
         *
         * @return The metricFamilyName.
         */
        @Override
        public String getMetricFamilyName() {
            Object ref = metricFamilyName_;
            if (ref instanceof String) {
                return (String) ref;
            } else {
                com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                metricFamilyName_ = s;
                return s;
            }
        }

        /**
         * <code>string metric_family_name = 2;</code>
         *
         * @return The bytes for metricFamilyName.
         */
        @Override
        public com.google.protobuf.ByteString getMetricFamilyNameBytes() {
            Object ref = metricFamilyName_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                metricFamilyName_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        public static final int HELP_FIELD_NUMBER = 4;

        @SuppressWarnings("serial")
        private volatile Object help_ = "";

        /**
         * <code>string help = 4;</code>
         *
         * @return The help.
         */
        @Override
        public String getHelp() {
            Object ref = help_;
            if (ref instanceof String) {
                return (String) ref;
            } else {
                com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                help_ = s;
                return s;
            }
        }

        /**
         * <code>string help = 4;</code>
         *
         * @return The bytes for help.
         */
        @Override
        public com.google.protobuf.ByteString getHelpBytes() {
            Object ref = help_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                help_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        public static final int UNIT_FIELD_NUMBER = 5;

        @SuppressWarnings("serial")
        private volatile Object unit_ = "";

        /**
         * <code>string unit = 5;</code>
         *
         * @return The unit.
         */
        @Override
        public String getUnit() {
            Object ref = unit_;
            if (ref instanceof String) {
                return (String) ref;
            } else {
                com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                unit_ = s;
                return s;
            }
        }

        /**
         * <code>string unit = 5;</code>
         *
         * @return The bytes for unit.
         */
        @Override
        public com.google.protobuf.ByteString getUnitBytes() {
            Object ref = unit_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                unit_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
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
            if (type_ != Types.MetricMetadata.MetricType.UNKNOWN.getNumber()) {
                output.writeEnum(1, type_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(metricFamilyName_)) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 2, metricFamilyName_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(help_)) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 4, help_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(unit_)) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 5, unit_);
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
            if (type_ != Types.MetricMetadata.MetricType.UNKNOWN.getNumber()) {
                size += com.google.protobuf.CodedOutputStream.computeEnumSize(1, type_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(metricFamilyName_)) {
                size +=
                        com.google.protobuf.GeneratedMessageV3.computeStringSize(
                                2, metricFamilyName_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(help_)) {
                size += com.google.protobuf.GeneratedMessageV3.computeStringSize(4, help_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(unit_)) {
                size += com.google.protobuf.GeneratedMessageV3.computeStringSize(5, unit_);
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
            if (!(obj instanceof Types.MetricMetadata)) {
                return super.equals(obj);
            }
            Types.MetricMetadata other = (Types.MetricMetadata) obj;

            if (type_ != other.type_) {
                return false;
            }
            if (!getMetricFamilyName().equals(other.getMetricFamilyName())) {
                return false;
            }
            if (!getHelp().equals(other.getHelp())) {
                return false;
            }
            if (!getUnit().equals(other.getUnit())) {
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
            hash = (37 * hash) + TYPE_FIELD_NUMBER;
            hash = (53 * hash) + type_;
            hash = (37 * hash) + METRIC_FAMILY_NAME_FIELD_NUMBER;
            hash = (53 * hash) + getMetricFamilyName().hashCode();
            hash = (37 * hash) + HELP_FIELD_NUMBER;
            hash = (53 * hash) + getHelp().hashCode();
            hash = (37 * hash) + UNIT_FIELD_NUMBER;
            hash = (53 * hash) + getUnit().hashCode();
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Types.MetricMetadata parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.MetricMetadata parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.MetricMetadata parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.MetricMetadata parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.MetricMetadata parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.MetricMetadata parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.MetricMetadata parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.MetricMetadata parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.MetricMetadata parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Types.MetricMetadata parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.MetricMetadata parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.MetricMetadata parseFrom(
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

        public static Builder newBuilder(Types.MetricMetadata prototype) {
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

        /** Protobuf type {@code prometheus.MetricMetadata} */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.MetricMetadata)
                Types.MetricMetadataOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Types.internal_static_prometheus_MetricMetadata_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Types.internal_static_prometheus_MetricMetadata_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Types.MetricMetadata.class, Types.MetricMetadata.Builder.class);
            }

            // Construct using Types.MetricMetadata.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                type_ = 0;
                metricFamilyName_ = "";
                help_ = "";
                unit_ = "";
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Types.internal_static_prometheus_MetricMetadata_descriptor;
            }

            @Override
            public Types.MetricMetadata getDefaultInstanceForType() {
                return Types.MetricMetadata.getDefaultInstance();
            }

            @Override
            public Types.MetricMetadata build() {
                Types.MetricMetadata result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Types.MetricMetadata buildPartial() {
                Types.MetricMetadata result = new Types.MetricMetadata(this);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartial0(Types.MetricMetadata result) {
                int from_bitField0_ = bitField0_;
                if (((from_bitField0_ & 0x00000001) != 0)) {
                    result.type_ = type_;
                }
                if (((from_bitField0_ & 0x00000002) != 0)) {
                    result.metricFamilyName_ = metricFamilyName_;
                }
                if (((from_bitField0_ & 0x00000004) != 0)) {
                    result.help_ = help_;
                }
                if (((from_bitField0_ & 0x00000008) != 0)) {
                    result.unit_ = unit_;
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
                if (other instanceof Types.MetricMetadata) {
                    return mergeFrom((Types.MetricMetadata) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Types.MetricMetadata other) {
                if (other == Types.MetricMetadata.getDefaultInstance()) {
                    return this;
                }
                if (other.type_ != 0) {
                    setTypeValue(other.getTypeValue());
                }
                if (!other.getMetricFamilyName().isEmpty()) {
                    metricFamilyName_ = other.metricFamilyName_;
                    bitField0_ |= 0x00000002;
                    onChanged();
                }
                if (!other.getHelp().isEmpty()) {
                    help_ = other.help_;
                    bitField0_ |= 0x00000004;
                    onChanged();
                }
                if (!other.getUnit().isEmpty()) {
                    unit_ = other.unit_;
                    bitField0_ |= 0x00000008;
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
                            case 8:
                                {
                                    type_ = input.readEnum();
                                    bitField0_ |= 0x00000001;
                                    break;
                                } // case 8
                            case 18:
                                {
                                    metricFamilyName_ = input.readStringRequireUtf8();
                                    bitField0_ |= 0x00000002;
                                    break;
                                } // case 18
                            case 34:
                                {
                                    help_ = input.readStringRequireUtf8();
                                    bitField0_ |= 0x00000004;
                                    break;
                                } // case 34
                            case 42:
                                {
                                    unit_ = input.readStringRequireUtf8();
                                    bitField0_ |= 0x00000008;
                                    break;
                                } // case 42
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

            private int type_ = 0;

            /**
             *
             *
             * <pre>
             * Represents the metric type, these match the set from Prometheus.
             * Refer to github.com/prometheus/common/model/metadata.go for details.
             * </pre>
             *
             * <code>.prometheus.MetricMetadata.MetricType type = 1;</code>
             *
             * @return The enum numeric value on the wire for type.
             */
            @Override
            public int getTypeValue() {
                return type_;
            }

            /**
             *
             *
             * <pre>
             * Represents the metric type, these match the set from Prometheus.
             * Refer to github.com/prometheus/common/model/metadata.go for details.
             * </pre>
             *
             * <code>.prometheus.MetricMetadata.MetricType type = 1;</code>
             *
             * @param value The enum numeric value on the wire for type to set.
             * @return This builder for chaining.
             */
            public Builder setTypeValue(int value) {
                type_ = value;
                bitField0_ |= 0x00000001;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Represents the metric type, these match the set from Prometheus.
             * Refer to github.com/prometheus/common/model/metadata.go for details.
             * </pre>
             *
             * <code>.prometheus.MetricMetadata.MetricType type = 1;</code>
             *
             * @return The type.
             */
            @Override
            public Types.MetricMetadata.MetricType getType() {
                Types.MetricMetadata.MetricType result =
                        Types.MetricMetadata.MetricType.forNumber(type_);
                return result == null ? Types.MetricMetadata.MetricType.UNRECOGNIZED : result;
            }

            /**
             *
             *
             * <pre>
             * Represents the metric type, these match the set from Prometheus.
             * Refer to github.com/prometheus/common/model/metadata.go for details.
             * </pre>
             *
             * <code>.prometheus.MetricMetadata.MetricType type = 1;</code>
             *
             * @param value The type to set.
             * @return This builder for chaining.
             */
            public Builder setType(Types.MetricMetadata.MetricType value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                bitField0_ |= 0x00000001;
                type_ = value.getNumber();
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Represents the metric type, these match the set from Prometheus.
             * Refer to github.com/prometheus/common/model/metadata.go for details.
             * </pre>
             *
             * <code>.prometheus.MetricMetadata.MetricType type = 1;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearType() {
                bitField0_ = (bitField0_ & ~0x00000001);
                type_ = 0;
                onChanged();
                return this;
            }

            private Object metricFamilyName_ = "";

            /**
             * <code>string metric_family_name = 2;</code>
             *
             * @return The metricFamilyName.
             */
            public String getMetricFamilyName() {
                Object ref = metricFamilyName_;
                if (!(ref instanceof String)) {
                    com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                    String s = bs.toStringUtf8();
                    metricFamilyName_ = s;
                    return s;
                } else {
                    return (String) ref;
                }
            }

            /**
             * <code>string metric_family_name = 2;</code>
             *
             * @return The bytes for metricFamilyName.
             */
            public com.google.protobuf.ByteString getMetricFamilyNameBytes() {
                Object ref = metricFamilyName_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                    metricFamilyName_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>string metric_family_name = 2;</code>
             *
             * @param value The metricFamilyName to set.
             * @return This builder for chaining.
             */
            public Builder setMetricFamilyName(String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                metricFamilyName_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            /**
             * <code>string metric_family_name = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearMetricFamilyName() {
                metricFamilyName_ = getDefaultInstance().getMetricFamilyName();
                bitField0_ = (bitField0_ & ~0x00000002);
                onChanged();
                return this;
            }

            /**
             * <code>string metric_family_name = 2;</code>
             *
             * @param value The bytes for metricFamilyName to set.
             * @return This builder for chaining.
             */
            public Builder setMetricFamilyNameBytes(com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);
                metricFamilyName_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            private Object help_ = "";

            /**
             * <code>string help = 4;</code>
             *
             * @return The help.
             */
            public String getHelp() {
                Object ref = help_;
                if (!(ref instanceof String)) {
                    com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                    String s = bs.toStringUtf8();
                    help_ = s;
                    return s;
                } else {
                    return (String) ref;
                }
            }

            /**
             * <code>string help = 4;</code>
             *
             * @return The bytes for help.
             */
            public com.google.protobuf.ByteString getHelpBytes() {
                Object ref = help_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                    help_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>string help = 4;</code>
             *
             * @param value The help to set.
             * @return This builder for chaining.
             */
            public Builder setHelp(String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                help_ = value;
                bitField0_ |= 0x00000004;
                onChanged();
                return this;
            }

            /**
             * <code>string help = 4;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearHelp() {
                help_ = getDefaultInstance().getHelp();
                bitField0_ = (bitField0_ & ~0x00000004);
                onChanged();
                return this;
            }

            /**
             * <code>string help = 4;</code>
             *
             * @param value The bytes for help to set.
             * @return This builder for chaining.
             */
            public Builder setHelpBytes(com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);
                help_ = value;
                bitField0_ |= 0x00000004;
                onChanged();
                return this;
            }

            private Object unit_ = "";

            /**
             * <code>string unit = 5;</code>
             *
             * @return The unit.
             */
            public String getUnit() {
                Object ref = unit_;
                if (!(ref instanceof String)) {
                    com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                    String s = bs.toStringUtf8();
                    unit_ = s;
                    return s;
                } else {
                    return (String) ref;
                }
            }

            /**
             * <code>string unit = 5;</code>
             *
             * @return The bytes for unit.
             */
            public com.google.protobuf.ByteString getUnitBytes() {
                Object ref = unit_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                    unit_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>string unit = 5;</code>
             *
             * @param value The unit to set.
             * @return This builder for chaining.
             */
            public Builder setUnit(String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                unit_ = value;
                bitField0_ |= 0x00000008;
                onChanged();
                return this;
            }

            /**
             * <code>string unit = 5;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearUnit() {
                unit_ = getDefaultInstance().getUnit();
                bitField0_ = (bitField0_ & ~0x00000008);
                onChanged();
                return this;
            }

            /**
             * <code>string unit = 5;</code>
             *
             * @param value The bytes for unit to set.
             * @return This builder for chaining.
             */
            public Builder setUnitBytes(com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);
                unit_ = value;
                bitField0_ |= 0x00000008;
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

            // @@protoc_insertion_point(builder_scope:prometheus.MetricMetadata)
        }

        // @@protoc_insertion_point(class_scope:prometheus.MetricMetadata)
        private static final Types.MetricMetadata DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Types.MetricMetadata();
        }

        public static Types.MetricMetadata getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<MetricMetadata> PARSER =
                new com.google.protobuf.AbstractParser<MetricMetadata>() {
                    @Override
                    public MetricMetadata parsePartialFrom(
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

        public static com.google.protobuf.Parser<MetricMetadata> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<MetricMetadata> getParserForType() {
            return PARSER;
        }

        @Override
        public Types.MetricMetadata getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface SampleOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.Sample)
            com.google.protobuf.MessageOrBuilder {

        /**
         * <code>double value = 1;</code>
         *
         * @return The value.
         */
        double getValue();

        /**
         *
         *
         * <pre>
         * timestamp is in ms format, see model/timestamp/timestamp.go for
         * conversion from time.Time to Prometheus timestamp.
         * </pre>
         *
         * <code>int64 timestamp = 2;</code>
         *
         * @return The timestamp.
         */
        long getTimestamp();
    }

    /** Protobuf type {@code prometheus.Sample} */
    public static final class Sample extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.Sample)
            SampleOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use Sample.newBuilder() to construct.
        private Sample(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private Sample() {}

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new Sample();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Types.internal_static_prometheus_Sample_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Types.internal_static_prometheus_Sample_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Types.Sample.class, Types.Sample.Builder.class);
        }

        public static final int VALUE_FIELD_NUMBER = 1;
        private double value_ = 0D;

        /**
         * <code>double value = 1;</code>
         *
         * @return The value.
         */
        @Override
        public double getValue() {
            return value_;
        }

        public static final int TIMESTAMP_FIELD_NUMBER = 2;
        private long timestamp_ = 0L;

        /**
         *
         *
         * <pre>
         * timestamp is in ms format, see model/timestamp/timestamp.go for
         * conversion from time.Time to Prometheus timestamp.
         * </pre>
         *
         * <code>int64 timestamp = 2;</code>
         *
         * @return The timestamp.
         */
        @Override
        public long getTimestamp() {
            return timestamp_;
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
            if (Double.doubleToRawLongBits(value_) != 0) {
                output.writeDouble(1, value_);
            }
            if (timestamp_ != 0L) {
                output.writeInt64(2, timestamp_);
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
            if (Double.doubleToRawLongBits(value_) != 0) {
                size += com.google.protobuf.CodedOutputStream.computeDoubleSize(1, value_);
            }
            if (timestamp_ != 0L) {
                size += com.google.protobuf.CodedOutputStream.computeInt64Size(2, timestamp_);
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
            if (!(obj instanceof Types.Sample)) {
                return super.equals(obj);
            }
            Types.Sample other = (Types.Sample) obj;

            if (Double.doubleToLongBits(getValue()) != Double.doubleToLongBits(other.getValue())) {
                return false;
            }
            if (getTimestamp() != other.getTimestamp()) {
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
            hash = (37 * hash) + VALUE_FIELD_NUMBER;
            hash =
                    (53 * hash)
                            + com.google.protobuf.Internal.hashLong(
                                    Double.doubleToLongBits(getValue()));
            hash = (37 * hash) + TIMESTAMP_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getTimestamp());
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Types.Sample parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Sample parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Sample parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Sample parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Sample parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Sample parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Sample parseFrom(java.io.InputStream input) throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.Sample parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.Sample parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Types.Sample parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.Sample parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.Sample parseFrom(
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

        public static Builder newBuilder(Types.Sample prototype) {
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

        /** Protobuf type {@code prometheus.Sample} */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.Sample)
                Types.SampleOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Types.internal_static_prometheus_Sample_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Types.internal_static_prometheus_Sample_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Types.Sample.class, Types.Sample.Builder.class);
            }

            // Construct using Types.Sample.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                value_ = 0D;
                timestamp_ = 0L;
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Types.internal_static_prometheus_Sample_descriptor;
            }

            @Override
            public Types.Sample getDefaultInstanceForType() {
                return Types.Sample.getDefaultInstance();
            }

            @Override
            public Types.Sample build() {
                Types.Sample result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Types.Sample buildPartial() {
                Types.Sample result = new Types.Sample(this);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartial0(Types.Sample result) {
                int from_bitField0_ = bitField0_;
                if (((from_bitField0_ & 0x00000001) != 0)) {
                    result.value_ = value_;
                }
                if (((from_bitField0_ & 0x00000002) != 0)) {
                    result.timestamp_ = timestamp_;
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
                if (other instanceof Types.Sample) {
                    return mergeFrom((Types.Sample) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Types.Sample other) {
                if (other == Types.Sample.getDefaultInstance()) {
                    return this;
                }
                if (other.getValue() != 0D) {
                    setValue(other.getValue());
                }
                if (other.getTimestamp() != 0L) {
                    setTimestamp(other.getTimestamp());
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
                            case 9:
                                {
                                    value_ = input.readDouble();
                                    bitField0_ |= 0x00000001;
                                    break;
                                } // case 9
                            case 16:
                                {
                                    timestamp_ = input.readInt64();
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

            private double value_;

            /**
             * <code>double value = 1;</code>
             *
             * @return The value.
             */
            @Override
            public double getValue() {
                return value_;
            }

            /**
             * <code>double value = 1;</code>
             *
             * @param value The value to set.
             * @return This builder for chaining.
             */
            public Builder setValue(double value) {

                value_ = value;
                bitField0_ |= 0x00000001;
                onChanged();
                return this;
            }

            /**
             * <code>double value = 1;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearValue() {
                bitField0_ = (bitField0_ & ~0x00000001);
                value_ = 0D;
                onChanged();
                return this;
            }

            private long timestamp_;

            /**
             *
             *
             * <pre>
             * timestamp is in ms format, see model/timestamp/timestamp.go for
             * conversion from time.Time to Prometheus timestamp.
             * </pre>
             *
             * <code>int64 timestamp = 2;</code>
             *
             * @return The timestamp.
             */
            @Override
            public long getTimestamp() {
                return timestamp_;
            }

            /**
             *
             *
             * <pre>
             * timestamp is in ms format, see model/timestamp/timestamp.go for
             * conversion from time.Time to Prometheus timestamp.
             * </pre>
             *
             * <code>int64 timestamp = 2;</code>
             *
             * @param value The timestamp to set.
             * @return This builder for chaining.
             */
            public Builder setTimestamp(long value) {

                timestamp_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * timestamp is in ms format, see model/timestamp/timestamp.go for
             * conversion from time.Time to Prometheus timestamp.
             * </pre>
             *
             * <code>int64 timestamp = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearTimestamp() {
                bitField0_ = (bitField0_ & ~0x00000002);
                timestamp_ = 0L;
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

            // @@protoc_insertion_point(builder_scope:prometheus.Sample)
        }

        // @@protoc_insertion_point(class_scope:prometheus.Sample)
        private static final Types.Sample DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Types.Sample();
        }

        public static Types.Sample getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<Sample> PARSER =
                new com.google.protobuf.AbstractParser<Sample>() {
                    @Override
                    public Sample parsePartialFrom(
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

        public static com.google.protobuf.Parser<Sample> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<Sample> getParserForType() {
            return PARSER;
        }

        @Override
        public Types.Sample getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface ExemplarOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.Exemplar)
            com.google.protobuf.MessageOrBuilder {

        /**
         *
         *
         * <pre>
         * Optional, can be empty.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        java.util.List<Types.Label> getLabelsList();

        /**
         *
         *
         * <pre>
         * Optional, can be empty.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        Types.Label getLabels(int index);

        /**
         *
         *
         * <pre>
         * Optional, can be empty.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        int getLabelsCount();

        /**
         *
         *
         * <pre>
         * Optional, can be empty.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        java.util.List<? extends Types.LabelOrBuilder> getLabelsOrBuilderList();

        /**
         *
         *
         * <pre>
         * Optional, can be empty.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        Types.LabelOrBuilder getLabelsOrBuilder(int index);

        /**
         * <code>double value = 2;</code>
         *
         * @return The value.
         */
        double getValue();

        /**
         *
         *
         * <pre>
         * timestamp is in ms format, see model/timestamp/timestamp.go for
         * conversion from time.Time to Prometheus timestamp.
         * </pre>
         *
         * <code>int64 timestamp = 3;</code>
         *
         * @return The timestamp.
         */
        long getTimestamp();
    }

    /** Protobuf type {@code prometheus.Exemplar} */
    public static final class Exemplar extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.Exemplar)
            ExemplarOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use Exemplar.newBuilder() to construct.
        private Exemplar(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private Exemplar() {
            labels_ = java.util.Collections.emptyList();
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new Exemplar();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Types.internal_static_prometheus_Exemplar_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Types.internal_static_prometheus_Exemplar_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Types.Exemplar.class, Types.Exemplar.Builder.class);
        }

        public static final int LABELS_FIELD_NUMBER = 1;

        @SuppressWarnings("serial")
        private java.util.List<Types.Label> labels_;

        /**
         *
         *
         * <pre>
         * Optional, can be empty.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public java.util.List<Types.Label> getLabelsList() {
            return labels_;
        }

        /**
         *
         *
         * <pre>
         * Optional, can be empty.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public java.util.List<? extends Types.LabelOrBuilder> getLabelsOrBuilderList() {
            return labels_;
        }

        /**
         *
         *
         * <pre>
         * Optional, can be empty.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public int getLabelsCount() {
            return labels_.size();
        }

        /**
         *
         *
         * <pre>
         * Optional, can be empty.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public Types.Label getLabels(int index) {
            return labels_.get(index);
        }

        /**
         *
         *
         * <pre>
         * Optional, can be empty.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public Types.LabelOrBuilder getLabelsOrBuilder(int index) {
            return labels_.get(index);
        }

        public static final int VALUE_FIELD_NUMBER = 2;
        private double value_ = 0D;

        /**
         * <code>double value = 2;</code>
         *
         * @return The value.
         */
        @Override
        public double getValue() {
            return value_;
        }

        public static final int TIMESTAMP_FIELD_NUMBER = 3;
        private long timestamp_ = 0L;

        /**
         *
         *
         * <pre>
         * timestamp is in ms format, see model/timestamp/timestamp.go for
         * conversion from time.Time to Prometheus timestamp.
         * </pre>
         *
         * <code>int64 timestamp = 3;</code>
         *
         * @return The timestamp.
         */
        @Override
        public long getTimestamp() {
            return timestamp_;
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
            for (int i = 0; i < labels_.size(); i++) {
                output.writeMessage(1, labels_.get(i));
            }
            if (Double.doubleToRawLongBits(value_) != 0) {
                output.writeDouble(2, value_);
            }
            if (timestamp_ != 0L) {
                output.writeInt64(3, timestamp_);
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
            for (int i = 0; i < labels_.size(); i++) {
                size += com.google.protobuf.CodedOutputStream.computeMessageSize(1, labels_.get(i));
            }
            if (Double.doubleToRawLongBits(value_) != 0) {
                size += com.google.protobuf.CodedOutputStream.computeDoubleSize(2, value_);
            }
            if (timestamp_ != 0L) {
                size += com.google.protobuf.CodedOutputStream.computeInt64Size(3, timestamp_);
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
            if (!(obj instanceof Types.Exemplar)) {
                return super.equals(obj);
            }
            Types.Exemplar other = (Types.Exemplar) obj;

            if (!getLabelsList().equals(other.getLabelsList())) {
                return false;
            }
            if (Double.doubleToLongBits(getValue()) != Double.doubleToLongBits(other.getValue())) {
                return false;
            }
            if (getTimestamp() != other.getTimestamp()) {
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
            if (getLabelsCount() > 0) {
                hash = (37 * hash) + LABELS_FIELD_NUMBER;
                hash = (53 * hash) + getLabelsList().hashCode();
            }
            hash = (37 * hash) + VALUE_FIELD_NUMBER;
            hash =
                    (53 * hash)
                            + com.google.protobuf.Internal.hashLong(
                                    Double.doubleToLongBits(getValue()));
            hash = (37 * hash) + TIMESTAMP_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getTimestamp());
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Types.Exemplar parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Exemplar parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Exemplar parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Exemplar parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Exemplar parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Exemplar parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Exemplar parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.Exemplar parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.Exemplar parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Types.Exemplar parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.Exemplar parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.Exemplar parseFrom(
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

        public static Builder newBuilder(Types.Exemplar prototype) {
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

        /** Protobuf type {@code prometheus.Exemplar} */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.Exemplar)
                Types.ExemplarOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Types.internal_static_prometheus_Exemplar_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Types.internal_static_prometheus_Exemplar_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Types.Exemplar.class, Types.Exemplar.Builder.class);
            }

            // Construct using Types.Exemplar.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                if (labelsBuilder_ == null) {
                    labels_ = java.util.Collections.emptyList();
                } else {
                    labels_ = null;
                    labelsBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000001);
                value_ = 0D;
                timestamp_ = 0L;
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Types.internal_static_prometheus_Exemplar_descriptor;
            }

            @Override
            public Types.Exemplar getDefaultInstanceForType() {
                return Types.Exemplar.getDefaultInstance();
            }

            @Override
            public Types.Exemplar build() {
                Types.Exemplar result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Types.Exemplar buildPartial() {
                Types.Exemplar result = new Types.Exemplar(this);
                buildPartialRepeatedFields(result);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartialRepeatedFields(Types.Exemplar result) {
                if (labelsBuilder_ == null) {
                    if (((bitField0_ & 0x00000001) != 0)) {
                        labels_ = java.util.Collections.unmodifiableList(labels_);
                        bitField0_ = (bitField0_ & ~0x00000001);
                    }
                    result.labels_ = labels_;
                } else {
                    result.labels_ = labelsBuilder_.build();
                }
            }

            private void buildPartial0(Types.Exemplar result) {
                int from_bitField0_ = bitField0_;
                if (((from_bitField0_ & 0x00000002) != 0)) {
                    result.value_ = value_;
                }
                if (((from_bitField0_ & 0x00000004) != 0)) {
                    result.timestamp_ = timestamp_;
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
                if (other instanceof Types.Exemplar) {
                    return mergeFrom((Types.Exemplar) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Types.Exemplar other) {
                if (other == Types.Exemplar.getDefaultInstance()) {
                    return this;
                }
                if (labelsBuilder_ == null) {
                    if (!other.labels_.isEmpty()) {
                        if (labels_.isEmpty()) {
                            labels_ = other.labels_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                        } else {
                            ensureLabelsIsMutable();
                            labels_.addAll(other.labels_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.labels_.isEmpty()) {
                        if (labelsBuilder_.isEmpty()) {
                            labelsBuilder_.dispose();
                            labelsBuilder_ = null;
                            labels_ = other.labels_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                            labelsBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getLabelsFieldBuilder()
                                            : null;
                        } else {
                            labelsBuilder_.addAllMessages(other.labels_);
                        }
                    }
                }
                if (other.getValue() != 0D) {
                    setValue(other.getValue());
                }
                if (other.getTimestamp() != 0L) {
                    setTimestamp(other.getTimestamp());
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
                                    Types.Label m =
                                            input.readMessage(
                                                    Types.Label.parser(), extensionRegistry);
                                    if (labelsBuilder_ == null) {
                                        ensureLabelsIsMutable();
                                        labels_.add(m);
                                    } else {
                                        labelsBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 10
                            case 17:
                                {
                                    value_ = input.readDouble();
                                    bitField0_ |= 0x00000002;
                                    break;
                                } // case 17
                            case 24:
                                {
                                    timestamp_ = input.readInt64();
                                    bitField0_ |= 0x00000004;
                                    break;
                                } // case 24
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

            private java.util.List<Types.Label> labels_ = java.util.Collections.emptyList();

            private void ensureLabelsIsMutable() {
                if (!((bitField0_ & 0x00000001) != 0)) {
                    labels_ = new java.util.ArrayList<Types.Label>(labels_);
                    bitField0_ |= 0x00000001;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Label, Types.Label.Builder, Types.LabelOrBuilder>
                    labelsBuilder_;

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<Types.Label> getLabelsList() {
                if (labelsBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(labels_);
                } else {
                    return labelsBuilder_.getMessageList();
                }
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public int getLabelsCount() {
                if (labelsBuilder_ == null) {
                    return labels_.size();
                } else {
                    return labelsBuilder_.getCount();
                }
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label getLabels(int index) {
                if (labelsBuilder_ == null) {
                    return labels_.get(index);
                } else {
                    return labelsBuilder_.getMessage(index);
                }
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder setLabels(int index, Types.Label value) {
                if (labelsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureLabelsIsMutable();
                    labels_.set(index, value);
                    onChanged();
                } else {
                    labelsBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder setLabels(int index, Types.Label.Builder builderForValue) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    labelsBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(Types.Label value) {
                if (labelsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureLabelsIsMutable();
                    labels_.add(value);
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(int index, Types.Label value) {
                if (labelsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureLabelsIsMutable();
                    labels_.add(index, value);
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(Types.Label.Builder builderForValue) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.add(builderForValue.build());
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(int index, Types.Label.Builder builderForValue) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addAllLabels(Iterable<? extends Types.Label> values) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, labels_);
                    onChanged();
                } else {
                    labelsBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder clearLabels() {
                if (labelsBuilder_ == null) {
                    labels_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000001);
                    onChanged();
                } else {
                    labelsBuilder_.clear();
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder removeLabels(int index) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.remove(index);
                    onChanged();
                } else {
                    labelsBuilder_.remove(index);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label.Builder getLabelsBuilder(int index) {
                return getLabelsFieldBuilder().getBuilder(index);
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.LabelOrBuilder getLabelsOrBuilder(int index) {
                if (labelsBuilder_ == null) {
                    return labels_.get(index);
                } else {
                    return labelsBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<? extends Types.LabelOrBuilder> getLabelsOrBuilderList() {
                if (labelsBuilder_ != null) {
                    return labelsBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(labels_);
                }
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label.Builder addLabelsBuilder() {
                return getLabelsFieldBuilder().addBuilder(Types.Label.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label.Builder addLabelsBuilder(int index) {
                return getLabelsFieldBuilder().addBuilder(index, Types.Label.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * Optional, can be empty.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<Types.Label.Builder> getLabelsBuilderList() {
                return getLabelsFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Label, Types.Label.Builder, Types.LabelOrBuilder>
                    getLabelsFieldBuilder() {
                if (labelsBuilder_ == null) {
                    labelsBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.Label, Types.Label.Builder, Types.LabelOrBuilder>(
                                    labels_,
                                    ((bitField0_ & 0x00000001) != 0),
                                    getParentForChildren(),
                                    isClean());
                    labels_ = null;
                }
                return labelsBuilder_;
            }

            private double value_;

            /**
             * <code>double value = 2;</code>
             *
             * @return The value.
             */
            @Override
            public double getValue() {
                return value_;
            }

            /**
             * <code>double value = 2;</code>
             *
             * @param value The value to set.
             * @return This builder for chaining.
             */
            public Builder setValue(double value) {

                value_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            /**
             * <code>double value = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearValue() {
                bitField0_ = (bitField0_ & ~0x00000002);
                value_ = 0D;
                onChanged();
                return this;
            }

            private long timestamp_;

            /**
             *
             *
             * <pre>
             * timestamp is in ms format, see model/timestamp/timestamp.go for
             * conversion from time.Time to Prometheus timestamp.
             * </pre>
             *
             * <code>int64 timestamp = 3;</code>
             *
             * @return The timestamp.
             */
            @Override
            public long getTimestamp() {
                return timestamp_;
            }

            /**
             *
             *
             * <pre>
             * timestamp is in ms format, see model/timestamp/timestamp.go for
             * conversion from time.Time to Prometheus timestamp.
             * </pre>
             *
             * <code>int64 timestamp = 3;</code>
             *
             * @param value The timestamp to set.
             * @return This builder for chaining.
             */
            public Builder setTimestamp(long value) {

                timestamp_ = value;
                bitField0_ |= 0x00000004;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * timestamp is in ms format, see model/timestamp/timestamp.go for
             * conversion from time.Time to Prometheus timestamp.
             * </pre>
             *
             * <code>int64 timestamp = 3;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearTimestamp() {
                bitField0_ = (bitField0_ & ~0x00000004);
                timestamp_ = 0L;
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

            // @@protoc_insertion_point(builder_scope:prometheus.Exemplar)
        }

        // @@protoc_insertion_point(class_scope:prometheus.Exemplar)
        private static final Types.Exemplar DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Types.Exemplar();
        }

        public static Types.Exemplar getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<Exemplar> PARSER =
                new com.google.protobuf.AbstractParser<Exemplar>() {
                    @Override
                    public Exemplar parsePartialFrom(
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

        public static com.google.protobuf.Parser<Exemplar> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<Exemplar> getParserForType() {
            return PARSER;
        }

        @Override
        public Types.Exemplar getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface HistogramOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.Histogram)
            com.google.protobuf.MessageOrBuilder {

        /**
         * <code>uint64 count_int = 1;</code>
         *
         * @return Whether the countInt field is set.
         */
        boolean hasCountInt();

        /**
         * <code>uint64 count_int = 1;</code>
         *
         * @return The countInt.
         */
        long getCountInt();

        /**
         * <code>double count_float = 2;</code>
         *
         * @return Whether the countFloat field is set.
         */
        boolean hasCountFloat();

        /**
         * <code>double count_float = 2;</code>
         *
         * @return The countFloat.
         */
        double getCountFloat();

        /**
         *
         *
         * <pre>
         * Sum of observations in the histogram.
         * </pre>
         *
         * <code>double sum = 3;</code>
         *
         * @return The sum.
         */
        double getSum();

        /**
         *
         *
         * <pre>
         * The schema defines the bucket schema. Currently, valid numbers
         * are -4 &lt;= n &lt;= 8. They are all for base-2 bucket schemas, where 1
         * is a bucket boundary in each case, and then each power of two is
         * divided into 2^n logarithmic buckets. Or in other words, each
         * bucket boundary is the previous boundary times 2^(2^-n). In the
         * future, more bucket schemas may be added using numbers &lt; -4 or &gt;
         * 8.
         * </pre>
         *
         * <code>sint32 schema = 4;</code>
         *
         * @return The schema.
         */
        int getSchema();

        /**
         *
         *
         * <pre>
         * Breadth of the zero bucket.
         * </pre>
         *
         * <code>double zero_threshold = 5;</code>
         *
         * @return The zeroThreshold.
         */
        double getZeroThreshold();

        /**
         * <code>uint64 zero_count_int = 6;</code>
         *
         * @return Whether the zeroCountInt field is set.
         */
        boolean hasZeroCountInt();

        /**
         * <code>uint64 zero_count_int = 6;</code>
         *
         * @return The zeroCountInt.
         */
        long getZeroCountInt();

        /**
         * <code>double zero_count_float = 7;</code>
         *
         * @return Whether the zeroCountFloat field is set.
         */
        boolean hasZeroCountFloat();

        /**
         * <code>double zero_count_float = 7;</code>
         *
         * @return The zeroCountFloat.
         */
        double getZeroCountFloat();

        /**
         *
         *
         * <pre>
         * Negative Buckets.
         * </pre>
         *
         * <code>repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
         * </code>
         */
        java.util.List<Types.BucketSpan> getNegativeSpansList();

        /**
         *
         *
         * <pre>
         * Negative Buckets.
         * </pre>
         *
         * <code>repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
         * </code>
         */
        Types.BucketSpan getNegativeSpans(int index);

        /**
         *
         *
         * <pre>
         * Negative Buckets.
         * </pre>
         *
         * <code>repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
         * </code>
         */
        int getNegativeSpansCount();

        /**
         *
         *
         * <pre>
         * Negative Buckets.
         * </pre>
         *
         * <code>repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
         * </code>
         */
        java.util.List<? extends Types.BucketSpanOrBuilder> getNegativeSpansOrBuilderList();

        /**
         *
         *
         * <pre>
         * Negative Buckets.
         * </pre>
         *
         * <code>repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
         * </code>
         */
        Types.BucketSpanOrBuilder getNegativeSpansOrBuilder(int index);

        /**
         *
         *
         * <pre>
         * Use either "negative_deltas" or "negative_counts", the former for
         * regular histograms with integer counts, the latter for float
         * histograms.
         * </pre>
         *
         * <code>repeated sint64 negative_deltas = 9;</code>
         *
         * @return A list containing the negativeDeltas.
         */
        java.util.List<Long> getNegativeDeltasList();

        /**
         *
         *
         * <pre>
         * Use either "negative_deltas" or "negative_counts", the former for
         * regular histograms with integer counts, the latter for float
         * histograms.
         * </pre>
         *
         * <code>repeated sint64 negative_deltas = 9;</code>
         *
         * @return The count of negativeDeltas.
         */
        int getNegativeDeltasCount();

        /**
         *
         *
         * <pre>
         * Use either "negative_deltas" or "negative_counts", the former for
         * regular histograms with integer counts, the latter for float
         * histograms.
         * </pre>
         *
         * <code>repeated sint64 negative_deltas = 9;</code>
         *
         * @param index The index of the element to return.
         * @return The negativeDeltas at the given index.
         */
        long getNegativeDeltas(int index);

        /**
         *
         *
         * <pre>
         * Absolute count of each bucket.
         * </pre>
         *
         * <code>repeated double negative_counts = 10;</code>
         *
         * @return A list containing the negativeCounts.
         */
        java.util.List<Double> getNegativeCountsList();

        /**
         *
         *
         * <pre>
         * Absolute count of each bucket.
         * </pre>
         *
         * <code>repeated double negative_counts = 10;</code>
         *
         * @return The count of negativeCounts.
         */
        int getNegativeCountsCount();

        /**
         *
         *
         * <pre>
         * Absolute count of each bucket.
         * </pre>
         *
         * <code>repeated double negative_counts = 10;</code>
         *
         * @param index The index of the element to return.
         * @return The negativeCounts at the given index.
         */
        double getNegativeCounts(int index);

        /**
         *
         *
         * <pre>
         * Positive Buckets.
         * </pre>
         *
         * <code>
         * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
         * </code>
         */
        java.util.List<Types.BucketSpan> getPositiveSpansList();

        /**
         *
         *
         * <pre>
         * Positive Buckets.
         * </pre>
         *
         * <code>
         * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
         * </code>
         */
        Types.BucketSpan getPositiveSpans(int index);

        /**
         *
         *
         * <pre>
         * Positive Buckets.
         * </pre>
         *
         * <code>
         * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
         * </code>
         */
        int getPositiveSpansCount();

        /**
         *
         *
         * <pre>
         * Positive Buckets.
         * </pre>
         *
         * <code>
         * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
         * </code>
         */
        java.util.List<? extends Types.BucketSpanOrBuilder> getPositiveSpansOrBuilderList();

        /**
         *
         *
         * <pre>
         * Positive Buckets.
         * </pre>
         *
         * <code>
         * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
         * </code>
         */
        Types.BucketSpanOrBuilder getPositiveSpansOrBuilder(int index);

        /**
         *
         *
         * <pre>
         * Use either "positive_deltas" or "positive_counts", the former for
         * regular histograms with integer counts, the latter for float
         * histograms.
         * </pre>
         *
         * <code>repeated sint64 positive_deltas = 12;</code>
         *
         * @return A list containing the positiveDeltas.
         */
        java.util.List<Long> getPositiveDeltasList();

        /**
         *
         *
         * <pre>
         * Use either "positive_deltas" or "positive_counts", the former for
         * regular histograms with integer counts, the latter for float
         * histograms.
         * </pre>
         *
         * <code>repeated sint64 positive_deltas = 12;</code>
         *
         * @return The count of positiveDeltas.
         */
        int getPositiveDeltasCount();

        /**
         *
         *
         * <pre>
         * Use either "positive_deltas" or "positive_counts", the former for
         * regular histograms with integer counts, the latter for float
         * histograms.
         * </pre>
         *
         * <code>repeated sint64 positive_deltas = 12;</code>
         *
         * @param index The index of the element to return.
         * @return The positiveDeltas at the given index.
         */
        long getPositiveDeltas(int index);

        /**
         *
         *
         * <pre>
         * Absolute count of each bucket.
         * </pre>
         *
         * <code>repeated double positive_counts = 13;</code>
         *
         * @return A list containing the positiveCounts.
         */
        java.util.List<Double> getPositiveCountsList();

        /**
         *
         *
         * <pre>
         * Absolute count of each bucket.
         * </pre>
         *
         * <code>repeated double positive_counts = 13;</code>
         *
         * @return The count of positiveCounts.
         */
        int getPositiveCountsCount();

        /**
         *
         *
         * <pre>
         * Absolute count of each bucket.
         * </pre>
         *
         * <code>repeated double positive_counts = 13;</code>
         *
         * @param index The index of the element to return.
         * @return The positiveCounts at the given index.
         */
        double getPositiveCounts(int index);

        /**
         * <code>.prometheus.Histogram.ResetHint reset_hint = 14;</code>
         *
         * @return The enum numeric value on the wire for resetHint.
         */
        int getResetHintValue();

        /**
         * <code>.prometheus.Histogram.ResetHint reset_hint = 14;</code>
         *
         * @return The resetHint.
         */
        Types.Histogram.ResetHint getResetHint();

        /**
         *
         *
         * <pre>
         * timestamp is in ms format, see model/timestamp/timestamp.go for
         * conversion from time.Time to Prometheus timestamp.
         * </pre>
         *
         * <code>int64 timestamp = 15;</code>
         *
         * @return The timestamp.
         */
        long getTimestamp();

        Types.Histogram.CountCase getCountCase();

        Types.Histogram.ZeroCountCase getZeroCountCase();
    }

    /**
     *
     *
     * <pre>
     * A native histogram, also known as a sparse histogram.
     * Original design doc:
     * https://docs.google.com/document/d/1cLNv3aufPZb3fNfaJgdaRBZsInZKKIHo9E6HinJVbpM/edit
     * The appendix of this design doc also explains the concept of float
     * histograms. This Histogram message can represent both, the usual
     * integer histogram as well as a float histogram.
     * </pre>
     *
     * <p>Protobuf type {@code prometheus.Histogram}
     */
    public static final class Histogram extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.Histogram)
            HistogramOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use Histogram.newBuilder() to construct.
        private Histogram(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private Histogram() {
            negativeSpans_ = java.util.Collections.emptyList();
            negativeDeltas_ = emptyLongList();
            negativeCounts_ = emptyDoubleList();
            positiveSpans_ = java.util.Collections.emptyList();
            positiveDeltas_ = emptyLongList();
            positiveCounts_ = emptyDoubleList();
            resetHint_ = 0;
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new Histogram();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Types.internal_static_prometheus_Histogram_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Types.internal_static_prometheus_Histogram_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Types.Histogram.class, Types.Histogram.Builder.class);
        }

        /** Protobuf enum {@code prometheus.Histogram.ResetHint} */
        public enum ResetHint implements com.google.protobuf.ProtocolMessageEnum {
            /**
             *
             *
             * <pre>
             * Need to test for a counter reset explicitly.
             * </pre>
             *
             * <code>UNKNOWN = 0;</code>
             */
            UNKNOWN(0),
            /**
             *
             *
             * <pre>
             * This is the 1st histogram after a counter reset.
             * </pre>
             *
             * <code>YES = 1;</code>
             */
            YES(1),
            /**
             *
             *
             * <pre>
             * There was no counter reset between this and the previous Histogram.
             * </pre>
             *
             * <code>NO = 2;</code>
             */
            NO(2),
            /**
             *
             *
             * <pre>
             * This is a gauge histogram where counter resets don't happen.
             * </pre>
             *
             * <code>GAUGE = 3;</code>
             */
            GAUGE(3),
            UNRECOGNIZED(-1),
            ;

            /**
             *
             *
             * <pre>
             * Need to test for a counter reset explicitly.
             * </pre>
             *
             * <code>UNKNOWN = 0;</code>
             */
            public static final int UNKNOWN_VALUE = 0;
            /**
             *
             *
             * <pre>
             * This is the 1st histogram after a counter reset.
             * </pre>
             *
             * <code>YES = 1;</code>
             */
            public static final int YES_VALUE = 1;
            /**
             *
             *
             * <pre>
             * There was no counter reset between this and the previous Histogram.
             * </pre>
             *
             * <code>NO = 2;</code>
             */
            public static final int NO_VALUE = 2;
            /**
             *
             *
             * <pre>
             * This is a gauge histogram where counter resets don't happen.
             * </pre>
             *
             * <code>GAUGE = 3;</code>
             */
            public static final int GAUGE_VALUE = 3;

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
            public static ResetHint valueOf(int value) {
                return forNumber(value);
            }

            /**
             * @param value The numeric wire value of the corresponding enum entry.
             * @return The enum associated with the given numeric wire value.
             */
            public static ResetHint forNumber(int value) {
                switch (value) {
                    case 0:
                        return UNKNOWN;
                    case 1:
                        return YES;
                    case 2:
                        return NO;
                    case 3:
                        return GAUGE;
                    default:
                        return null;
                }
            }

            public static com.google.protobuf.Internal.EnumLiteMap<ResetHint>
                    internalGetValueMap() {
                return internalValueMap;
            }

            private static final com.google.protobuf.Internal.EnumLiteMap<ResetHint>
                    internalValueMap =
                            new com.google.protobuf.Internal.EnumLiteMap<ResetHint>() {
                                public ResetHint findValueByNumber(int number) {
                                    return ResetHint.forNumber(number);
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
                return Types.Histogram.getDescriptor().getEnumTypes().get(0);
            }

            private static final ResetHint[] VALUES = values();

            public static ResetHint valueOf(
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

            private ResetHint(int value) {
                this.value = value;
            }

            // @@protoc_insertion_point(enum_scope:prometheus.Histogram.ResetHint)
        }

        private int countCase_ = 0;

        @SuppressWarnings("serial")
        private Object count_;

        public enum CountCase implements com.google.protobuf.Internal.EnumLite, InternalOneOfEnum {
            COUNT_INT(1),
            COUNT_FLOAT(2),
            COUNT_NOT_SET(0);
            private final int value;

            private CountCase(int value) {
                this.value = value;
            }

            /**
             * @param value The number of the enum to look for.
             * @return The enum associated with the given number.
             * @deprecated Use {@link #forNumber(int)} instead.
             */
            @Deprecated
            public static CountCase valueOf(int value) {
                return forNumber(value);
            }

            public static CountCase forNumber(int value) {
                switch (value) {
                    case 1:
                        return COUNT_INT;
                    case 2:
                        return COUNT_FLOAT;
                    case 0:
                        return COUNT_NOT_SET;
                    default:
                        return null;
                }
            }

            public int getNumber() {
                return this.value;
            }
        };

        public CountCase getCountCase() {
            return CountCase.forNumber(countCase_);
        }

        private int zeroCountCase_ = 0;

        @SuppressWarnings("serial")
        private Object zeroCount_;

        public enum ZeroCountCase
                implements com.google.protobuf.Internal.EnumLite, InternalOneOfEnum {
            ZERO_COUNT_INT(6),
            ZERO_COUNT_FLOAT(7),
            ZEROCOUNT_NOT_SET(0);
            private final int value;

            private ZeroCountCase(int value) {
                this.value = value;
            }

            /**
             * @param value The number of the enum to look for.
             * @return The enum associated with the given number.
             * @deprecated Use {@link #forNumber(int)} instead.
             */
            @Deprecated
            public static ZeroCountCase valueOf(int value) {
                return forNumber(value);
            }

            public static ZeroCountCase forNumber(int value) {
                switch (value) {
                    case 6:
                        return ZERO_COUNT_INT;
                    case 7:
                        return ZERO_COUNT_FLOAT;
                    case 0:
                        return ZEROCOUNT_NOT_SET;
                    default:
                        return null;
                }
            }

            public int getNumber() {
                return this.value;
            }
        };

        public ZeroCountCase getZeroCountCase() {
            return ZeroCountCase.forNumber(zeroCountCase_);
        }

        public static final int COUNT_INT_FIELD_NUMBER = 1;

        /**
         * <code>uint64 count_int = 1;</code>
         *
         * @return Whether the countInt field is set.
         */
        @Override
        public boolean hasCountInt() {
            return countCase_ == 1;
        }

        /**
         * <code>uint64 count_int = 1;</code>
         *
         * @return The countInt.
         */
        @Override
        public long getCountInt() {
            if (countCase_ == 1) {
                return (Long) count_;
            }
            return 0L;
        }

        public static final int COUNT_FLOAT_FIELD_NUMBER = 2;

        /**
         * <code>double count_float = 2;</code>
         *
         * @return Whether the countFloat field is set.
         */
        @Override
        public boolean hasCountFloat() {
            return countCase_ == 2;
        }

        /**
         * <code>double count_float = 2;</code>
         *
         * @return The countFloat.
         */
        @Override
        public double getCountFloat() {
            if (countCase_ == 2) {
                return (Double) count_;
            }
            return 0D;
        }

        public static final int SUM_FIELD_NUMBER = 3;
        private double sum_ = 0D;

        /**
         *
         *
         * <pre>
         * Sum of observations in the histogram.
         * </pre>
         *
         * <code>double sum = 3;</code>
         *
         * @return The sum.
         */
        @Override
        public double getSum() {
            return sum_;
        }

        public static final int SCHEMA_FIELD_NUMBER = 4;
        private int schema_ = 0;

        /**
         *
         *
         * <pre>
         * The schema defines the bucket schema. Currently, valid numbers
         * are -4 &lt;= n &lt;= 8. They are all for base-2 bucket schemas, where 1
         * is a bucket boundary in each case, and then each power of two is
         * divided into 2^n logarithmic buckets. Or in other words, each
         * bucket boundary is the previous boundary times 2^(2^-n). In the
         * future, more bucket schemas may be added using numbers &lt; -4 or &gt;
         * 8.
         * </pre>
         *
         * <code>sint32 schema = 4;</code>
         *
         * @return The schema.
         */
        @Override
        public int getSchema() {
            return schema_;
        }

        public static final int ZERO_THRESHOLD_FIELD_NUMBER = 5;
        private double zeroThreshold_ = 0D;

        /**
         *
         *
         * <pre>
         * Breadth of the zero bucket.
         * </pre>
         *
         * <code>double zero_threshold = 5;</code>
         *
         * @return The zeroThreshold.
         */
        @Override
        public double getZeroThreshold() {
            return zeroThreshold_;
        }

        public static final int ZERO_COUNT_INT_FIELD_NUMBER = 6;

        /**
         * <code>uint64 zero_count_int = 6;</code>
         *
         * @return Whether the zeroCountInt field is set.
         */
        @Override
        public boolean hasZeroCountInt() {
            return zeroCountCase_ == 6;
        }

        /**
         * <code>uint64 zero_count_int = 6;</code>
         *
         * @return The zeroCountInt.
         */
        @Override
        public long getZeroCountInt() {
            if (zeroCountCase_ == 6) {
                return (Long) zeroCount_;
            }
            return 0L;
        }

        public static final int ZERO_COUNT_FLOAT_FIELD_NUMBER = 7;

        /**
         * <code>double zero_count_float = 7;</code>
         *
         * @return Whether the zeroCountFloat field is set.
         */
        @Override
        public boolean hasZeroCountFloat() {
            return zeroCountCase_ == 7;
        }

        /**
         * <code>double zero_count_float = 7;</code>
         *
         * @return The zeroCountFloat.
         */
        @Override
        public double getZeroCountFloat() {
            if (zeroCountCase_ == 7) {
                return (Double) zeroCount_;
            }
            return 0D;
        }

        public static final int NEGATIVE_SPANS_FIELD_NUMBER = 8;

        @SuppressWarnings("serial")
        private java.util.List<Types.BucketSpan> negativeSpans_;

        /**
         *
         *
         * <pre>
         * Negative Buckets.
         * </pre>
         *
         * <code>repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public java.util.List<Types.BucketSpan> getNegativeSpansList() {
            return negativeSpans_;
        }

        /**
         *
         *
         * <pre>
         * Negative Buckets.
         * </pre>
         *
         * <code>repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public java.util.List<? extends Types.BucketSpanOrBuilder> getNegativeSpansOrBuilderList() {
            return negativeSpans_;
        }

        /**
         *
         *
         * <pre>
         * Negative Buckets.
         * </pre>
         *
         * <code>repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public int getNegativeSpansCount() {
            return negativeSpans_.size();
        }

        /**
         *
         *
         * <pre>
         * Negative Buckets.
         * </pre>
         *
         * <code>repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public Types.BucketSpan getNegativeSpans(int index) {
            return negativeSpans_.get(index);
        }

        /**
         *
         *
         * <pre>
         * Negative Buckets.
         * </pre>
         *
         * <code>repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public Types.BucketSpanOrBuilder getNegativeSpansOrBuilder(int index) {
            return negativeSpans_.get(index);
        }

        public static final int NEGATIVE_DELTAS_FIELD_NUMBER = 9;

        @SuppressWarnings("serial")
        private com.google.protobuf.Internal.LongList negativeDeltas_ = emptyLongList();

        /**
         *
         *
         * <pre>
         * Use either "negative_deltas" or "negative_counts", the former for
         * regular histograms with integer counts, the latter for float
         * histograms.
         * </pre>
         *
         * <code>repeated sint64 negative_deltas = 9;</code>
         *
         * @return A list containing the negativeDeltas.
         */
        @Override
        public java.util.List<Long> getNegativeDeltasList() {
            return negativeDeltas_;
        }

        /**
         *
         *
         * <pre>
         * Use either "negative_deltas" or "negative_counts", the former for
         * regular histograms with integer counts, the latter for float
         * histograms.
         * </pre>
         *
         * <code>repeated sint64 negative_deltas = 9;</code>
         *
         * @return The count of negativeDeltas.
         */
        public int getNegativeDeltasCount() {
            return negativeDeltas_.size();
        }

        /**
         *
         *
         * <pre>
         * Use either "negative_deltas" or "negative_counts", the former for
         * regular histograms with integer counts, the latter for float
         * histograms.
         * </pre>
         *
         * <code>repeated sint64 negative_deltas = 9;</code>
         *
         * @param index The index of the element to return.
         * @return The negativeDeltas at the given index.
         */
        public long getNegativeDeltas(int index) {
            return negativeDeltas_.getLong(index);
        }

        private int negativeDeltasMemoizedSerializedSize = -1;

        public static final int NEGATIVE_COUNTS_FIELD_NUMBER = 10;

        @SuppressWarnings("serial")
        private com.google.protobuf.Internal.DoubleList negativeCounts_ = emptyDoubleList();

        /**
         *
         *
         * <pre>
         * Absolute count of each bucket.
         * </pre>
         *
         * <code>repeated double negative_counts = 10;</code>
         *
         * @return A list containing the negativeCounts.
         */
        @Override
        public java.util.List<Double> getNegativeCountsList() {
            return negativeCounts_;
        }

        /**
         *
         *
         * <pre>
         * Absolute count of each bucket.
         * </pre>
         *
         * <code>repeated double negative_counts = 10;</code>
         *
         * @return The count of negativeCounts.
         */
        public int getNegativeCountsCount() {
            return negativeCounts_.size();
        }

        /**
         *
         *
         * <pre>
         * Absolute count of each bucket.
         * </pre>
         *
         * <code>repeated double negative_counts = 10;</code>
         *
         * @param index The index of the element to return.
         * @return The negativeCounts at the given index.
         */
        public double getNegativeCounts(int index) {
            return negativeCounts_.getDouble(index);
        }

        private int negativeCountsMemoizedSerializedSize = -1;

        public static final int POSITIVE_SPANS_FIELD_NUMBER = 11;

        @SuppressWarnings("serial")
        private java.util.List<Types.BucketSpan> positiveSpans_;

        /**
         *
         *
         * <pre>
         * Positive Buckets.
         * </pre>
         *
         * <code>
         * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public java.util.List<Types.BucketSpan> getPositiveSpansList() {
            return positiveSpans_;
        }

        /**
         *
         *
         * <pre>
         * Positive Buckets.
         * </pre>
         *
         * <code>
         * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public java.util.List<? extends Types.BucketSpanOrBuilder> getPositiveSpansOrBuilderList() {
            return positiveSpans_;
        }

        /**
         *
         *
         * <pre>
         * Positive Buckets.
         * </pre>
         *
         * <code>
         * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public int getPositiveSpansCount() {
            return positiveSpans_.size();
        }

        /**
         *
         *
         * <pre>
         * Positive Buckets.
         * </pre>
         *
         * <code>
         * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public Types.BucketSpan getPositiveSpans(int index) {
            return positiveSpans_.get(index);
        }

        /**
         *
         *
         * <pre>
         * Positive Buckets.
         * </pre>
         *
         * <code>
         * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public Types.BucketSpanOrBuilder getPositiveSpansOrBuilder(int index) {
            return positiveSpans_.get(index);
        }

        public static final int POSITIVE_DELTAS_FIELD_NUMBER = 12;

        @SuppressWarnings("serial")
        private com.google.protobuf.Internal.LongList positiveDeltas_ = emptyLongList();

        /**
         *
         *
         * <pre>
         * Use either "positive_deltas" or "positive_counts", the former for
         * regular histograms with integer counts, the latter for float
         * histograms.
         * </pre>
         *
         * <code>repeated sint64 positive_deltas = 12;</code>
         *
         * @return A list containing the positiveDeltas.
         */
        @Override
        public java.util.List<Long> getPositiveDeltasList() {
            return positiveDeltas_;
        }

        /**
         *
         *
         * <pre>
         * Use either "positive_deltas" or "positive_counts", the former for
         * regular histograms with integer counts, the latter for float
         * histograms.
         * </pre>
         *
         * <code>repeated sint64 positive_deltas = 12;</code>
         *
         * @return The count of positiveDeltas.
         */
        public int getPositiveDeltasCount() {
            return positiveDeltas_.size();
        }

        /**
         *
         *
         * <pre>
         * Use either "positive_deltas" or "positive_counts", the former for
         * regular histograms with integer counts, the latter for float
         * histograms.
         * </pre>
         *
         * <code>repeated sint64 positive_deltas = 12;</code>
         *
         * @param index The index of the element to return.
         * @return The positiveDeltas at the given index.
         */
        public long getPositiveDeltas(int index) {
            return positiveDeltas_.getLong(index);
        }

        private int positiveDeltasMemoizedSerializedSize = -1;

        public static final int POSITIVE_COUNTS_FIELD_NUMBER = 13;

        @SuppressWarnings("serial")
        private com.google.protobuf.Internal.DoubleList positiveCounts_ = emptyDoubleList();

        /**
         *
         *
         * <pre>
         * Absolute count of each bucket.
         * </pre>
         *
         * <code>repeated double positive_counts = 13;</code>
         *
         * @return A list containing the positiveCounts.
         */
        @Override
        public java.util.List<Double> getPositiveCountsList() {
            return positiveCounts_;
        }

        /**
         *
         *
         * <pre>
         * Absolute count of each bucket.
         * </pre>
         *
         * <code>repeated double positive_counts = 13;</code>
         *
         * @return The count of positiveCounts.
         */
        public int getPositiveCountsCount() {
            return positiveCounts_.size();
        }

        /**
         *
         *
         * <pre>
         * Absolute count of each bucket.
         * </pre>
         *
         * <code>repeated double positive_counts = 13;</code>
         *
         * @param index The index of the element to return.
         * @return The positiveCounts at the given index.
         */
        public double getPositiveCounts(int index) {
            return positiveCounts_.getDouble(index);
        }

        private int positiveCountsMemoizedSerializedSize = -1;

        public static final int RESET_HINT_FIELD_NUMBER = 14;
        private int resetHint_ = 0;

        /**
         * <code>.prometheus.Histogram.ResetHint reset_hint = 14;</code>
         *
         * @return The enum numeric value on the wire for resetHint.
         */
        @Override
        public int getResetHintValue() {
            return resetHint_;
        }

        /**
         * <code>.prometheus.Histogram.ResetHint reset_hint = 14;</code>
         *
         * @return The resetHint.
         */
        @Override
        public Types.Histogram.ResetHint getResetHint() {
            Types.Histogram.ResetHint result = Types.Histogram.ResetHint.forNumber(resetHint_);
            return result == null ? Types.Histogram.ResetHint.UNRECOGNIZED : result;
        }

        public static final int TIMESTAMP_FIELD_NUMBER = 15;
        private long timestamp_ = 0L;

        /**
         *
         *
         * <pre>
         * timestamp is in ms format, see model/timestamp/timestamp.go for
         * conversion from time.Time to Prometheus timestamp.
         * </pre>
         *
         * <code>int64 timestamp = 15;</code>
         *
         * @return The timestamp.
         */
        @Override
        public long getTimestamp() {
            return timestamp_;
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
            getSerializedSize();
            if (countCase_ == 1) {
                output.writeUInt64(1, (long) ((Long) count_));
            }
            if (countCase_ == 2) {
                output.writeDouble(2, (double) ((Double) count_));
            }
            if (Double.doubleToRawLongBits(sum_) != 0) {
                output.writeDouble(3, sum_);
            }
            if (schema_ != 0) {
                output.writeSInt32(4, schema_);
            }
            if (Double.doubleToRawLongBits(zeroThreshold_) != 0) {
                output.writeDouble(5, zeroThreshold_);
            }
            if (zeroCountCase_ == 6) {
                output.writeUInt64(6, (long) ((Long) zeroCount_));
            }
            if (zeroCountCase_ == 7) {
                output.writeDouble(7, (double) ((Double) zeroCount_));
            }
            for (int i = 0; i < negativeSpans_.size(); i++) {
                output.writeMessage(8, negativeSpans_.get(i));
            }
            if (getNegativeDeltasList().size() > 0) {
                output.writeUInt32NoTag(74);
                output.writeUInt32NoTag(negativeDeltasMemoizedSerializedSize);
            }
            for (int i = 0; i < negativeDeltas_.size(); i++) {
                output.writeSInt64NoTag(negativeDeltas_.getLong(i));
            }
            if (getNegativeCountsList().size() > 0) {
                output.writeUInt32NoTag(82);
                output.writeUInt32NoTag(negativeCountsMemoizedSerializedSize);
            }
            for (int i = 0; i < negativeCounts_.size(); i++) {
                output.writeDoubleNoTag(negativeCounts_.getDouble(i));
            }
            for (int i = 0; i < positiveSpans_.size(); i++) {
                output.writeMessage(11, positiveSpans_.get(i));
            }
            if (getPositiveDeltasList().size() > 0) {
                output.writeUInt32NoTag(98);
                output.writeUInt32NoTag(positiveDeltasMemoizedSerializedSize);
            }
            for (int i = 0; i < positiveDeltas_.size(); i++) {
                output.writeSInt64NoTag(positiveDeltas_.getLong(i));
            }
            if (getPositiveCountsList().size() > 0) {
                output.writeUInt32NoTag(106);
                output.writeUInt32NoTag(positiveCountsMemoizedSerializedSize);
            }
            for (int i = 0; i < positiveCounts_.size(); i++) {
                output.writeDoubleNoTag(positiveCounts_.getDouble(i));
            }
            if (resetHint_ != Types.Histogram.ResetHint.UNKNOWN.getNumber()) {
                output.writeEnum(14, resetHint_);
            }
            if (timestamp_ != 0L) {
                output.writeInt64(15, timestamp_);
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
            if (countCase_ == 1) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeUInt64Size(
                                1, (long) ((Long) count_));
            }
            if (countCase_ == 2) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeDoubleSize(
                                2, (double) ((Double) count_));
            }
            if (Double.doubleToRawLongBits(sum_) != 0) {
                size += com.google.protobuf.CodedOutputStream.computeDoubleSize(3, sum_);
            }
            if (schema_ != 0) {
                size += com.google.protobuf.CodedOutputStream.computeSInt32Size(4, schema_);
            }
            if (Double.doubleToRawLongBits(zeroThreshold_) != 0) {
                size += com.google.protobuf.CodedOutputStream.computeDoubleSize(5, zeroThreshold_);
            }
            if (zeroCountCase_ == 6) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeUInt64Size(
                                6, (long) ((Long) zeroCount_));
            }
            if (zeroCountCase_ == 7) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeDoubleSize(
                                7, (double) ((Double) zeroCount_));
            }
            for (int i = 0; i < negativeSpans_.size(); i++) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeMessageSize(
                                8, negativeSpans_.get(i));
            }
            {
                int dataSize = 0;
                for (int i = 0; i < negativeDeltas_.size(); i++) {
                    dataSize +=
                            com.google.protobuf.CodedOutputStream.computeSInt64SizeNoTag(
                                    negativeDeltas_.getLong(i));
                }
                size += dataSize;
                if (!getNegativeDeltasList().isEmpty()) {
                    size += 1;
                    size += com.google.protobuf.CodedOutputStream.computeInt32SizeNoTag(dataSize);
                }
                negativeDeltasMemoizedSerializedSize = dataSize;
            }
            {
                int dataSize = 0;
                dataSize = 8 * getNegativeCountsList().size();
                size += dataSize;
                if (!getNegativeCountsList().isEmpty()) {
                    size += 1;
                    size += com.google.protobuf.CodedOutputStream.computeInt32SizeNoTag(dataSize);
                }
                negativeCountsMemoizedSerializedSize = dataSize;
            }
            for (int i = 0; i < positiveSpans_.size(); i++) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeMessageSize(
                                11, positiveSpans_.get(i));
            }
            {
                int dataSize = 0;
                for (int i = 0; i < positiveDeltas_.size(); i++) {
                    dataSize +=
                            com.google.protobuf.CodedOutputStream.computeSInt64SizeNoTag(
                                    positiveDeltas_.getLong(i));
                }
                size += dataSize;
                if (!getPositiveDeltasList().isEmpty()) {
                    size += 1;
                    size += com.google.protobuf.CodedOutputStream.computeInt32SizeNoTag(dataSize);
                }
                positiveDeltasMemoizedSerializedSize = dataSize;
            }
            {
                int dataSize = 0;
                dataSize = 8 * getPositiveCountsList().size();
                size += dataSize;
                if (!getPositiveCountsList().isEmpty()) {
                    size += 1;
                    size += com.google.protobuf.CodedOutputStream.computeInt32SizeNoTag(dataSize);
                }
                positiveCountsMemoizedSerializedSize = dataSize;
            }
            if (resetHint_ != Types.Histogram.ResetHint.UNKNOWN.getNumber()) {
                size += com.google.protobuf.CodedOutputStream.computeEnumSize(14, resetHint_);
            }
            if (timestamp_ != 0L) {
                size += com.google.protobuf.CodedOutputStream.computeInt64Size(15, timestamp_);
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
            if (!(obj instanceof Types.Histogram)) {
                return super.equals(obj);
            }
            Types.Histogram other = (Types.Histogram) obj;

            if (Double.doubleToLongBits(getSum()) != Double.doubleToLongBits(other.getSum())) {
                return false;
            }
            if (getSchema() != other.getSchema()) {
                return false;
            }
            if (Double.doubleToLongBits(getZeroThreshold())
                    != Double.doubleToLongBits(other.getZeroThreshold())) {
                return false;
            }
            if (!getNegativeSpansList().equals(other.getNegativeSpansList())) {
                return false;
            }
            if (!getNegativeDeltasList().equals(other.getNegativeDeltasList())) {
                return false;
            }
            if (!getNegativeCountsList().equals(other.getNegativeCountsList())) {
                return false;
            }
            if (!getPositiveSpansList().equals(other.getPositiveSpansList())) {
                return false;
            }
            if (!getPositiveDeltasList().equals(other.getPositiveDeltasList())) {
                return false;
            }
            if (!getPositiveCountsList().equals(other.getPositiveCountsList())) {
                return false;
            }
            if (resetHint_ != other.resetHint_) {
                return false;
            }
            if (getTimestamp() != other.getTimestamp()) {
                return false;
            }
            if (!getCountCase().equals(other.getCountCase())) {
                return false;
            }
            switch (countCase_) {
                case 1:
                    if (getCountInt() != other.getCountInt()) {
                        return false;
                    }
                    break;
                case 2:
                    if (Double.doubleToLongBits(getCountFloat())
                            != Double.doubleToLongBits(other.getCountFloat())) {
                        return false;
                    }
                    break;
                case 0:
                default:
            }
            if (!getZeroCountCase().equals(other.getZeroCountCase())) {
                return false;
            }
            switch (zeroCountCase_) {
                case 6:
                    if (getZeroCountInt() != other.getZeroCountInt()) {
                        return false;
                    }
                    break;
                case 7:
                    if (Double.doubleToLongBits(getZeroCountFloat())
                            != Double.doubleToLongBits(other.getZeroCountFloat())) {
                        return false;
                    }
                    break;
                case 0:
                default:
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
            hash = (37 * hash) + SUM_FIELD_NUMBER;
            hash =
                    (53 * hash)
                            + com.google.protobuf.Internal.hashLong(
                                    Double.doubleToLongBits(getSum()));
            hash = (37 * hash) + SCHEMA_FIELD_NUMBER;
            hash = (53 * hash) + getSchema();
            hash = (37 * hash) + ZERO_THRESHOLD_FIELD_NUMBER;
            hash =
                    (53 * hash)
                            + com.google.protobuf.Internal.hashLong(
                                    Double.doubleToLongBits(getZeroThreshold()));
            if (getNegativeSpansCount() > 0) {
                hash = (37 * hash) + NEGATIVE_SPANS_FIELD_NUMBER;
                hash = (53 * hash) + getNegativeSpansList().hashCode();
            }
            if (getNegativeDeltasCount() > 0) {
                hash = (37 * hash) + NEGATIVE_DELTAS_FIELD_NUMBER;
                hash = (53 * hash) + getNegativeDeltasList().hashCode();
            }
            if (getNegativeCountsCount() > 0) {
                hash = (37 * hash) + NEGATIVE_COUNTS_FIELD_NUMBER;
                hash = (53 * hash) + getNegativeCountsList().hashCode();
            }
            if (getPositiveSpansCount() > 0) {
                hash = (37 * hash) + POSITIVE_SPANS_FIELD_NUMBER;
                hash = (53 * hash) + getPositiveSpansList().hashCode();
            }
            if (getPositiveDeltasCount() > 0) {
                hash = (37 * hash) + POSITIVE_DELTAS_FIELD_NUMBER;
                hash = (53 * hash) + getPositiveDeltasList().hashCode();
            }
            if (getPositiveCountsCount() > 0) {
                hash = (37 * hash) + POSITIVE_COUNTS_FIELD_NUMBER;
                hash = (53 * hash) + getPositiveCountsList().hashCode();
            }
            hash = (37 * hash) + RESET_HINT_FIELD_NUMBER;
            hash = (53 * hash) + resetHint_;
            hash = (37 * hash) + TIMESTAMP_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getTimestamp());
            switch (countCase_) {
                case 1:
                    hash = (37 * hash) + COUNT_INT_FIELD_NUMBER;
                    hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getCountInt());
                    break;
                case 2:
                    hash = (37 * hash) + COUNT_FLOAT_FIELD_NUMBER;
                    hash =
                            (53 * hash)
                                    + com.google.protobuf.Internal.hashLong(
                                            Double.doubleToLongBits(getCountFloat()));
                    break;
                case 0:
                default:
            }
            switch (zeroCountCase_) {
                case 6:
                    hash = (37 * hash) + ZERO_COUNT_INT_FIELD_NUMBER;
                    hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getZeroCountInt());
                    break;
                case 7:
                    hash = (37 * hash) + ZERO_COUNT_FLOAT_FIELD_NUMBER;
                    hash =
                            (53 * hash)
                                    + com.google.protobuf.Internal.hashLong(
                                            Double.doubleToLongBits(getZeroCountFloat()));
                    break;
                case 0:
                default:
            }
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Types.Histogram parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Histogram parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Histogram parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Histogram parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Histogram parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Histogram parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Histogram parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.Histogram parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.Histogram parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Types.Histogram parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.Histogram parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.Histogram parseFrom(
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

        public static Builder newBuilder(Types.Histogram prototype) {
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
         * A native histogram, also known as a sparse histogram.
         * Original design doc:
         * https://docs.google.com/document/d/1cLNv3aufPZb3fNfaJgdaRBZsInZKKIHo9E6HinJVbpM/edit
         * The appendix of this design doc also explains the concept of float
         * histograms. This Histogram message can represent both, the usual
         * integer histogram as well as a float histogram.
         * </pre>
         *
         * <p>Protobuf type {@code prometheus.Histogram}
         */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.Histogram)
                Types.HistogramOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Types.internal_static_prometheus_Histogram_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Types.internal_static_prometheus_Histogram_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Types.Histogram.class, Types.Histogram.Builder.class);
            }

            // Construct using Types.Histogram.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                sum_ = 0D;
                schema_ = 0;
                zeroThreshold_ = 0D;
                if (negativeSpansBuilder_ == null) {
                    negativeSpans_ = java.util.Collections.emptyList();
                } else {
                    negativeSpans_ = null;
                    negativeSpansBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000080);
                negativeDeltas_ = emptyLongList();
                negativeCounts_ = emptyDoubleList();
                if (positiveSpansBuilder_ == null) {
                    positiveSpans_ = java.util.Collections.emptyList();
                } else {
                    positiveSpans_ = null;
                    positiveSpansBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000400);
                positiveDeltas_ = emptyLongList();
                positiveCounts_ = emptyDoubleList();
                resetHint_ = 0;
                timestamp_ = 0L;
                countCase_ = 0;
                count_ = null;
                zeroCountCase_ = 0;
                zeroCount_ = null;
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Types.internal_static_prometheus_Histogram_descriptor;
            }

            @Override
            public Types.Histogram getDefaultInstanceForType() {
                return Types.Histogram.getDefaultInstance();
            }

            @Override
            public Types.Histogram build() {
                Types.Histogram result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Types.Histogram buildPartial() {
                Types.Histogram result = new Types.Histogram(this);
                buildPartialRepeatedFields(result);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                buildPartialOneofs(result);
                onBuilt();
                return result;
            }

            private void buildPartialRepeatedFields(Types.Histogram result) {
                if (negativeSpansBuilder_ == null) {
                    if (((bitField0_ & 0x00000080) != 0)) {
                        negativeSpans_ = java.util.Collections.unmodifiableList(negativeSpans_);
                        bitField0_ = (bitField0_ & ~0x00000080);
                    }
                    result.negativeSpans_ = negativeSpans_;
                } else {
                    result.negativeSpans_ = negativeSpansBuilder_.build();
                }
                if (positiveSpansBuilder_ == null) {
                    if (((bitField0_ & 0x00000400) != 0)) {
                        positiveSpans_ = java.util.Collections.unmodifiableList(positiveSpans_);
                        bitField0_ = (bitField0_ & ~0x00000400);
                    }
                    result.positiveSpans_ = positiveSpans_;
                } else {
                    result.positiveSpans_ = positiveSpansBuilder_.build();
                }
            }

            private void buildPartial0(Types.Histogram result) {
                int from_bitField0_ = bitField0_;
                if (((from_bitField0_ & 0x00000004) != 0)) {
                    result.sum_ = sum_;
                }
                if (((from_bitField0_ & 0x00000008) != 0)) {
                    result.schema_ = schema_;
                }
                if (((from_bitField0_ & 0x00000010) != 0)) {
                    result.zeroThreshold_ = zeroThreshold_;
                }
                if (((from_bitField0_ & 0x00000100) != 0)) {
                    negativeDeltas_.makeImmutable();
                    result.negativeDeltas_ = negativeDeltas_;
                }
                if (((from_bitField0_ & 0x00000200) != 0)) {
                    negativeCounts_.makeImmutable();
                    result.negativeCounts_ = negativeCounts_;
                }
                if (((from_bitField0_ & 0x00000800) != 0)) {
                    positiveDeltas_.makeImmutable();
                    result.positiveDeltas_ = positiveDeltas_;
                }
                if (((from_bitField0_ & 0x00001000) != 0)) {
                    positiveCounts_.makeImmutable();
                    result.positiveCounts_ = positiveCounts_;
                }
                if (((from_bitField0_ & 0x00002000) != 0)) {
                    result.resetHint_ = resetHint_;
                }
                if (((from_bitField0_ & 0x00004000) != 0)) {
                    result.timestamp_ = timestamp_;
                }
            }

            private void buildPartialOneofs(Types.Histogram result) {
                result.countCase_ = countCase_;
                result.count_ = this.count_;
                result.zeroCountCase_ = zeroCountCase_;
                result.zeroCount_ = this.zeroCount_;
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
                if (other instanceof Types.Histogram) {
                    return mergeFrom((Types.Histogram) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Types.Histogram other) {
                if (other == Types.Histogram.getDefaultInstance()) {
                    return this;
                }
                if (other.getSum() != 0D) {
                    setSum(other.getSum());
                }
                if (other.getSchema() != 0) {
                    setSchema(other.getSchema());
                }
                if (other.getZeroThreshold() != 0D) {
                    setZeroThreshold(other.getZeroThreshold());
                }
                if (negativeSpansBuilder_ == null) {
                    if (!other.negativeSpans_.isEmpty()) {
                        if (negativeSpans_.isEmpty()) {
                            negativeSpans_ = other.negativeSpans_;
                            bitField0_ = (bitField0_ & ~0x00000080);
                        } else {
                            ensureNegativeSpansIsMutable();
                            negativeSpans_.addAll(other.negativeSpans_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.negativeSpans_.isEmpty()) {
                        if (negativeSpansBuilder_.isEmpty()) {
                            negativeSpansBuilder_.dispose();
                            negativeSpansBuilder_ = null;
                            negativeSpans_ = other.negativeSpans_;
                            bitField0_ = (bitField0_ & ~0x00000080);
                            negativeSpansBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getNegativeSpansFieldBuilder()
                                            : null;
                        } else {
                            negativeSpansBuilder_.addAllMessages(other.negativeSpans_);
                        }
                    }
                }
                if (!other.negativeDeltas_.isEmpty()) {
                    if (negativeDeltas_.isEmpty()) {
                        negativeDeltas_ = other.negativeDeltas_;
                        negativeDeltas_.makeImmutable();
                        bitField0_ |= 0x00000100;
                    } else {
                        ensureNegativeDeltasIsMutable();
                        negativeDeltas_.addAll(other.negativeDeltas_);
                    }
                    onChanged();
                }
                if (!other.negativeCounts_.isEmpty()) {
                    if (negativeCounts_.isEmpty()) {
                        negativeCounts_ = other.negativeCounts_;
                        negativeCounts_.makeImmutable();
                        bitField0_ |= 0x00000200;
                    } else {
                        ensureNegativeCountsIsMutable();
                        negativeCounts_.addAll(other.negativeCounts_);
                    }
                    onChanged();
                }
                if (positiveSpansBuilder_ == null) {
                    if (!other.positiveSpans_.isEmpty()) {
                        if (positiveSpans_.isEmpty()) {
                            positiveSpans_ = other.positiveSpans_;
                            bitField0_ = (bitField0_ & ~0x00000400);
                        } else {
                            ensurePositiveSpansIsMutable();
                            positiveSpans_.addAll(other.positiveSpans_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.positiveSpans_.isEmpty()) {
                        if (positiveSpansBuilder_.isEmpty()) {
                            positiveSpansBuilder_.dispose();
                            positiveSpansBuilder_ = null;
                            positiveSpans_ = other.positiveSpans_;
                            bitField0_ = (bitField0_ & ~0x00000400);
                            positiveSpansBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getPositiveSpansFieldBuilder()
                                            : null;
                        } else {
                            positiveSpansBuilder_.addAllMessages(other.positiveSpans_);
                        }
                    }
                }
                if (!other.positiveDeltas_.isEmpty()) {
                    if (positiveDeltas_.isEmpty()) {
                        positiveDeltas_ = other.positiveDeltas_;
                        positiveDeltas_.makeImmutable();
                        bitField0_ |= 0x00000800;
                    } else {
                        ensurePositiveDeltasIsMutable();
                        positiveDeltas_.addAll(other.positiveDeltas_);
                    }
                    onChanged();
                }
                if (!other.positiveCounts_.isEmpty()) {
                    if (positiveCounts_.isEmpty()) {
                        positiveCounts_ = other.positiveCounts_;
                        positiveCounts_.makeImmutable();
                        bitField0_ |= 0x00001000;
                    } else {
                        ensurePositiveCountsIsMutable();
                        positiveCounts_.addAll(other.positiveCounts_);
                    }
                    onChanged();
                }
                if (other.resetHint_ != 0) {
                    setResetHintValue(other.getResetHintValue());
                }
                if (other.getTimestamp() != 0L) {
                    setTimestamp(other.getTimestamp());
                }
                switch (other.getCountCase()) {
                    case COUNT_INT:
                        {
                            setCountInt(other.getCountInt());
                            break;
                        }
                    case COUNT_FLOAT:
                        {
                            setCountFloat(other.getCountFloat());
                            break;
                        }
                    case COUNT_NOT_SET:
                        {
                            break;
                        }
                }
                switch (other.getZeroCountCase()) {
                    case ZERO_COUNT_INT:
                        {
                            setZeroCountInt(other.getZeroCountInt());
                            break;
                        }
                    case ZERO_COUNT_FLOAT:
                        {
                            setZeroCountFloat(other.getZeroCountFloat());
                            break;
                        }
                    case ZEROCOUNT_NOT_SET:
                        {
                            break;
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
                            case 8:
                                {
                                    count_ = input.readUInt64();
                                    countCase_ = 1;
                                    break;
                                } // case 8
                            case 17:
                                {
                                    count_ = input.readDouble();
                                    countCase_ = 2;
                                    break;
                                } // case 17
                            case 25:
                                {
                                    sum_ = input.readDouble();
                                    bitField0_ |= 0x00000004;
                                    break;
                                } // case 25
                            case 32:
                                {
                                    schema_ = input.readSInt32();
                                    bitField0_ |= 0x00000008;
                                    break;
                                } // case 32
                            case 41:
                                {
                                    zeroThreshold_ = input.readDouble();
                                    bitField0_ |= 0x00000010;
                                    break;
                                } // case 41
                            case 48:
                                {
                                    zeroCount_ = input.readUInt64();
                                    zeroCountCase_ = 6;
                                    break;
                                } // case 48
                            case 57:
                                {
                                    zeroCount_ = input.readDouble();
                                    zeroCountCase_ = 7;
                                    break;
                                } // case 57
                            case 66:
                                {
                                    Types.BucketSpan m =
                                            input.readMessage(
                                                    Types.BucketSpan.parser(), extensionRegistry);
                                    if (negativeSpansBuilder_ == null) {
                                        ensureNegativeSpansIsMutable();
                                        negativeSpans_.add(m);
                                    } else {
                                        negativeSpansBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 66
                            case 72:
                                {
                                    long v = input.readSInt64();
                                    ensureNegativeDeltasIsMutable();
                                    negativeDeltas_.addLong(v);
                                    break;
                                } // case 72
                            case 74:
                                {
                                    int length = input.readRawVarint32();
                                    int limit = input.pushLimit(length);
                                    ensureNegativeDeltasIsMutable();
                                    while (input.getBytesUntilLimit() > 0) {
                                        negativeDeltas_.addLong(input.readSInt64());
                                    }
                                    input.popLimit(limit);
                                    break;
                                } // case 74
                            case 81:
                                {
                                    double v = input.readDouble();
                                    ensureNegativeCountsIsMutable();
                                    negativeCounts_.addDouble(v);
                                    break;
                                } // case 81
                            case 82:
                                {
                                    int length = input.readRawVarint32();
                                    int limit = input.pushLimit(length);
                                    int alloc = length > 4096 ? 4096 : length;
                                    ensureNegativeCountsIsMutable(alloc / 8);
                                    while (input.getBytesUntilLimit() > 0) {
                                        negativeCounts_.addDouble(input.readDouble());
                                    }
                                    input.popLimit(limit);
                                    break;
                                } // case 82
                            case 90:
                                {
                                    Types.BucketSpan m =
                                            input.readMessage(
                                                    Types.BucketSpan.parser(), extensionRegistry);
                                    if (positiveSpansBuilder_ == null) {
                                        ensurePositiveSpansIsMutable();
                                        positiveSpans_.add(m);
                                    } else {
                                        positiveSpansBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 90
                            case 96:
                                {
                                    long v = input.readSInt64();
                                    ensurePositiveDeltasIsMutable();
                                    positiveDeltas_.addLong(v);
                                    break;
                                } // case 96
                            case 98:
                                {
                                    int length = input.readRawVarint32();
                                    int limit = input.pushLimit(length);
                                    ensurePositiveDeltasIsMutable();
                                    while (input.getBytesUntilLimit() > 0) {
                                        positiveDeltas_.addLong(input.readSInt64());
                                    }
                                    input.popLimit(limit);
                                    break;
                                } // case 98
                            case 105:
                                {
                                    double v = input.readDouble();
                                    ensurePositiveCountsIsMutable();
                                    positiveCounts_.addDouble(v);
                                    break;
                                } // case 105
                            case 106:
                                {
                                    int length = input.readRawVarint32();
                                    int limit = input.pushLimit(length);
                                    int alloc = length > 4096 ? 4096 : length;
                                    ensurePositiveCountsIsMutable(alloc / 8);
                                    while (input.getBytesUntilLimit() > 0) {
                                        positiveCounts_.addDouble(input.readDouble());
                                    }
                                    input.popLimit(limit);
                                    break;
                                } // case 106
                            case 112:
                                {
                                    resetHint_ = input.readEnum();
                                    bitField0_ |= 0x00002000;
                                    break;
                                } // case 112
                            case 120:
                                {
                                    timestamp_ = input.readInt64();
                                    bitField0_ |= 0x00004000;
                                    break;
                                } // case 120
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

            private int countCase_ = 0;
            private Object count_;

            public CountCase getCountCase() {
                return CountCase.forNumber(countCase_);
            }

            public Builder clearCount() {
                countCase_ = 0;
                count_ = null;
                onChanged();
                return this;
            }

            private int zeroCountCase_ = 0;
            private Object zeroCount_;

            public ZeroCountCase getZeroCountCase() {
                return ZeroCountCase.forNumber(zeroCountCase_);
            }

            public Builder clearZeroCount() {
                zeroCountCase_ = 0;
                zeroCount_ = null;
                onChanged();
                return this;
            }

            private int bitField0_;

            /**
             * <code>uint64 count_int = 1;</code>
             *
             * @return Whether the countInt field is set.
             */
            public boolean hasCountInt() {
                return countCase_ == 1;
            }

            /**
             * <code>uint64 count_int = 1;</code>
             *
             * @return The countInt.
             */
            public long getCountInt() {
                if (countCase_ == 1) {
                    return (Long) count_;
                }
                return 0L;
            }

            /**
             * <code>uint64 count_int = 1;</code>
             *
             * @param value The countInt to set.
             * @return This builder for chaining.
             */
            public Builder setCountInt(long value) {

                countCase_ = 1;
                count_ = value;
                onChanged();
                return this;
            }

            /**
             * <code>uint64 count_int = 1;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearCountInt() {
                if (countCase_ == 1) {
                    countCase_ = 0;
                    count_ = null;
                    onChanged();
                }
                return this;
            }

            /**
             * <code>double count_float = 2;</code>
             *
             * @return Whether the countFloat field is set.
             */
            public boolean hasCountFloat() {
                return countCase_ == 2;
            }

            /**
             * <code>double count_float = 2;</code>
             *
             * @return The countFloat.
             */
            public double getCountFloat() {
                if (countCase_ == 2) {
                    return (Double) count_;
                }
                return 0D;
            }

            /**
             * <code>double count_float = 2;</code>
             *
             * @param value The countFloat to set.
             * @return This builder for chaining.
             */
            public Builder setCountFloat(double value) {

                countCase_ = 2;
                count_ = value;
                onChanged();
                return this;
            }

            /**
             * <code>double count_float = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearCountFloat() {
                if (countCase_ == 2) {
                    countCase_ = 0;
                    count_ = null;
                    onChanged();
                }
                return this;
            }

            private double sum_;

            /**
             *
             *
             * <pre>
             * Sum of observations in the histogram.
             * </pre>
             *
             * <code>double sum = 3;</code>
             *
             * @return The sum.
             */
            @Override
            public double getSum() {
                return sum_;
            }

            /**
             *
             *
             * <pre>
             * Sum of observations in the histogram.
             * </pre>
             *
             * <code>double sum = 3;</code>
             *
             * @param value The sum to set.
             * @return This builder for chaining.
             */
            public Builder setSum(double value) {

                sum_ = value;
                bitField0_ |= 0x00000004;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Sum of observations in the histogram.
             * </pre>
             *
             * <code>double sum = 3;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearSum() {
                bitField0_ = (bitField0_ & ~0x00000004);
                sum_ = 0D;
                onChanged();
                return this;
            }

            private int schema_;

            /**
             *
             *
             * <pre>
             * The schema defines the bucket schema. Currently, valid numbers
             * are -4 &lt;= n &lt;= 8. They are all for base-2 bucket schemas, where 1
             * is a bucket boundary in each case, and then each power of two is
             * divided into 2^n logarithmic buckets. Or in other words, each
             * bucket boundary is the previous boundary times 2^(2^-n). In the
             * future, more bucket schemas may be added using numbers &lt; -4 or &gt;
             * 8.
             * </pre>
             *
             * <code>sint32 schema = 4;</code>
             *
             * @return The schema.
             */
            @Override
            public int getSchema() {
                return schema_;
            }

            /**
             *
             *
             * <pre>
             * The schema defines the bucket schema. Currently, valid numbers
             * are -4 &lt;= n &lt;= 8. They are all for base-2 bucket schemas, where 1
             * is a bucket boundary in each case, and then each power of two is
             * divided into 2^n logarithmic buckets. Or in other words, each
             * bucket boundary is the previous boundary times 2^(2^-n). In the
             * future, more bucket schemas may be added using numbers &lt; -4 or &gt;
             * 8.
             * </pre>
             *
             * <code>sint32 schema = 4;</code>
             *
             * @param value The schema to set.
             * @return This builder for chaining.
             */
            public Builder setSchema(int value) {

                schema_ = value;
                bitField0_ |= 0x00000008;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * The schema defines the bucket schema. Currently, valid numbers
             * are -4 &lt;= n &lt;= 8. They are all for base-2 bucket schemas, where 1
             * is a bucket boundary in each case, and then each power of two is
             * divided into 2^n logarithmic buckets. Or in other words, each
             * bucket boundary is the previous boundary times 2^(2^-n). In the
             * future, more bucket schemas may be added using numbers &lt; -4 or &gt;
             * 8.
             * </pre>
             *
             * <code>sint32 schema = 4;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearSchema() {
                bitField0_ = (bitField0_ & ~0x00000008);
                schema_ = 0;
                onChanged();
                return this;
            }

            private double zeroThreshold_;

            /**
             *
             *
             * <pre>
             * Breadth of the zero bucket.
             * </pre>
             *
             * <code>double zero_threshold = 5;</code>
             *
             * @return The zeroThreshold.
             */
            @Override
            public double getZeroThreshold() {
                return zeroThreshold_;
            }

            /**
             *
             *
             * <pre>
             * Breadth of the zero bucket.
             * </pre>
             *
             * <code>double zero_threshold = 5;</code>
             *
             * @param value The zeroThreshold to set.
             * @return This builder for chaining.
             */
            public Builder setZeroThreshold(double value) {

                zeroThreshold_ = value;
                bitField0_ |= 0x00000010;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Breadth of the zero bucket.
             * </pre>
             *
             * <code>double zero_threshold = 5;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearZeroThreshold() {
                bitField0_ = (bitField0_ & ~0x00000010);
                zeroThreshold_ = 0D;
                onChanged();
                return this;
            }

            /**
             * <code>uint64 zero_count_int = 6;</code>
             *
             * @return Whether the zeroCountInt field is set.
             */
            public boolean hasZeroCountInt() {
                return zeroCountCase_ == 6;
            }

            /**
             * <code>uint64 zero_count_int = 6;</code>
             *
             * @return The zeroCountInt.
             */
            public long getZeroCountInt() {
                if (zeroCountCase_ == 6) {
                    return (Long) zeroCount_;
                }
                return 0L;
            }

            /**
             * <code>uint64 zero_count_int = 6;</code>
             *
             * @param value The zeroCountInt to set.
             * @return This builder for chaining.
             */
            public Builder setZeroCountInt(long value) {

                zeroCountCase_ = 6;
                zeroCount_ = value;
                onChanged();
                return this;
            }

            /**
             * <code>uint64 zero_count_int = 6;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearZeroCountInt() {
                if (zeroCountCase_ == 6) {
                    zeroCountCase_ = 0;
                    zeroCount_ = null;
                    onChanged();
                }
                return this;
            }

            /**
             * <code>double zero_count_float = 7;</code>
             *
             * @return Whether the zeroCountFloat field is set.
             */
            public boolean hasZeroCountFloat() {
                return zeroCountCase_ == 7;
            }

            /**
             * <code>double zero_count_float = 7;</code>
             *
             * @return The zeroCountFloat.
             */
            public double getZeroCountFloat() {
                if (zeroCountCase_ == 7) {
                    return (Double) zeroCount_;
                }
                return 0D;
            }

            /**
             * <code>double zero_count_float = 7;</code>
             *
             * @param value The zeroCountFloat to set.
             * @return This builder for chaining.
             */
            public Builder setZeroCountFloat(double value) {

                zeroCountCase_ = 7;
                zeroCount_ = value;
                onChanged();
                return this;
            }

            /**
             * <code>double zero_count_float = 7;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearZeroCountFloat() {
                if (zeroCountCase_ == 7) {
                    zeroCountCase_ = 0;
                    zeroCount_ = null;
                    onChanged();
                }
                return this;
            }

            private java.util.List<Types.BucketSpan> negativeSpans_ =
                    java.util.Collections.emptyList();

            private void ensureNegativeSpansIsMutable() {
                if (!((bitField0_ & 0x00000080) != 0)) {
                    negativeSpans_ = new java.util.ArrayList<Types.BucketSpan>(negativeSpans_);
                    bitField0_ |= 0x00000080;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.BucketSpan, Types.BucketSpan.Builder, Types.BucketSpanOrBuilder>
                    negativeSpansBuilder_;

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<Types.BucketSpan> getNegativeSpansList() {
                if (negativeSpansBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(negativeSpans_);
                } else {
                    return negativeSpansBuilder_.getMessageList();
                }
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public int getNegativeSpansCount() {
                if (negativeSpansBuilder_ == null) {
                    return negativeSpans_.size();
                } else {
                    return negativeSpansBuilder_.getCount();
                }
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.BucketSpan getNegativeSpans(int index) {
                if (negativeSpansBuilder_ == null) {
                    return negativeSpans_.get(index);
                } else {
                    return negativeSpansBuilder_.getMessage(index);
                }
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder setNegativeSpans(int index, Types.BucketSpan value) {
                if (negativeSpansBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureNegativeSpansIsMutable();
                    negativeSpans_.set(index, value);
                    onChanged();
                } else {
                    negativeSpansBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder setNegativeSpans(int index, Types.BucketSpan.Builder builderForValue) {
                if (negativeSpansBuilder_ == null) {
                    ensureNegativeSpansIsMutable();
                    negativeSpans_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    negativeSpansBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addNegativeSpans(Types.BucketSpan value) {
                if (negativeSpansBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureNegativeSpansIsMutable();
                    negativeSpans_.add(value);
                    onChanged();
                } else {
                    negativeSpansBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addNegativeSpans(int index, Types.BucketSpan value) {
                if (negativeSpansBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureNegativeSpansIsMutable();
                    negativeSpans_.add(index, value);
                    onChanged();
                } else {
                    negativeSpansBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addNegativeSpans(Types.BucketSpan.Builder builderForValue) {
                if (negativeSpansBuilder_ == null) {
                    ensureNegativeSpansIsMutable();
                    negativeSpans_.add(builderForValue.build());
                    onChanged();
                } else {
                    negativeSpansBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addNegativeSpans(int index, Types.BucketSpan.Builder builderForValue) {
                if (negativeSpansBuilder_ == null) {
                    ensureNegativeSpansIsMutable();
                    negativeSpans_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    negativeSpansBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addAllNegativeSpans(Iterable<? extends Types.BucketSpan> values) {
                if (negativeSpansBuilder_ == null) {
                    ensureNegativeSpansIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, negativeSpans_);
                    onChanged();
                } else {
                    negativeSpansBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder clearNegativeSpans() {
                if (negativeSpansBuilder_ == null) {
                    negativeSpans_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000080);
                    onChanged();
                } else {
                    negativeSpansBuilder_.clear();
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder removeNegativeSpans(int index) {
                if (negativeSpansBuilder_ == null) {
                    ensureNegativeSpansIsMutable();
                    negativeSpans_.remove(index);
                    onChanged();
                } else {
                    negativeSpansBuilder_.remove(index);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.BucketSpan.Builder getNegativeSpansBuilder(int index) {
                return getNegativeSpansFieldBuilder().getBuilder(index);
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.BucketSpanOrBuilder getNegativeSpansOrBuilder(int index) {
                if (negativeSpansBuilder_ == null) {
                    return negativeSpans_.get(index);
                } else {
                    return negativeSpansBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<? extends Types.BucketSpanOrBuilder>
                    getNegativeSpansOrBuilderList() {
                if (negativeSpansBuilder_ != null) {
                    return negativeSpansBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(negativeSpans_);
                }
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.BucketSpan.Builder addNegativeSpansBuilder() {
                return getNegativeSpansFieldBuilder()
                        .addBuilder(Types.BucketSpan.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.BucketSpan.Builder addNegativeSpansBuilder(int index) {
                return getNegativeSpansFieldBuilder()
                        .addBuilder(index, Types.BucketSpan.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * Negative Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan negative_spans = 8 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<Types.BucketSpan.Builder> getNegativeSpansBuilderList() {
                return getNegativeSpansFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.BucketSpan, Types.BucketSpan.Builder, Types.BucketSpanOrBuilder>
                    getNegativeSpansFieldBuilder() {
                if (negativeSpansBuilder_ == null) {
                    negativeSpansBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.BucketSpan,
                                    Types.BucketSpan.Builder,
                                    Types.BucketSpanOrBuilder>(
                                    negativeSpans_,
                                    ((bitField0_ & 0x00000080) != 0),
                                    getParentForChildren(),
                                    isClean());
                    negativeSpans_ = null;
                }
                return negativeSpansBuilder_;
            }

            private com.google.protobuf.Internal.LongList negativeDeltas_ = emptyLongList();

            private void ensureNegativeDeltasIsMutable() {
                if (!negativeDeltas_.isModifiable()) {
                    negativeDeltas_ = makeMutableCopy(negativeDeltas_);
                }
                bitField0_ |= 0x00000100;
            }

            /**
             *
             *
             * <pre>
             * Use either "negative_deltas" or "negative_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 negative_deltas = 9;</code>
             *
             * @return A list containing the negativeDeltas.
             */
            public java.util.List<Long> getNegativeDeltasList() {
                negativeDeltas_.makeImmutable();
                return negativeDeltas_;
            }

            /**
             *
             *
             * <pre>
             * Use either "negative_deltas" or "negative_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 negative_deltas = 9;</code>
             *
             * @return The count of negativeDeltas.
             */
            public int getNegativeDeltasCount() {
                return negativeDeltas_.size();
            }

            /**
             *
             *
             * <pre>
             * Use either "negative_deltas" or "negative_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 negative_deltas = 9;</code>
             *
             * @param index The index of the element to return.
             * @return The negativeDeltas at the given index.
             */
            public long getNegativeDeltas(int index) {
                return negativeDeltas_.getLong(index);
            }

            /**
             *
             *
             * <pre>
             * Use either "negative_deltas" or "negative_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 negative_deltas = 9;</code>
             *
             * @param index The index to set the value at.
             * @param value The negativeDeltas to set.
             * @return This builder for chaining.
             */
            public Builder setNegativeDeltas(int index, long value) {

                ensureNegativeDeltasIsMutable();
                negativeDeltas_.setLong(index, value);
                bitField0_ |= 0x00000100;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Use either "negative_deltas" or "negative_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 negative_deltas = 9;</code>
             *
             * @param value The negativeDeltas to add.
             * @return This builder for chaining.
             */
            public Builder addNegativeDeltas(long value) {

                ensureNegativeDeltasIsMutable();
                negativeDeltas_.addLong(value);
                bitField0_ |= 0x00000100;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Use either "negative_deltas" or "negative_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 negative_deltas = 9;</code>
             *
             * @param values The negativeDeltas to add.
             * @return This builder for chaining.
             */
            public Builder addAllNegativeDeltas(Iterable<? extends Long> values) {
                ensureNegativeDeltasIsMutable();
                com.google.protobuf.AbstractMessageLite.Builder.addAll(values, negativeDeltas_);
                bitField0_ |= 0x00000100;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Use either "negative_deltas" or "negative_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 negative_deltas = 9;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearNegativeDeltas() {
                negativeDeltas_ = emptyLongList();
                bitField0_ = (bitField0_ & ~0x00000100);
                onChanged();
                return this;
            }

            private com.google.protobuf.Internal.DoubleList negativeCounts_ = emptyDoubleList();

            private void ensureNegativeCountsIsMutable() {
                if (!negativeCounts_.isModifiable()) {
                    negativeCounts_ = makeMutableCopy(negativeCounts_);
                }
                bitField0_ |= 0x00000200;
            }

            private void ensureNegativeCountsIsMutable(int capacity) {
                if (!negativeCounts_.isModifiable()) {
                    negativeCounts_ = makeMutableCopy(negativeCounts_, capacity);
                }
                bitField0_ |= 0x00000200;
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double negative_counts = 10;</code>
             *
             * @return A list containing the negativeCounts.
             */
            public java.util.List<Double> getNegativeCountsList() {
                negativeCounts_.makeImmutable();
                return negativeCounts_;
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double negative_counts = 10;</code>
             *
             * @return The count of negativeCounts.
             */
            public int getNegativeCountsCount() {
                return negativeCounts_.size();
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double negative_counts = 10;</code>
             *
             * @param index The index of the element to return.
             * @return The negativeCounts at the given index.
             */
            public double getNegativeCounts(int index) {
                return negativeCounts_.getDouble(index);
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double negative_counts = 10;</code>
             *
             * @param index The index to set the value at.
             * @param value The negativeCounts to set.
             * @return This builder for chaining.
             */
            public Builder setNegativeCounts(int index, double value) {

                ensureNegativeCountsIsMutable();
                negativeCounts_.setDouble(index, value);
                bitField0_ |= 0x00000200;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double negative_counts = 10;</code>
             *
             * @param value The negativeCounts to add.
             * @return This builder for chaining.
             */
            public Builder addNegativeCounts(double value) {

                ensureNegativeCountsIsMutable();
                negativeCounts_.addDouble(value);
                bitField0_ |= 0x00000200;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double negative_counts = 10;</code>
             *
             * @param values The negativeCounts to add.
             * @return This builder for chaining.
             */
            public Builder addAllNegativeCounts(Iterable<? extends Double> values) {
                ensureNegativeCountsIsMutable();
                com.google.protobuf.AbstractMessageLite.Builder.addAll(values, negativeCounts_);
                bitField0_ |= 0x00000200;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double negative_counts = 10;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearNegativeCounts() {
                negativeCounts_ = emptyDoubleList();
                bitField0_ = (bitField0_ & ~0x00000200);
                onChanged();
                return this;
            }

            private java.util.List<Types.BucketSpan> positiveSpans_ =
                    java.util.Collections.emptyList();

            private void ensurePositiveSpansIsMutable() {
                if (!((bitField0_ & 0x00000400) != 0)) {
                    positiveSpans_ = new java.util.ArrayList<Types.BucketSpan>(positiveSpans_);
                    bitField0_ |= 0x00000400;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.BucketSpan, Types.BucketSpan.Builder, Types.BucketSpanOrBuilder>
                    positiveSpansBuilder_;

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<Types.BucketSpan> getPositiveSpansList() {
                if (positiveSpansBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(positiveSpans_);
                } else {
                    return positiveSpansBuilder_.getMessageList();
                }
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public int getPositiveSpansCount() {
                if (positiveSpansBuilder_ == null) {
                    return positiveSpans_.size();
                } else {
                    return positiveSpansBuilder_.getCount();
                }
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.BucketSpan getPositiveSpans(int index) {
                if (positiveSpansBuilder_ == null) {
                    return positiveSpans_.get(index);
                } else {
                    return positiveSpansBuilder_.getMessage(index);
                }
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder setPositiveSpans(int index, Types.BucketSpan value) {
                if (positiveSpansBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensurePositiveSpansIsMutable();
                    positiveSpans_.set(index, value);
                    onChanged();
                } else {
                    positiveSpansBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder setPositiveSpans(int index, Types.BucketSpan.Builder builderForValue) {
                if (positiveSpansBuilder_ == null) {
                    ensurePositiveSpansIsMutable();
                    positiveSpans_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    positiveSpansBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addPositiveSpans(Types.BucketSpan value) {
                if (positiveSpansBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensurePositiveSpansIsMutable();
                    positiveSpans_.add(value);
                    onChanged();
                } else {
                    positiveSpansBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addPositiveSpans(int index, Types.BucketSpan value) {
                if (positiveSpansBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensurePositiveSpansIsMutable();
                    positiveSpans_.add(index, value);
                    onChanged();
                } else {
                    positiveSpansBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addPositiveSpans(Types.BucketSpan.Builder builderForValue) {
                if (positiveSpansBuilder_ == null) {
                    ensurePositiveSpansIsMutable();
                    positiveSpans_.add(builderForValue.build());
                    onChanged();
                } else {
                    positiveSpansBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addPositiveSpans(int index, Types.BucketSpan.Builder builderForValue) {
                if (positiveSpansBuilder_ == null) {
                    ensurePositiveSpansIsMutable();
                    positiveSpans_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    positiveSpansBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addAllPositiveSpans(Iterable<? extends Types.BucketSpan> values) {
                if (positiveSpansBuilder_ == null) {
                    ensurePositiveSpansIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, positiveSpans_);
                    onChanged();
                } else {
                    positiveSpansBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder clearPositiveSpans() {
                if (positiveSpansBuilder_ == null) {
                    positiveSpans_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000400);
                    onChanged();
                } else {
                    positiveSpansBuilder_.clear();
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder removePositiveSpans(int index) {
                if (positiveSpansBuilder_ == null) {
                    ensurePositiveSpansIsMutable();
                    positiveSpans_.remove(index);
                    onChanged();
                } else {
                    positiveSpansBuilder_.remove(index);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.BucketSpan.Builder getPositiveSpansBuilder(int index) {
                return getPositiveSpansFieldBuilder().getBuilder(index);
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.BucketSpanOrBuilder getPositiveSpansOrBuilder(int index) {
                if (positiveSpansBuilder_ == null) {
                    return positiveSpans_.get(index);
                } else {
                    return positiveSpansBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<? extends Types.BucketSpanOrBuilder>
                    getPositiveSpansOrBuilderList() {
                if (positiveSpansBuilder_ != null) {
                    return positiveSpansBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(positiveSpans_);
                }
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.BucketSpan.Builder addPositiveSpansBuilder() {
                return getPositiveSpansFieldBuilder()
                        .addBuilder(Types.BucketSpan.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.BucketSpan.Builder addPositiveSpansBuilder(int index) {
                return getPositiveSpansFieldBuilder()
                        .addBuilder(index, Types.BucketSpan.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * Positive Buckets.
             * </pre>
             *
             * <code>
             * repeated .prometheus.BucketSpan positive_spans = 11 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<Types.BucketSpan.Builder> getPositiveSpansBuilderList() {
                return getPositiveSpansFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.BucketSpan, Types.BucketSpan.Builder, Types.BucketSpanOrBuilder>
                    getPositiveSpansFieldBuilder() {
                if (positiveSpansBuilder_ == null) {
                    positiveSpansBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.BucketSpan,
                                    Types.BucketSpan.Builder,
                                    Types.BucketSpanOrBuilder>(
                                    positiveSpans_,
                                    ((bitField0_ & 0x00000400) != 0),
                                    getParentForChildren(),
                                    isClean());
                    positiveSpans_ = null;
                }
                return positiveSpansBuilder_;
            }

            private com.google.protobuf.Internal.LongList positiveDeltas_ = emptyLongList();

            private void ensurePositiveDeltasIsMutable() {
                if (!positiveDeltas_.isModifiable()) {
                    positiveDeltas_ = makeMutableCopy(positiveDeltas_);
                }
                bitField0_ |= 0x00000800;
            }

            /**
             *
             *
             * <pre>
             * Use either "positive_deltas" or "positive_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 positive_deltas = 12;</code>
             *
             * @return A list containing the positiveDeltas.
             */
            public java.util.List<Long> getPositiveDeltasList() {
                positiveDeltas_.makeImmutable();
                return positiveDeltas_;
            }

            /**
             *
             *
             * <pre>
             * Use either "positive_deltas" or "positive_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 positive_deltas = 12;</code>
             *
             * @return The count of positiveDeltas.
             */
            public int getPositiveDeltasCount() {
                return positiveDeltas_.size();
            }

            /**
             *
             *
             * <pre>
             * Use either "positive_deltas" or "positive_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 positive_deltas = 12;</code>
             *
             * @param index The index of the element to return.
             * @return The positiveDeltas at the given index.
             */
            public long getPositiveDeltas(int index) {
                return positiveDeltas_.getLong(index);
            }

            /**
             *
             *
             * <pre>
             * Use either "positive_deltas" or "positive_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 positive_deltas = 12;</code>
             *
             * @param index The index to set the value at.
             * @param value The positiveDeltas to set.
             * @return This builder for chaining.
             */
            public Builder setPositiveDeltas(int index, long value) {

                ensurePositiveDeltasIsMutable();
                positiveDeltas_.setLong(index, value);
                bitField0_ |= 0x00000800;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Use either "positive_deltas" or "positive_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 positive_deltas = 12;</code>
             *
             * @param value The positiveDeltas to add.
             * @return This builder for chaining.
             */
            public Builder addPositiveDeltas(long value) {

                ensurePositiveDeltasIsMutable();
                positiveDeltas_.addLong(value);
                bitField0_ |= 0x00000800;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Use either "positive_deltas" or "positive_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 positive_deltas = 12;</code>
             *
             * @param values The positiveDeltas to add.
             * @return This builder for chaining.
             */
            public Builder addAllPositiveDeltas(Iterable<? extends Long> values) {
                ensurePositiveDeltasIsMutable();
                com.google.protobuf.AbstractMessageLite.Builder.addAll(values, positiveDeltas_);
                bitField0_ |= 0x00000800;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Use either "positive_deltas" or "positive_counts", the former for
             * regular histograms with integer counts, the latter for float
             * histograms.
             * </pre>
             *
             * <code>repeated sint64 positive_deltas = 12;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearPositiveDeltas() {
                positiveDeltas_ = emptyLongList();
                bitField0_ = (bitField0_ & ~0x00000800);
                onChanged();
                return this;
            }

            private com.google.protobuf.Internal.DoubleList positiveCounts_ = emptyDoubleList();

            private void ensurePositiveCountsIsMutable() {
                if (!positiveCounts_.isModifiable()) {
                    positiveCounts_ = makeMutableCopy(positiveCounts_);
                }
                bitField0_ |= 0x00001000;
            }

            private void ensurePositiveCountsIsMutable(int capacity) {
                if (!positiveCounts_.isModifiable()) {
                    positiveCounts_ = makeMutableCopy(positiveCounts_, capacity);
                }
                bitField0_ |= 0x00001000;
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double positive_counts = 13;</code>
             *
             * @return A list containing the positiveCounts.
             */
            public java.util.List<Double> getPositiveCountsList() {
                positiveCounts_.makeImmutable();
                return positiveCounts_;
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double positive_counts = 13;</code>
             *
             * @return The count of positiveCounts.
             */
            public int getPositiveCountsCount() {
                return positiveCounts_.size();
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double positive_counts = 13;</code>
             *
             * @param index The index of the element to return.
             * @return The positiveCounts at the given index.
             */
            public double getPositiveCounts(int index) {
                return positiveCounts_.getDouble(index);
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double positive_counts = 13;</code>
             *
             * @param index The index to set the value at.
             * @param value The positiveCounts to set.
             * @return This builder for chaining.
             */
            public Builder setPositiveCounts(int index, double value) {

                ensurePositiveCountsIsMutable();
                positiveCounts_.setDouble(index, value);
                bitField0_ |= 0x00001000;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double positive_counts = 13;</code>
             *
             * @param value The positiveCounts to add.
             * @return This builder for chaining.
             */
            public Builder addPositiveCounts(double value) {

                ensurePositiveCountsIsMutable();
                positiveCounts_.addDouble(value);
                bitField0_ |= 0x00001000;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double positive_counts = 13;</code>
             *
             * @param values The positiveCounts to add.
             * @return This builder for chaining.
             */
            public Builder addAllPositiveCounts(Iterable<? extends Double> values) {
                ensurePositiveCountsIsMutable();
                com.google.protobuf.AbstractMessageLite.Builder.addAll(values, positiveCounts_);
                bitField0_ |= 0x00001000;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Absolute count of each bucket.
             * </pre>
             *
             * <code>repeated double positive_counts = 13;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearPositiveCounts() {
                positiveCounts_ = emptyDoubleList();
                bitField0_ = (bitField0_ & ~0x00001000);
                onChanged();
                return this;
            }

            private int resetHint_ = 0;

            /**
             * <code>.prometheus.Histogram.ResetHint reset_hint = 14;</code>
             *
             * @return The enum numeric value on the wire for resetHint.
             */
            @Override
            public int getResetHintValue() {
                return resetHint_;
            }

            /**
             * <code>.prometheus.Histogram.ResetHint reset_hint = 14;</code>
             *
             * @param value The enum numeric value on the wire for resetHint to set.
             * @return This builder for chaining.
             */
            public Builder setResetHintValue(int value) {
                resetHint_ = value;
                bitField0_ |= 0x00002000;
                onChanged();
                return this;
            }

            /**
             * <code>.prometheus.Histogram.ResetHint reset_hint = 14;</code>
             *
             * @return The resetHint.
             */
            @Override
            public Types.Histogram.ResetHint getResetHint() {
                Types.Histogram.ResetHint result = Types.Histogram.ResetHint.forNumber(resetHint_);
                return result == null ? Types.Histogram.ResetHint.UNRECOGNIZED : result;
            }

            /**
             * <code>.prometheus.Histogram.ResetHint reset_hint = 14;</code>
             *
             * @param value The resetHint to set.
             * @return This builder for chaining.
             */
            public Builder setResetHint(Types.Histogram.ResetHint value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                bitField0_ |= 0x00002000;
                resetHint_ = value.getNumber();
                onChanged();
                return this;
            }

            /**
             * <code>.prometheus.Histogram.ResetHint reset_hint = 14;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearResetHint() {
                bitField0_ = (bitField0_ & ~0x00002000);
                resetHint_ = 0;
                onChanged();
                return this;
            }

            private long timestamp_;

            /**
             *
             *
             * <pre>
             * timestamp is in ms format, see model/timestamp/timestamp.go for
             * conversion from time.Time to Prometheus timestamp.
             * </pre>
             *
             * <code>int64 timestamp = 15;</code>
             *
             * @return The timestamp.
             */
            @Override
            public long getTimestamp() {
                return timestamp_;
            }

            /**
             *
             *
             * <pre>
             * timestamp is in ms format, see model/timestamp/timestamp.go for
             * conversion from time.Time to Prometheus timestamp.
             * </pre>
             *
             * <code>int64 timestamp = 15;</code>
             *
             * @param value The timestamp to set.
             * @return This builder for chaining.
             */
            public Builder setTimestamp(long value) {

                timestamp_ = value;
                bitField0_ |= 0x00004000;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * timestamp is in ms format, see model/timestamp/timestamp.go for
             * conversion from time.Time to Prometheus timestamp.
             * </pre>
             *
             * <code>int64 timestamp = 15;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearTimestamp() {
                bitField0_ = (bitField0_ & ~0x00004000);
                timestamp_ = 0L;
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

            // @@protoc_insertion_point(builder_scope:prometheus.Histogram)
        }

        // @@protoc_insertion_point(class_scope:prometheus.Histogram)
        private static final Types.Histogram DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Types.Histogram();
        }

        public static Types.Histogram getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<Histogram> PARSER =
                new com.google.protobuf.AbstractParser<Histogram>() {
                    @Override
                    public Histogram parsePartialFrom(
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

        public static com.google.protobuf.Parser<Histogram> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<Histogram> getParserForType() {
            return PARSER;
        }

        @Override
        public Types.Histogram getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface BucketSpanOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.BucketSpan)
            com.google.protobuf.MessageOrBuilder {

        /**
         *
         *
         * <pre>
         * Gap to previous span, or starting point for 1st span (which can be negative).
         * </pre>
         *
         * <code>sint32 offset = 1;</code>
         *
         * @return The offset.
         */
        int getOffset();

        /**
         *
         *
         * <pre>
         * Length of consecutive buckets.
         * </pre>
         *
         * <code>uint32 length = 2;</code>
         *
         * @return The length.
         */
        int getLength();
    }

    /**
     *
     *
     * <pre>
     * A BucketSpan defines a number of consecutive buckets with their
     * offset. Logically, it would be more straightforward to include the
     * bucket counts in the Span. However, the protobuf representation is
     * more compact in the way the data is structured here (with all the
     * buckets in a single array separate from the Spans).
     * </pre>
     *
     * <p>Protobuf type {@code prometheus.BucketSpan}
     */
    public static final class BucketSpan extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.BucketSpan)
            BucketSpanOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use BucketSpan.newBuilder() to construct.
        private BucketSpan(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private BucketSpan() {}

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new BucketSpan();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Types.internal_static_prometheus_BucketSpan_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Types.internal_static_prometheus_BucketSpan_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Types.BucketSpan.class, Types.BucketSpan.Builder.class);
        }

        public static final int OFFSET_FIELD_NUMBER = 1;
        private int offset_ = 0;

        /**
         *
         *
         * <pre>
         * Gap to previous span, or starting point for 1st span (which can be negative).
         * </pre>
         *
         * <code>sint32 offset = 1;</code>
         *
         * @return The offset.
         */
        @Override
        public int getOffset() {
            return offset_;
        }

        public static final int LENGTH_FIELD_NUMBER = 2;
        private int length_ = 0;

        /**
         *
         *
         * <pre>
         * Length of consecutive buckets.
         * </pre>
         *
         * <code>uint32 length = 2;</code>
         *
         * @return The length.
         */
        @Override
        public int getLength() {
            return length_;
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
            if (offset_ != 0) {
                output.writeSInt32(1, offset_);
            }
            if (length_ != 0) {
                output.writeUInt32(2, length_);
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
            if (offset_ != 0) {
                size += com.google.protobuf.CodedOutputStream.computeSInt32Size(1, offset_);
            }
            if (length_ != 0) {
                size += com.google.protobuf.CodedOutputStream.computeUInt32Size(2, length_);
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
            if (!(obj instanceof Types.BucketSpan)) {
                return super.equals(obj);
            }
            Types.BucketSpan other = (Types.BucketSpan) obj;

            if (getOffset() != other.getOffset()) {
                return false;
            }
            if (getLength() != other.getLength()) {
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
            hash = (37 * hash) + OFFSET_FIELD_NUMBER;
            hash = (53 * hash) + getOffset();
            hash = (37 * hash) + LENGTH_FIELD_NUMBER;
            hash = (53 * hash) + getLength();
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Types.BucketSpan parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.BucketSpan parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.BucketSpan parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.BucketSpan parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.BucketSpan parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.BucketSpan parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.BucketSpan parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.BucketSpan parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.BucketSpan parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Types.BucketSpan parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.BucketSpan parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.BucketSpan parseFrom(
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

        public static Builder newBuilder(Types.BucketSpan prototype) {
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
         * A BucketSpan defines a number of consecutive buckets with their
         * offset. Logically, it would be more straightforward to include the
         * bucket counts in the Span. However, the protobuf representation is
         * more compact in the way the data is structured here (with all the
         * buckets in a single array separate from the Spans).
         * </pre>
         *
         * <p>Protobuf type {@code prometheus.BucketSpan}
         */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.BucketSpan)
                Types.BucketSpanOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Types.internal_static_prometheus_BucketSpan_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Types.internal_static_prometheus_BucketSpan_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Types.BucketSpan.class, Types.BucketSpan.Builder.class);
            }

            // Construct using Types.BucketSpan.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                offset_ = 0;
                length_ = 0;
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Types.internal_static_prometheus_BucketSpan_descriptor;
            }

            @Override
            public Types.BucketSpan getDefaultInstanceForType() {
                return Types.BucketSpan.getDefaultInstance();
            }

            @Override
            public Types.BucketSpan build() {
                Types.BucketSpan result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Types.BucketSpan buildPartial() {
                Types.BucketSpan result = new Types.BucketSpan(this);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartial0(Types.BucketSpan result) {
                int from_bitField0_ = bitField0_;
                if (((from_bitField0_ & 0x00000001) != 0)) {
                    result.offset_ = offset_;
                }
                if (((from_bitField0_ & 0x00000002) != 0)) {
                    result.length_ = length_;
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
                if (other instanceof Types.BucketSpan) {
                    return mergeFrom((Types.BucketSpan) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Types.BucketSpan other) {
                if (other == Types.BucketSpan.getDefaultInstance()) {
                    return this;
                }
                if (other.getOffset() != 0) {
                    setOffset(other.getOffset());
                }
                if (other.getLength() != 0) {
                    setLength(other.getLength());
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
                                    offset_ = input.readSInt32();
                                    bitField0_ |= 0x00000001;
                                    break;
                                } // case 8
                            case 16:
                                {
                                    length_ = input.readUInt32();
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

            private int offset_;

            /**
             *
             *
             * <pre>
             * Gap to previous span, or starting point for 1st span (which can be negative).
             * </pre>
             *
             * <code>sint32 offset = 1;</code>
             *
             * @return The offset.
             */
            @Override
            public int getOffset() {
                return offset_;
            }

            /**
             *
             *
             * <pre>
             * Gap to previous span, or starting point for 1st span (which can be negative).
             * </pre>
             *
             * <code>sint32 offset = 1;</code>
             *
             * @param value The offset to set.
             * @return This builder for chaining.
             */
            public Builder setOffset(int value) {

                offset_ = value;
                bitField0_ |= 0x00000001;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Gap to previous span, or starting point for 1st span (which can be negative).
             * </pre>
             *
             * <code>sint32 offset = 1;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearOffset() {
                bitField0_ = (bitField0_ & ~0x00000001);
                offset_ = 0;
                onChanged();
                return this;
            }

            private int length_;

            /**
             *
             *
             * <pre>
             * Length of consecutive buckets.
             * </pre>
             *
             * <code>uint32 length = 2;</code>
             *
             * @return The length.
             */
            @Override
            public int getLength() {
                return length_;
            }

            /**
             *
             *
             * <pre>
             * Length of consecutive buckets.
             * </pre>
             *
             * <code>uint32 length = 2;</code>
             *
             * @param value The length to set.
             * @return This builder for chaining.
             */
            public Builder setLength(int value) {

                length_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Length of consecutive buckets.
             * </pre>
             *
             * <code>uint32 length = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearLength() {
                bitField0_ = (bitField0_ & ~0x00000002);
                length_ = 0;
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

            // @@protoc_insertion_point(builder_scope:prometheus.BucketSpan)
        }

        // @@protoc_insertion_point(class_scope:prometheus.BucketSpan)
        private static final Types.BucketSpan DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Types.BucketSpan();
        }

        public static Types.BucketSpan getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<BucketSpan> PARSER =
                new com.google.protobuf.AbstractParser<BucketSpan>() {
                    @Override
                    public BucketSpan parsePartialFrom(
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

        public static com.google.protobuf.Parser<BucketSpan> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<BucketSpan> getParserForType() {
            return PARSER;
        }

        @Override
        public Types.BucketSpan getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface TimeSeriesOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.TimeSeries)
            com.google.protobuf.MessageOrBuilder {

        /**
         *
         *
         * <pre>
         * For a timeseries to be valid, and for the samples and exemplars
         * to be ingested by the remote system properly, the labels field is required.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        java.util.List<Types.Label> getLabelsList();

        /**
         *
         *
         * <pre>
         * For a timeseries to be valid, and for the samples and exemplars
         * to be ingested by the remote system properly, the labels field is required.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        Types.Label getLabels(int index);

        /**
         *
         *
         * <pre>
         * For a timeseries to be valid, and for the samples and exemplars
         * to be ingested by the remote system properly, the labels field is required.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        int getLabelsCount();

        /**
         *
         *
         * <pre>
         * For a timeseries to be valid, and for the samples and exemplars
         * to be ingested by the remote system properly, the labels field is required.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        java.util.List<? extends Types.LabelOrBuilder> getLabelsOrBuilderList();

        /**
         *
         *
         * <pre>
         * For a timeseries to be valid, and for the samples and exemplars
         * to be ingested by the remote system properly, the labels field is required.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        Types.LabelOrBuilder getLabelsOrBuilder(int index);

        /** <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code> */
        java.util.List<Types.Sample> getSamplesList();

        /** <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code> */
        Types.Sample getSamples(int index);

        /** <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code> */
        int getSamplesCount();

        /** <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code> */
        java.util.List<? extends Types.SampleOrBuilder> getSamplesOrBuilderList();

        /** <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code> */
        Types.SampleOrBuilder getSamplesOrBuilder(int index);

        /**
         * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];</code>
         */
        java.util.List<Types.Exemplar> getExemplarsList();

        /**
         * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];</code>
         */
        Types.Exemplar getExemplars(int index);

        /**
         * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];</code>
         */
        int getExemplarsCount();

        /**
         * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];</code>
         */
        java.util.List<? extends Types.ExemplarOrBuilder> getExemplarsOrBuilderList();

        /**
         * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];</code>
         */
        Types.ExemplarOrBuilder getExemplarsOrBuilder(int index);

        /**
         * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
         * </code>
         */
        java.util.List<Types.Histogram> getHistogramsList();

        /**
         * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
         * </code>
         */
        Types.Histogram getHistograms(int index);

        /**
         * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
         * </code>
         */
        int getHistogramsCount();

        /**
         * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
         * </code>
         */
        java.util.List<? extends Types.HistogramOrBuilder> getHistogramsOrBuilderList();

        /**
         * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
         * </code>
         */
        Types.HistogramOrBuilder getHistogramsOrBuilder(int index);
    }

    /**
     *
     *
     * <pre>
     * TimeSeries represents samples and labels for a single time series.
     * </pre>
     *
     * <p>Protobuf type {@code prometheus.TimeSeries}
     */
    public static final class TimeSeries extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.TimeSeries)
            TimeSeriesOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use TimeSeries.newBuilder() to construct.
        private TimeSeries(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private TimeSeries() {
            labels_ = java.util.Collections.emptyList();
            samples_ = java.util.Collections.emptyList();
            exemplars_ = java.util.Collections.emptyList();
            histograms_ = java.util.Collections.emptyList();
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new TimeSeries();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Types.internal_static_prometheus_TimeSeries_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Types.internal_static_prometheus_TimeSeries_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Types.TimeSeries.class, Types.TimeSeries.Builder.class);
        }

        public static final int LABELS_FIELD_NUMBER = 1;

        @SuppressWarnings("serial")
        private java.util.List<Types.Label> labels_;

        /**
         *
         *
         * <pre>
         * For a timeseries to be valid, and for the samples and exemplars
         * to be ingested by the remote system properly, the labels field is required.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public java.util.List<Types.Label> getLabelsList() {
            return labels_;
        }

        /**
         *
         *
         * <pre>
         * For a timeseries to be valid, and for the samples and exemplars
         * to be ingested by the remote system properly, the labels field is required.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public java.util.List<? extends Types.LabelOrBuilder> getLabelsOrBuilderList() {
            return labels_;
        }

        /**
         *
         *
         * <pre>
         * For a timeseries to be valid, and for the samples and exemplars
         * to be ingested by the remote system properly, the labels field is required.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public int getLabelsCount() {
            return labels_.size();
        }

        /**
         *
         *
         * <pre>
         * For a timeseries to be valid, and for the samples and exemplars
         * to be ingested by the remote system properly, the labels field is required.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public Types.Label getLabels(int index) {
            return labels_.get(index);
        }

        /**
         *
         *
         * <pre>
         * For a timeseries to be valid, and for the samples and exemplars
         * to be ingested by the remote system properly, the labels field is required.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public Types.LabelOrBuilder getLabelsOrBuilder(int index) {
            return labels_.get(index);
        }

        public static final int SAMPLES_FIELD_NUMBER = 2;

        @SuppressWarnings("serial")
        private java.util.List<Types.Sample> samples_;

        /** <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code> */
        @Override
        public java.util.List<Types.Sample> getSamplesList() {
            return samples_;
        }

        /** <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code> */
        @Override
        public java.util.List<? extends Types.SampleOrBuilder> getSamplesOrBuilderList() {
            return samples_;
        }

        /** <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code> */
        @Override
        public int getSamplesCount() {
            return samples_.size();
        }

        /** <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code> */
        @Override
        public Types.Sample getSamples(int index) {
            return samples_.get(index);
        }

        /** <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code> */
        @Override
        public Types.SampleOrBuilder getSamplesOrBuilder(int index) {
            return samples_.get(index);
        }

        public static final int EXEMPLARS_FIELD_NUMBER = 3;

        @SuppressWarnings("serial")
        private java.util.List<Types.Exemplar> exemplars_;

        /**
         * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public java.util.List<Types.Exemplar> getExemplarsList() {
            return exemplars_;
        }

        /**
         * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public java.util.List<? extends Types.ExemplarOrBuilder> getExemplarsOrBuilderList() {
            return exemplars_;
        }

        /**
         * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public int getExemplarsCount() {
            return exemplars_.size();
        }

        /**
         * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public Types.Exemplar getExemplars(int index) {
            return exemplars_.get(index);
        }

        /**
         * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public Types.ExemplarOrBuilder getExemplarsOrBuilder(int index) {
            return exemplars_.get(index);
        }

        public static final int HISTOGRAMS_FIELD_NUMBER = 4;

        @SuppressWarnings("serial")
        private java.util.List<Types.Histogram> histograms_;

        /**
         * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public java.util.List<Types.Histogram> getHistogramsList() {
            return histograms_;
        }

        /**
         * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public java.util.List<? extends Types.HistogramOrBuilder> getHistogramsOrBuilderList() {
            return histograms_;
        }

        /**
         * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public int getHistogramsCount() {
            return histograms_.size();
        }

        /**
         * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public Types.Histogram getHistograms(int index) {
            return histograms_.get(index);
        }

        /**
         * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
         * </code>
         */
        @Override
        public Types.HistogramOrBuilder getHistogramsOrBuilder(int index) {
            return histograms_.get(index);
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
            for (int i = 0; i < labels_.size(); i++) {
                output.writeMessage(1, labels_.get(i));
            }
            for (int i = 0; i < samples_.size(); i++) {
                output.writeMessage(2, samples_.get(i));
            }
            for (int i = 0; i < exemplars_.size(); i++) {
                output.writeMessage(3, exemplars_.get(i));
            }
            for (int i = 0; i < histograms_.size(); i++) {
                output.writeMessage(4, histograms_.get(i));
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
            for (int i = 0; i < labels_.size(); i++) {
                size += com.google.protobuf.CodedOutputStream.computeMessageSize(1, labels_.get(i));
            }
            for (int i = 0; i < samples_.size(); i++) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeMessageSize(
                                2, samples_.get(i));
            }
            for (int i = 0; i < exemplars_.size(); i++) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeMessageSize(
                                3, exemplars_.get(i));
            }
            for (int i = 0; i < histograms_.size(); i++) {
                size +=
                        com.google.protobuf.CodedOutputStream.computeMessageSize(
                                4, histograms_.get(i));
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
            if (!(obj instanceof Types.TimeSeries)) {
                return super.equals(obj);
            }
            Types.TimeSeries other = (Types.TimeSeries) obj;

            if (!getLabelsList().equals(other.getLabelsList())) {
                return false;
            }
            if (!getSamplesList().equals(other.getSamplesList())) {
                return false;
            }
            if (!getExemplarsList().equals(other.getExemplarsList())) {
                return false;
            }
            if (!getHistogramsList().equals(other.getHistogramsList())) {
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
            if (getLabelsCount() > 0) {
                hash = (37 * hash) + LABELS_FIELD_NUMBER;
                hash = (53 * hash) + getLabelsList().hashCode();
            }
            if (getSamplesCount() > 0) {
                hash = (37 * hash) + SAMPLES_FIELD_NUMBER;
                hash = (53 * hash) + getSamplesList().hashCode();
            }
            if (getExemplarsCount() > 0) {
                hash = (37 * hash) + EXEMPLARS_FIELD_NUMBER;
                hash = (53 * hash) + getExemplarsList().hashCode();
            }
            if (getHistogramsCount() > 0) {
                hash = (37 * hash) + HISTOGRAMS_FIELD_NUMBER;
                hash = (53 * hash) + getHistogramsList().hashCode();
            }
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Types.TimeSeries parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.TimeSeries parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.TimeSeries parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.TimeSeries parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.TimeSeries parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.TimeSeries parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.TimeSeries parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.TimeSeries parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.TimeSeries parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Types.TimeSeries parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.TimeSeries parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.TimeSeries parseFrom(
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

        public static Builder newBuilder(Types.TimeSeries prototype) {
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
         * TimeSeries represents samples and labels for a single time series.
         * </pre>
         *
         * <p>Protobuf type {@code prometheus.TimeSeries}
         */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.TimeSeries)
                Types.TimeSeriesOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Types.internal_static_prometheus_TimeSeries_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Types.internal_static_prometheus_TimeSeries_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Types.TimeSeries.class, Types.TimeSeries.Builder.class);
            }

            // Construct using Types.TimeSeries.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                if (labelsBuilder_ == null) {
                    labels_ = java.util.Collections.emptyList();
                } else {
                    labels_ = null;
                    labelsBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000001);
                if (samplesBuilder_ == null) {
                    samples_ = java.util.Collections.emptyList();
                } else {
                    samples_ = null;
                    samplesBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000002);
                if (exemplarsBuilder_ == null) {
                    exemplars_ = java.util.Collections.emptyList();
                } else {
                    exemplars_ = null;
                    exemplarsBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000004);
                if (histogramsBuilder_ == null) {
                    histograms_ = java.util.Collections.emptyList();
                } else {
                    histograms_ = null;
                    histogramsBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000008);
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Types.internal_static_prometheus_TimeSeries_descriptor;
            }

            @Override
            public Types.TimeSeries getDefaultInstanceForType() {
                return Types.TimeSeries.getDefaultInstance();
            }

            @Override
            public Types.TimeSeries build() {
                Types.TimeSeries result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Types.TimeSeries buildPartial() {
                Types.TimeSeries result = new Types.TimeSeries(this);
                buildPartialRepeatedFields(result);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartialRepeatedFields(Types.TimeSeries result) {
                if (labelsBuilder_ == null) {
                    if (((bitField0_ & 0x00000001) != 0)) {
                        labels_ = java.util.Collections.unmodifiableList(labels_);
                        bitField0_ = (bitField0_ & ~0x00000001);
                    }
                    result.labels_ = labels_;
                } else {
                    result.labels_ = labelsBuilder_.build();
                }
                if (samplesBuilder_ == null) {
                    if (((bitField0_ & 0x00000002) != 0)) {
                        samples_ = java.util.Collections.unmodifiableList(samples_);
                        bitField0_ = (bitField0_ & ~0x00000002);
                    }
                    result.samples_ = samples_;
                } else {
                    result.samples_ = samplesBuilder_.build();
                }
                if (exemplarsBuilder_ == null) {
                    if (((bitField0_ & 0x00000004) != 0)) {
                        exemplars_ = java.util.Collections.unmodifiableList(exemplars_);
                        bitField0_ = (bitField0_ & ~0x00000004);
                    }
                    result.exemplars_ = exemplars_;
                } else {
                    result.exemplars_ = exemplarsBuilder_.build();
                }
                if (histogramsBuilder_ == null) {
                    if (((bitField0_ & 0x00000008) != 0)) {
                        histograms_ = java.util.Collections.unmodifiableList(histograms_);
                        bitField0_ = (bitField0_ & ~0x00000008);
                    }
                    result.histograms_ = histograms_;
                } else {
                    result.histograms_ = histogramsBuilder_.build();
                }
            }

            private void buildPartial0(Types.TimeSeries result) {
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
                if (other instanceof Types.TimeSeries) {
                    return mergeFrom((Types.TimeSeries) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Types.TimeSeries other) {
                if (other == Types.TimeSeries.getDefaultInstance()) {
                    return this;
                }
                if (labelsBuilder_ == null) {
                    if (!other.labels_.isEmpty()) {
                        if (labels_.isEmpty()) {
                            labels_ = other.labels_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                        } else {
                            ensureLabelsIsMutable();
                            labels_.addAll(other.labels_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.labels_.isEmpty()) {
                        if (labelsBuilder_.isEmpty()) {
                            labelsBuilder_.dispose();
                            labelsBuilder_ = null;
                            labels_ = other.labels_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                            labelsBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getLabelsFieldBuilder()
                                            : null;
                        } else {
                            labelsBuilder_.addAllMessages(other.labels_);
                        }
                    }
                }
                if (samplesBuilder_ == null) {
                    if (!other.samples_.isEmpty()) {
                        if (samples_.isEmpty()) {
                            samples_ = other.samples_;
                            bitField0_ = (bitField0_ & ~0x00000002);
                        } else {
                            ensureSamplesIsMutable();
                            samples_.addAll(other.samples_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.samples_.isEmpty()) {
                        if (samplesBuilder_.isEmpty()) {
                            samplesBuilder_.dispose();
                            samplesBuilder_ = null;
                            samples_ = other.samples_;
                            bitField0_ = (bitField0_ & ~0x00000002);
                            samplesBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getSamplesFieldBuilder()
                                            : null;
                        } else {
                            samplesBuilder_.addAllMessages(other.samples_);
                        }
                    }
                }
                if (exemplarsBuilder_ == null) {
                    if (!other.exemplars_.isEmpty()) {
                        if (exemplars_.isEmpty()) {
                            exemplars_ = other.exemplars_;
                            bitField0_ = (bitField0_ & ~0x00000004);
                        } else {
                            ensureExemplarsIsMutable();
                            exemplars_.addAll(other.exemplars_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.exemplars_.isEmpty()) {
                        if (exemplarsBuilder_.isEmpty()) {
                            exemplarsBuilder_.dispose();
                            exemplarsBuilder_ = null;
                            exemplars_ = other.exemplars_;
                            bitField0_ = (bitField0_ & ~0x00000004);
                            exemplarsBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getExemplarsFieldBuilder()
                                            : null;
                        } else {
                            exemplarsBuilder_.addAllMessages(other.exemplars_);
                        }
                    }
                }
                if (histogramsBuilder_ == null) {
                    if (!other.histograms_.isEmpty()) {
                        if (histograms_.isEmpty()) {
                            histograms_ = other.histograms_;
                            bitField0_ = (bitField0_ & ~0x00000008);
                        } else {
                            ensureHistogramsIsMutable();
                            histograms_.addAll(other.histograms_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.histograms_.isEmpty()) {
                        if (histogramsBuilder_.isEmpty()) {
                            histogramsBuilder_.dispose();
                            histogramsBuilder_ = null;
                            histograms_ = other.histograms_;
                            bitField0_ = (bitField0_ & ~0x00000008);
                            histogramsBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getHistogramsFieldBuilder()
                                            : null;
                        } else {
                            histogramsBuilder_.addAllMessages(other.histograms_);
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
                                    Types.Label m =
                                            input.readMessage(
                                                    Types.Label.parser(), extensionRegistry);
                                    if (labelsBuilder_ == null) {
                                        ensureLabelsIsMutable();
                                        labels_.add(m);
                                    } else {
                                        labelsBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 10
                            case 18:
                                {
                                    Types.Sample m =
                                            input.readMessage(
                                                    Types.Sample.parser(), extensionRegistry);
                                    if (samplesBuilder_ == null) {
                                        ensureSamplesIsMutable();
                                        samples_.add(m);
                                    } else {
                                        samplesBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 18
                            case 26:
                                {
                                    Types.Exemplar m =
                                            input.readMessage(
                                                    Types.Exemplar.parser(), extensionRegistry);
                                    if (exemplarsBuilder_ == null) {
                                        ensureExemplarsIsMutable();
                                        exemplars_.add(m);
                                    } else {
                                        exemplarsBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 26
                            case 34:
                                {
                                    Types.Histogram m =
                                            input.readMessage(
                                                    Types.Histogram.parser(), extensionRegistry);
                                    if (histogramsBuilder_ == null) {
                                        ensureHistogramsIsMutable();
                                        histograms_.add(m);
                                    } else {
                                        histogramsBuilder_.addMessage(m);
                                    }
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

            private java.util.List<Types.Label> labels_ = java.util.Collections.emptyList();

            private void ensureLabelsIsMutable() {
                if (!((bitField0_ & 0x00000001) != 0)) {
                    labels_ = new java.util.ArrayList<Types.Label>(labels_);
                    bitField0_ |= 0x00000001;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Label, Types.Label.Builder, Types.LabelOrBuilder>
                    labelsBuilder_;

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<Types.Label> getLabelsList() {
                if (labelsBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(labels_);
                } else {
                    return labelsBuilder_.getMessageList();
                }
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public int getLabelsCount() {
                if (labelsBuilder_ == null) {
                    return labels_.size();
                } else {
                    return labelsBuilder_.getCount();
                }
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label getLabels(int index) {
                if (labelsBuilder_ == null) {
                    return labels_.get(index);
                } else {
                    return labelsBuilder_.getMessage(index);
                }
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder setLabels(int index, Types.Label value) {
                if (labelsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureLabelsIsMutable();
                    labels_.set(index, value);
                    onChanged();
                } else {
                    labelsBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder setLabels(int index, Types.Label.Builder builderForValue) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    labelsBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(Types.Label value) {
                if (labelsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureLabelsIsMutable();
                    labels_.add(value);
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(int index, Types.Label value) {
                if (labelsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureLabelsIsMutable();
                    labels_.add(index, value);
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(Types.Label.Builder builderForValue) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.add(builderForValue.build());
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(int index, Types.Label.Builder builderForValue) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addAllLabels(Iterable<? extends Types.Label> values) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, labels_);
                    onChanged();
                } else {
                    labelsBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder clearLabels() {
                if (labelsBuilder_ == null) {
                    labels_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000001);
                    onChanged();
                } else {
                    labelsBuilder_.clear();
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder removeLabels(int index) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.remove(index);
                    onChanged();
                } else {
                    labelsBuilder_.remove(index);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label.Builder getLabelsBuilder(int index) {
                return getLabelsFieldBuilder().getBuilder(index);
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.LabelOrBuilder getLabelsOrBuilder(int index) {
                if (labelsBuilder_ == null) {
                    return labels_.get(index);
                } else {
                    return labelsBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<? extends Types.LabelOrBuilder> getLabelsOrBuilderList() {
                if (labelsBuilder_ != null) {
                    return labelsBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(labels_);
                }
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label.Builder addLabelsBuilder() {
                return getLabelsFieldBuilder().addBuilder(Types.Label.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label.Builder addLabelsBuilder(int index) {
                return getLabelsFieldBuilder().addBuilder(index, Types.Label.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * For a timeseries to be valid, and for the samples and exemplars
             * to be ingested by the remote system properly, the labels field is required.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<Types.Label.Builder> getLabelsBuilderList() {
                return getLabelsFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Label, Types.Label.Builder, Types.LabelOrBuilder>
                    getLabelsFieldBuilder() {
                if (labelsBuilder_ == null) {
                    labelsBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.Label, Types.Label.Builder, Types.LabelOrBuilder>(
                                    labels_,
                                    ((bitField0_ & 0x00000001) != 0),
                                    getParentForChildren(),
                                    isClean());
                    labels_ = null;
                }
                return labelsBuilder_;
            }

            private java.util.List<Types.Sample> samples_ = java.util.Collections.emptyList();

            private void ensureSamplesIsMutable() {
                if (!((bitField0_ & 0x00000002) != 0)) {
                    samples_ = new java.util.ArrayList<Types.Sample>(samples_);
                    bitField0_ |= 0x00000002;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Sample, Types.Sample.Builder, Types.SampleOrBuilder>
                    samplesBuilder_;

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<Types.Sample> getSamplesList() {
                if (samplesBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(samples_);
                } else {
                    return samplesBuilder_.getMessageList();
                }
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public int getSamplesCount() {
                if (samplesBuilder_ == null) {
                    return samples_.size();
                } else {
                    return samplesBuilder_.getCount();
                }
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Sample getSamples(int index) {
                if (samplesBuilder_ == null) {
                    return samples_.get(index);
                } else {
                    return samplesBuilder_.getMessage(index);
                }
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder setSamples(int index, Types.Sample value) {
                if (samplesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureSamplesIsMutable();
                    samples_.set(index, value);
                    onChanged();
                } else {
                    samplesBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder setSamples(int index, Types.Sample.Builder builderForValue) {
                if (samplesBuilder_ == null) {
                    ensureSamplesIsMutable();
                    samples_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    samplesBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addSamples(Types.Sample value) {
                if (samplesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureSamplesIsMutable();
                    samples_.add(value);
                    onChanged();
                } else {
                    samplesBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addSamples(int index, Types.Sample value) {
                if (samplesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureSamplesIsMutable();
                    samples_.add(index, value);
                    onChanged();
                } else {
                    samplesBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addSamples(Types.Sample.Builder builderForValue) {
                if (samplesBuilder_ == null) {
                    ensureSamplesIsMutable();
                    samples_.add(builderForValue.build());
                    onChanged();
                } else {
                    samplesBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addSamples(int index, Types.Sample.Builder builderForValue) {
                if (samplesBuilder_ == null) {
                    ensureSamplesIsMutable();
                    samples_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    samplesBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addAllSamples(Iterable<? extends Types.Sample> values) {
                if (samplesBuilder_ == null) {
                    ensureSamplesIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, samples_);
                    onChanged();
                } else {
                    samplesBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder clearSamples() {
                if (samplesBuilder_ == null) {
                    samples_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000002);
                    onChanged();
                } else {
                    samplesBuilder_.clear();
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder removeSamples(int index) {
                if (samplesBuilder_ == null) {
                    ensureSamplesIsMutable();
                    samples_.remove(index);
                    onChanged();
                } else {
                    samplesBuilder_.remove(index);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Sample.Builder getSamplesBuilder(int index) {
                return getSamplesFieldBuilder().getBuilder(index);
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Types.SampleOrBuilder getSamplesOrBuilder(int index) {
                if (samplesBuilder_ == null) {
                    return samples_.get(index);
                } else {
                    return samplesBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<? extends Types.SampleOrBuilder> getSamplesOrBuilderList() {
                if (samplesBuilder_ != null) {
                    return samplesBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(samples_);
                }
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Sample.Builder addSamplesBuilder() {
                return getSamplesFieldBuilder().addBuilder(Types.Sample.getDefaultInstance());
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Sample.Builder addSamplesBuilder(int index) {
                return getSamplesFieldBuilder()
                        .addBuilder(index, Types.Sample.getDefaultInstance());
            }

            /**
             * <code>repeated .prometheus.Sample samples = 2 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<Types.Sample.Builder> getSamplesBuilderList() {
                return getSamplesFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Sample, Types.Sample.Builder, Types.SampleOrBuilder>
                    getSamplesFieldBuilder() {
                if (samplesBuilder_ == null) {
                    samplesBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.Sample, Types.Sample.Builder, Types.SampleOrBuilder>(
                                    samples_,
                                    ((bitField0_ & 0x00000002) != 0),
                                    getParentForChildren(),
                                    isClean());
                    samples_ = null;
                }
                return samplesBuilder_;
            }

            private java.util.List<Types.Exemplar> exemplars_ = java.util.Collections.emptyList();

            private void ensureExemplarsIsMutable() {
                if (!((bitField0_ & 0x00000004) != 0)) {
                    exemplars_ = new java.util.ArrayList<Types.Exemplar>(exemplars_);
                    bitField0_ |= 0x00000004;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Exemplar, Types.Exemplar.Builder, Types.ExemplarOrBuilder>
                    exemplarsBuilder_;

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<Types.Exemplar> getExemplarsList() {
                if (exemplarsBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(exemplars_);
                } else {
                    return exemplarsBuilder_.getMessageList();
                }
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public int getExemplarsCount() {
                if (exemplarsBuilder_ == null) {
                    return exemplars_.size();
                } else {
                    return exemplarsBuilder_.getCount();
                }
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.Exemplar getExemplars(int index) {
                if (exemplarsBuilder_ == null) {
                    return exemplars_.get(index);
                } else {
                    return exemplarsBuilder_.getMessage(index);
                }
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder setExemplars(int index, Types.Exemplar value) {
                if (exemplarsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureExemplarsIsMutable();
                    exemplars_.set(index, value);
                    onChanged();
                } else {
                    exemplarsBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder setExemplars(int index, Types.Exemplar.Builder builderForValue) {
                if (exemplarsBuilder_ == null) {
                    ensureExemplarsIsMutable();
                    exemplars_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    exemplarsBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addExemplars(Types.Exemplar value) {
                if (exemplarsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureExemplarsIsMutable();
                    exemplars_.add(value);
                    onChanged();
                } else {
                    exemplarsBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addExemplars(int index, Types.Exemplar value) {
                if (exemplarsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureExemplarsIsMutable();
                    exemplars_.add(index, value);
                    onChanged();
                } else {
                    exemplarsBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addExemplars(Types.Exemplar.Builder builderForValue) {
                if (exemplarsBuilder_ == null) {
                    ensureExemplarsIsMutable();
                    exemplars_.add(builderForValue.build());
                    onChanged();
                } else {
                    exemplarsBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addExemplars(int index, Types.Exemplar.Builder builderForValue) {
                if (exemplarsBuilder_ == null) {
                    ensureExemplarsIsMutable();
                    exemplars_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    exemplarsBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addAllExemplars(Iterable<? extends Types.Exemplar> values) {
                if (exemplarsBuilder_ == null) {
                    ensureExemplarsIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, exemplars_);
                    onChanged();
                } else {
                    exemplarsBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder clearExemplars() {
                if (exemplarsBuilder_ == null) {
                    exemplars_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000004);
                    onChanged();
                } else {
                    exemplarsBuilder_.clear();
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder removeExemplars(int index) {
                if (exemplarsBuilder_ == null) {
                    ensureExemplarsIsMutable();
                    exemplars_.remove(index);
                    onChanged();
                } else {
                    exemplarsBuilder_.remove(index);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.Exemplar.Builder getExemplarsBuilder(int index) {
                return getExemplarsFieldBuilder().getBuilder(index);
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.ExemplarOrBuilder getExemplarsOrBuilder(int index) {
                if (exemplarsBuilder_ == null) {
                    return exemplars_.get(index);
                } else {
                    return exemplarsBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<? extends Types.ExemplarOrBuilder> getExemplarsOrBuilderList() {
                if (exemplarsBuilder_ != null) {
                    return exemplarsBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(exemplars_);
                }
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.Exemplar.Builder addExemplarsBuilder() {
                return getExemplarsFieldBuilder().addBuilder(Types.Exemplar.getDefaultInstance());
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.Exemplar.Builder addExemplarsBuilder(int index) {
                return getExemplarsFieldBuilder()
                        .addBuilder(index, Types.Exemplar.getDefaultInstance());
            }

            /**
             * <code>repeated .prometheus.Exemplar exemplars = 3 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<Types.Exemplar.Builder> getExemplarsBuilderList() {
                return getExemplarsFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Exemplar, Types.Exemplar.Builder, Types.ExemplarOrBuilder>
                    getExemplarsFieldBuilder() {
                if (exemplarsBuilder_ == null) {
                    exemplarsBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.Exemplar,
                                    Types.Exemplar.Builder,
                                    Types.ExemplarOrBuilder>(
                                    exemplars_,
                                    ((bitField0_ & 0x00000004) != 0),
                                    getParentForChildren(),
                                    isClean());
                    exemplars_ = null;
                }
                return exemplarsBuilder_;
            }

            private java.util.List<Types.Histogram> histograms_ = java.util.Collections.emptyList();

            private void ensureHistogramsIsMutable() {
                if (!((bitField0_ & 0x00000008) != 0)) {
                    histograms_ = new java.util.ArrayList<Types.Histogram>(histograms_);
                    bitField0_ |= 0x00000008;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Histogram, Types.Histogram.Builder, Types.HistogramOrBuilder>
                    histogramsBuilder_;

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<Types.Histogram> getHistogramsList() {
                if (histogramsBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(histograms_);
                } else {
                    return histogramsBuilder_.getMessageList();
                }
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public int getHistogramsCount() {
                if (histogramsBuilder_ == null) {
                    return histograms_.size();
                } else {
                    return histogramsBuilder_.getCount();
                }
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.Histogram getHistograms(int index) {
                if (histogramsBuilder_ == null) {
                    return histograms_.get(index);
                } else {
                    return histogramsBuilder_.getMessage(index);
                }
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder setHistograms(int index, Types.Histogram value) {
                if (histogramsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureHistogramsIsMutable();
                    histograms_.set(index, value);
                    onChanged();
                } else {
                    histogramsBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder setHistograms(int index, Types.Histogram.Builder builderForValue) {
                if (histogramsBuilder_ == null) {
                    ensureHistogramsIsMutable();
                    histograms_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    histogramsBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addHistograms(Types.Histogram value) {
                if (histogramsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureHistogramsIsMutable();
                    histograms_.add(value);
                    onChanged();
                } else {
                    histogramsBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addHistograms(int index, Types.Histogram value) {
                if (histogramsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureHistogramsIsMutable();
                    histograms_.add(index, value);
                    onChanged();
                } else {
                    histogramsBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addHistograms(Types.Histogram.Builder builderForValue) {
                if (histogramsBuilder_ == null) {
                    ensureHistogramsIsMutable();
                    histograms_.add(builderForValue.build());
                    onChanged();
                } else {
                    histogramsBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addHistograms(int index, Types.Histogram.Builder builderForValue) {
                if (histogramsBuilder_ == null) {
                    ensureHistogramsIsMutable();
                    histograms_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    histogramsBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder addAllHistograms(Iterable<? extends Types.Histogram> values) {
                if (histogramsBuilder_ == null) {
                    ensureHistogramsIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, histograms_);
                    onChanged();
                } else {
                    histogramsBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder clearHistograms() {
                if (histogramsBuilder_ == null) {
                    histograms_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000008);
                    onChanged();
                } else {
                    histogramsBuilder_.clear();
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Builder removeHistograms(int index) {
                if (histogramsBuilder_ == null) {
                    ensureHistogramsIsMutable();
                    histograms_.remove(index);
                    onChanged();
                } else {
                    histogramsBuilder_.remove(index);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.Histogram.Builder getHistogramsBuilder(int index) {
                return getHistogramsFieldBuilder().getBuilder(index);
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.HistogramOrBuilder getHistogramsOrBuilder(int index) {
                if (histogramsBuilder_ == null) {
                    return histograms_.get(index);
                } else {
                    return histogramsBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<? extends Types.HistogramOrBuilder> getHistogramsOrBuilderList() {
                if (histogramsBuilder_ != null) {
                    return histogramsBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(histograms_);
                }
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.Histogram.Builder addHistogramsBuilder() {
                return getHistogramsFieldBuilder().addBuilder(Types.Histogram.getDefaultInstance());
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public Types.Histogram.Builder addHistogramsBuilder(int index) {
                return getHistogramsFieldBuilder()
                        .addBuilder(index, Types.Histogram.getDefaultInstance());
            }

            /**
             * <code>repeated .prometheus.Histogram histograms = 4 [(.gogoproto.nullable) = false];
             * </code>
             */
            public java.util.List<Types.Histogram.Builder> getHistogramsBuilderList() {
                return getHistogramsFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Histogram, Types.Histogram.Builder, Types.HistogramOrBuilder>
                    getHistogramsFieldBuilder() {
                if (histogramsBuilder_ == null) {
                    histogramsBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.Histogram,
                                    Types.Histogram.Builder,
                                    Types.HistogramOrBuilder>(
                                    histograms_,
                                    ((bitField0_ & 0x00000008) != 0),
                                    getParentForChildren(),
                                    isClean());
                    histograms_ = null;
                }
                return histogramsBuilder_;
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

            // @@protoc_insertion_point(builder_scope:prometheus.TimeSeries)
        }

        // @@protoc_insertion_point(class_scope:prometheus.TimeSeries)
        private static final Types.TimeSeries DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Types.TimeSeries();
        }

        public static Types.TimeSeries getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<TimeSeries> PARSER =
                new com.google.protobuf.AbstractParser<TimeSeries>() {
                    @Override
                    public TimeSeries parsePartialFrom(
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

        public static com.google.protobuf.Parser<TimeSeries> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<TimeSeries> getParserForType() {
            return PARSER;
        }

        @Override
        public Types.TimeSeries getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface LabelOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.Label)
            com.google.protobuf.MessageOrBuilder {

        /**
         * <code>string name = 1;</code>
         *
         * @return The name.
         */
        String getName();

        /**
         * <code>string name = 1;</code>
         *
         * @return The bytes for name.
         */
        com.google.protobuf.ByteString getNameBytes();

        /**
         * <code>string value = 2;</code>
         *
         * @return The value.
         */
        String getValue();

        /**
         * <code>string value = 2;</code>
         *
         * @return The bytes for value.
         */
        com.google.protobuf.ByteString getValueBytes();
    }

    /** Protobuf type {@code prometheus.Label} */
    public static final class Label extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.Label)
            LabelOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use Label.newBuilder() to construct.
        private Label(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private Label() {
            name_ = "";
            value_ = "";
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new Label();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Types.internal_static_prometheus_Label_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Types.internal_static_prometheus_Label_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(Types.Label.class, Types.Label.Builder.class);
        }

        public static final int NAME_FIELD_NUMBER = 1;

        @SuppressWarnings("serial")
        private volatile Object name_ = "";

        /**
         * <code>string name = 1;</code>
         *
         * @return The name.
         */
        @Override
        public String getName() {
            Object ref = name_;
            if (ref instanceof String) {
                return (String) ref;
            } else {
                com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                name_ = s;
                return s;
            }
        }

        /**
         * <code>string name = 1;</code>
         *
         * @return The bytes for name.
         */
        @Override
        public com.google.protobuf.ByteString getNameBytes() {
            Object ref = name_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                name_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        public static final int VALUE_FIELD_NUMBER = 2;

        @SuppressWarnings("serial")
        private volatile Object value_ = "";

        /**
         * <code>string value = 2;</code>
         *
         * @return The value.
         */
        @Override
        public String getValue() {
            Object ref = value_;
            if (ref instanceof String) {
                return (String) ref;
            } else {
                com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                value_ = s;
                return s;
            }
        }

        /**
         * <code>string value = 2;</code>
         *
         * @return The bytes for value.
         */
        @Override
        public com.google.protobuf.ByteString getValueBytes() {
            Object ref = value_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                value_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
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
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(name_)) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 1, name_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(value_)) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 2, value_);
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
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(name_)) {
                size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, name_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(value_)) {
                size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, value_);
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
            if (!(obj instanceof Types.Label)) {
                return super.equals(obj);
            }
            Types.Label other = (Types.Label) obj;

            if (!getName().equals(other.getName())) {
                return false;
            }
            if (!getValue().equals(other.getValue())) {
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
            hash = (37 * hash) + NAME_FIELD_NUMBER;
            hash = (53 * hash) + getName().hashCode();
            hash = (37 * hash) + VALUE_FIELD_NUMBER;
            hash = (53 * hash) + getValue().hashCode();
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Types.Label parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Label parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Label parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Label parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Label parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Label parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Label parseFrom(java.io.InputStream input) throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.Label parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.Label parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Types.Label parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.Label parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.Label parseFrom(
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

        public static Builder newBuilder(Types.Label prototype) {
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

        /** Protobuf type {@code prometheus.Label} */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.Label)
                Types.LabelOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Types.internal_static_prometheus_Label_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Types.internal_static_prometheus_Label_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Types.Label.class, Types.Label.Builder.class);
            }

            // Construct using Types.Label.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                name_ = "";
                value_ = "";
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Types.internal_static_prometheus_Label_descriptor;
            }

            @Override
            public Types.Label getDefaultInstanceForType() {
                return Types.Label.getDefaultInstance();
            }

            @Override
            public Types.Label build() {
                Types.Label result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Types.Label buildPartial() {
                Types.Label result = new Types.Label(this);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartial0(Types.Label result) {
                int from_bitField0_ = bitField0_;
                if (((from_bitField0_ & 0x00000001) != 0)) {
                    result.name_ = name_;
                }
                if (((from_bitField0_ & 0x00000002) != 0)) {
                    result.value_ = value_;
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
                if (other instanceof Types.Label) {
                    return mergeFrom((Types.Label) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Types.Label other) {
                if (other == Types.Label.getDefaultInstance()) {
                    return this;
                }
                if (!other.getName().isEmpty()) {
                    name_ = other.name_;
                    bitField0_ |= 0x00000001;
                    onChanged();
                }
                if (!other.getValue().isEmpty()) {
                    value_ = other.value_;
                    bitField0_ |= 0x00000002;
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
                                    name_ = input.readStringRequireUtf8();
                                    bitField0_ |= 0x00000001;
                                    break;
                                } // case 10
                            case 18:
                                {
                                    value_ = input.readStringRequireUtf8();
                                    bitField0_ |= 0x00000002;
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

            private Object name_ = "";

            /**
             * <code>string name = 1;</code>
             *
             * @return The name.
             */
            public String getName() {
                Object ref = name_;
                if (!(ref instanceof String)) {
                    com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                    String s = bs.toStringUtf8();
                    name_ = s;
                    return s;
                } else {
                    return (String) ref;
                }
            }

            /**
             * <code>string name = 1;</code>
             *
             * @return The bytes for name.
             */
            public com.google.protobuf.ByteString getNameBytes() {
                Object ref = name_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                    name_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>string name = 1;</code>
             *
             * @param value The name to set.
             * @return This builder for chaining.
             */
            public Builder setName(String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                name_ = value;
                bitField0_ |= 0x00000001;
                onChanged();
                return this;
            }

            /**
             * <code>string name = 1;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearName() {
                name_ = getDefaultInstance().getName();
                bitField0_ = (bitField0_ & ~0x00000001);
                onChanged();
                return this;
            }

            /**
             * <code>string name = 1;</code>
             *
             * @param value The bytes for name to set.
             * @return This builder for chaining.
             */
            public Builder setNameBytes(com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);
                name_ = value;
                bitField0_ |= 0x00000001;
                onChanged();
                return this;
            }

            private Object value_ = "";

            /**
             * <code>string value = 2;</code>
             *
             * @return The value.
             */
            public String getValue() {
                Object ref = value_;
                if (!(ref instanceof String)) {
                    com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                    String s = bs.toStringUtf8();
                    value_ = s;
                    return s;
                } else {
                    return (String) ref;
                }
            }

            /**
             * <code>string value = 2;</code>
             *
             * @return The bytes for value.
             */
            public com.google.protobuf.ByteString getValueBytes() {
                Object ref = value_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                    value_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>string value = 2;</code>
             *
             * @param value The value to set.
             * @return This builder for chaining.
             */
            public Builder setValue(String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                value_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            /**
             * <code>string value = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearValue() {
                value_ = getDefaultInstance().getValue();
                bitField0_ = (bitField0_ & ~0x00000002);
                onChanged();
                return this;
            }

            /**
             * <code>string value = 2;</code>
             *
             * @param value The bytes for value to set.
             * @return This builder for chaining.
             */
            public Builder setValueBytes(com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);
                value_ = value;
                bitField0_ |= 0x00000002;
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

            // @@protoc_insertion_point(builder_scope:prometheus.Label)
        }

        // @@protoc_insertion_point(class_scope:prometheus.Label)
        private static final Types.Label DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Types.Label();
        }

        public static Types.Label getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<Label> PARSER =
                new com.google.protobuf.AbstractParser<Label>() {
                    @Override
                    public Label parsePartialFrom(
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

        public static com.google.protobuf.Parser<Label> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<Label> getParserForType() {
            return PARSER;
        }

        @Override
        public Types.Label getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface LabelsOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.Labels)
            com.google.protobuf.MessageOrBuilder {

        /** <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code> */
        java.util.List<Types.Label> getLabelsList();

        /** <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code> */
        Types.Label getLabels(int index);

        /** <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code> */
        int getLabelsCount();

        /** <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code> */
        java.util.List<? extends Types.LabelOrBuilder> getLabelsOrBuilderList();

        /** <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code> */
        Types.LabelOrBuilder getLabelsOrBuilder(int index);
    }

    /** Protobuf type {@code prometheus.Labels} */
    public static final class Labels extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.Labels)
            LabelsOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use Labels.newBuilder() to construct.
        private Labels(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private Labels() {
            labels_ = java.util.Collections.emptyList();
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new Labels();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Types.internal_static_prometheus_Labels_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Types.internal_static_prometheus_Labels_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Types.Labels.class, Types.Labels.Builder.class);
        }

        public static final int LABELS_FIELD_NUMBER = 1;

        @SuppressWarnings("serial")
        private java.util.List<Types.Label> labels_;

        /** <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code> */
        @Override
        public java.util.List<Types.Label> getLabelsList() {
            return labels_;
        }

        /** <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code> */
        @Override
        public java.util.List<? extends Types.LabelOrBuilder> getLabelsOrBuilderList() {
            return labels_;
        }

        /** <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code> */
        @Override
        public int getLabelsCount() {
            return labels_.size();
        }

        /** <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code> */
        @Override
        public Types.Label getLabels(int index) {
            return labels_.get(index);
        }

        /** <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code> */
        @Override
        public Types.LabelOrBuilder getLabelsOrBuilder(int index) {
            return labels_.get(index);
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
            for (int i = 0; i < labels_.size(); i++) {
                output.writeMessage(1, labels_.get(i));
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
            for (int i = 0; i < labels_.size(); i++) {
                size += com.google.protobuf.CodedOutputStream.computeMessageSize(1, labels_.get(i));
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
            if (!(obj instanceof Types.Labels)) {
                return super.equals(obj);
            }
            Types.Labels other = (Types.Labels) obj;

            if (!getLabelsList().equals(other.getLabelsList())) {
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
            if (getLabelsCount() > 0) {
                hash = (37 * hash) + LABELS_FIELD_NUMBER;
                hash = (53 * hash) + getLabelsList().hashCode();
            }
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Types.Labels parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Labels parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Labels parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Labels parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Labels parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Labels parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Labels parseFrom(java.io.InputStream input) throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.Labels parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.Labels parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Types.Labels parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.Labels parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.Labels parseFrom(
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

        public static Builder newBuilder(Types.Labels prototype) {
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

        /** Protobuf type {@code prometheus.Labels} */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.Labels)
                Types.LabelsOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Types.internal_static_prometheus_Labels_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Types.internal_static_prometheus_Labels_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Types.Labels.class, Types.Labels.Builder.class);
            }

            // Construct using Types.Labels.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                if (labelsBuilder_ == null) {
                    labels_ = java.util.Collections.emptyList();
                } else {
                    labels_ = null;
                    labelsBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000001);
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Types.internal_static_prometheus_Labels_descriptor;
            }

            @Override
            public Types.Labels getDefaultInstanceForType() {
                return Types.Labels.getDefaultInstance();
            }

            @Override
            public Types.Labels build() {
                Types.Labels result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Types.Labels buildPartial() {
                Types.Labels result = new Types.Labels(this);
                buildPartialRepeatedFields(result);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartialRepeatedFields(Types.Labels result) {
                if (labelsBuilder_ == null) {
                    if (((bitField0_ & 0x00000001) != 0)) {
                        labels_ = java.util.Collections.unmodifiableList(labels_);
                        bitField0_ = (bitField0_ & ~0x00000001);
                    }
                    result.labels_ = labels_;
                } else {
                    result.labels_ = labelsBuilder_.build();
                }
            }

            private void buildPartial0(Types.Labels result) {
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
                if (other instanceof Types.Labels) {
                    return mergeFrom((Types.Labels) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Types.Labels other) {
                if (other == Types.Labels.getDefaultInstance()) {
                    return this;
                }
                if (labelsBuilder_ == null) {
                    if (!other.labels_.isEmpty()) {
                        if (labels_.isEmpty()) {
                            labels_ = other.labels_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                        } else {
                            ensureLabelsIsMutable();
                            labels_.addAll(other.labels_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.labels_.isEmpty()) {
                        if (labelsBuilder_.isEmpty()) {
                            labelsBuilder_.dispose();
                            labelsBuilder_ = null;
                            labels_ = other.labels_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                            labelsBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getLabelsFieldBuilder()
                                            : null;
                        } else {
                            labelsBuilder_.addAllMessages(other.labels_);
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
                                    Types.Label m =
                                            input.readMessage(
                                                    Types.Label.parser(), extensionRegistry);
                                    if (labelsBuilder_ == null) {
                                        ensureLabelsIsMutable();
                                        labels_.add(m);
                                    } else {
                                        labelsBuilder_.addMessage(m);
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

            private java.util.List<Types.Label> labels_ = java.util.Collections.emptyList();

            private void ensureLabelsIsMutable() {
                if (!((bitField0_ & 0x00000001) != 0)) {
                    labels_ = new java.util.ArrayList<Types.Label>(labels_);
                    bitField0_ |= 0x00000001;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Label, Types.Label.Builder, Types.LabelOrBuilder>
                    labelsBuilder_;

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<Types.Label> getLabelsList() {
                if (labelsBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(labels_);
                } else {
                    return labelsBuilder_.getMessageList();
                }
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public int getLabelsCount() {
                if (labelsBuilder_ == null) {
                    return labels_.size();
                } else {
                    return labelsBuilder_.getCount();
                }
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label getLabels(int index) {
                if (labelsBuilder_ == null) {
                    return labels_.get(index);
                } else {
                    return labelsBuilder_.getMessage(index);
                }
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder setLabels(int index, Types.Label value) {
                if (labelsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureLabelsIsMutable();
                    labels_.set(index, value);
                    onChanged();
                } else {
                    labelsBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder setLabels(int index, Types.Label.Builder builderForValue) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    labelsBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(Types.Label value) {
                if (labelsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureLabelsIsMutable();
                    labels_.add(value);
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(int index, Types.Label value) {
                if (labelsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureLabelsIsMutable();
                    labels_.add(index, value);
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(Types.Label.Builder builderForValue) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.add(builderForValue.build());
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(int index, Types.Label.Builder builderForValue) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addAllLabels(Iterable<? extends Types.Label> values) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, labels_);
                    onChanged();
                } else {
                    labelsBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder clearLabels() {
                if (labelsBuilder_ == null) {
                    labels_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000001);
                    onChanged();
                } else {
                    labelsBuilder_.clear();
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder removeLabels(int index) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.remove(index);
                    onChanged();
                } else {
                    labelsBuilder_.remove(index);
                }
                return this;
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label.Builder getLabelsBuilder(int index) {
                return getLabelsFieldBuilder().getBuilder(index);
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.LabelOrBuilder getLabelsOrBuilder(int index) {
                if (labelsBuilder_ == null) {
                    return labels_.get(index);
                } else {
                    return labelsBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<? extends Types.LabelOrBuilder> getLabelsOrBuilderList() {
                if (labelsBuilder_ != null) {
                    return labelsBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(labels_);
                }
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label.Builder addLabelsBuilder() {
                return getLabelsFieldBuilder().addBuilder(Types.Label.getDefaultInstance());
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label.Builder addLabelsBuilder(int index) {
                return getLabelsFieldBuilder().addBuilder(index, Types.Label.getDefaultInstance());
            }

            /**
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<Types.Label.Builder> getLabelsBuilderList() {
                return getLabelsFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Label, Types.Label.Builder, Types.LabelOrBuilder>
                    getLabelsFieldBuilder() {
                if (labelsBuilder_ == null) {
                    labelsBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.Label, Types.Label.Builder, Types.LabelOrBuilder>(
                                    labels_,
                                    ((bitField0_ & 0x00000001) != 0),
                                    getParentForChildren(),
                                    isClean());
                    labels_ = null;
                }
                return labelsBuilder_;
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

            // @@protoc_insertion_point(builder_scope:prometheus.Labels)
        }

        // @@protoc_insertion_point(class_scope:prometheus.Labels)
        private static final Types.Labels DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Types.Labels();
        }

        public static Types.Labels getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<Labels> PARSER =
                new com.google.protobuf.AbstractParser<Labels>() {
                    @Override
                    public Labels parsePartialFrom(
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

        public static com.google.protobuf.Parser<Labels> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<Labels> getParserForType() {
            return PARSER;
        }

        @Override
        public Types.Labels getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface LabelMatcherOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.LabelMatcher)
            com.google.protobuf.MessageOrBuilder {

        /**
         * <code>.prometheus.LabelMatcher.Type type = 1;</code>
         *
         * @return The enum numeric value on the wire for type.
         */
        int getTypeValue();

        /**
         * <code>.prometheus.LabelMatcher.Type type = 1;</code>
         *
         * @return The type.
         */
        Types.LabelMatcher.Type getType();

        /**
         * <code>string name = 2;</code>
         *
         * @return The name.
         */
        String getName();

        /**
         * <code>string name = 2;</code>
         *
         * @return The bytes for name.
         */
        com.google.protobuf.ByteString getNameBytes();

        /**
         * <code>string value = 3;</code>
         *
         * @return The value.
         */
        String getValue();

        /**
         * <code>string value = 3;</code>
         *
         * @return The bytes for value.
         */
        com.google.protobuf.ByteString getValueBytes();
    }

    /**
     *
     *
     * <pre>
     * Matcher specifies a rule, which can match or set of labels or not.
     * </pre>
     *
     * <p>Protobuf type {@code prometheus.LabelMatcher}
     */
    public static final class LabelMatcher extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.LabelMatcher)
            LabelMatcherOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use LabelMatcher.newBuilder() to construct.
        private LabelMatcher(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private LabelMatcher() {
            type_ = 0;
            name_ = "";
            value_ = "";
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new LabelMatcher();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Types.internal_static_prometheus_LabelMatcher_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Types.internal_static_prometheus_LabelMatcher_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Types.LabelMatcher.class, Types.LabelMatcher.Builder.class);
        }

        /** Protobuf enum {@code prometheus.LabelMatcher.Type} */
        public enum Type implements com.google.protobuf.ProtocolMessageEnum {
            /** <code>EQ = 0;</code> */
            EQ(0),
            /** <code>NEQ = 1;</code> */
            NEQ(1),
            /** <code>RE = 2;</code> */
            RE(2),
            /** <code>NRE = 3;</code> */
            NRE(3),
            UNRECOGNIZED(-1),
            ;

            /** <code>EQ = 0;</code> */
            public static final int EQ_VALUE = 0;
            /** <code>NEQ = 1;</code> */
            public static final int NEQ_VALUE = 1;
            /** <code>RE = 2;</code> */
            public static final int RE_VALUE = 2;
            /** <code>NRE = 3;</code> */
            public static final int NRE_VALUE = 3;

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
            public static Type valueOf(int value) {
                return forNumber(value);
            }

            /**
             * @param value The numeric wire value of the corresponding enum entry.
             * @return The enum associated with the given numeric wire value.
             */
            public static Type forNumber(int value) {
                switch (value) {
                    case 0:
                        return EQ;
                    case 1:
                        return NEQ;
                    case 2:
                        return RE;
                    case 3:
                        return NRE;
                    default:
                        return null;
                }
            }

            public static com.google.protobuf.Internal.EnumLiteMap<Type> internalGetValueMap() {
                return internalValueMap;
            }

            private static final com.google.protobuf.Internal.EnumLiteMap<Type> internalValueMap =
                    new com.google.protobuf.Internal.EnumLiteMap<Type>() {
                        public Type findValueByNumber(int number) {
                            return Type.forNumber(number);
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
                return Types.LabelMatcher.getDescriptor().getEnumTypes().get(0);
            }

            private static final Type[] VALUES = values();

            public static Type valueOf(com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
                if (desc.getType() != getDescriptor()) {
                    throw new IllegalArgumentException("EnumValueDescriptor is not for this type.");
                }
                if (desc.getIndex() == -1) {
                    return UNRECOGNIZED;
                }
                return VALUES[desc.getIndex()];
            }

            private final int value;

            private Type(int value) {
                this.value = value;
            }

            // @@protoc_insertion_point(enum_scope:prometheus.LabelMatcher.Type)
        }

        public static final int TYPE_FIELD_NUMBER = 1;
        private int type_ = 0;

        /**
         * <code>.prometheus.LabelMatcher.Type type = 1;</code>
         *
         * @return The enum numeric value on the wire for type.
         */
        @Override
        public int getTypeValue() {
            return type_;
        }

        /**
         * <code>.prometheus.LabelMatcher.Type type = 1;</code>
         *
         * @return The type.
         */
        @Override
        public Types.LabelMatcher.Type getType() {
            Types.LabelMatcher.Type result = Types.LabelMatcher.Type.forNumber(type_);
            return result == null ? Types.LabelMatcher.Type.UNRECOGNIZED : result;
        }

        public static final int NAME_FIELD_NUMBER = 2;

        @SuppressWarnings("serial")
        private volatile Object name_ = "";

        /**
         * <code>string name = 2;</code>
         *
         * @return The name.
         */
        @Override
        public String getName() {
            Object ref = name_;
            if (ref instanceof String) {
                return (String) ref;
            } else {
                com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                name_ = s;
                return s;
            }
        }

        /**
         * <code>string name = 2;</code>
         *
         * @return The bytes for name.
         */
        @Override
        public com.google.protobuf.ByteString getNameBytes() {
            Object ref = name_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                name_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        public static final int VALUE_FIELD_NUMBER = 3;

        @SuppressWarnings("serial")
        private volatile Object value_ = "";

        /**
         * <code>string value = 3;</code>
         *
         * @return The value.
         */
        @Override
        public String getValue() {
            Object ref = value_;
            if (ref instanceof String) {
                return (String) ref;
            } else {
                com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                value_ = s;
                return s;
            }
        }

        /**
         * <code>string value = 3;</code>
         *
         * @return The bytes for value.
         */
        @Override
        public com.google.protobuf.ByteString getValueBytes() {
            Object ref = value_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                value_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
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
            if (type_ != Types.LabelMatcher.Type.EQ.getNumber()) {
                output.writeEnum(1, type_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(name_)) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 2, name_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(value_)) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 3, value_);
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
            if (type_ != Types.LabelMatcher.Type.EQ.getNumber()) {
                size += com.google.protobuf.CodedOutputStream.computeEnumSize(1, type_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(name_)) {
                size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, name_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(value_)) {
                size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, value_);
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
            if (!(obj instanceof Types.LabelMatcher)) {
                return super.equals(obj);
            }
            Types.LabelMatcher other = (Types.LabelMatcher) obj;

            if (type_ != other.type_) {
                return false;
            }
            if (!getName().equals(other.getName())) {
                return false;
            }
            if (!getValue().equals(other.getValue())) {
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
            hash = (37 * hash) + TYPE_FIELD_NUMBER;
            hash = (53 * hash) + type_;
            hash = (37 * hash) + NAME_FIELD_NUMBER;
            hash = (53 * hash) + getName().hashCode();
            hash = (37 * hash) + VALUE_FIELD_NUMBER;
            hash = (53 * hash) + getValue().hashCode();
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Types.LabelMatcher parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.LabelMatcher parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.LabelMatcher parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.LabelMatcher parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.LabelMatcher parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.LabelMatcher parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.LabelMatcher parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.LabelMatcher parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.LabelMatcher parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Types.LabelMatcher parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.LabelMatcher parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.LabelMatcher parseFrom(
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

        public static Builder newBuilder(Types.LabelMatcher prototype) {
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
         * Matcher specifies a rule, which can match or set of labels or not.
         * </pre>
         *
         * <p>Protobuf type {@code prometheus.LabelMatcher}
         */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.LabelMatcher)
                Types.LabelMatcherOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Types.internal_static_prometheus_LabelMatcher_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Types.internal_static_prometheus_LabelMatcher_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Types.LabelMatcher.class, Types.LabelMatcher.Builder.class);
            }

            // Construct using Types.LabelMatcher.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                type_ = 0;
                name_ = "";
                value_ = "";
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Types.internal_static_prometheus_LabelMatcher_descriptor;
            }

            @Override
            public Types.LabelMatcher getDefaultInstanceForType() {
                return Types.LabelMatcher.getDefaultInstance();
            }

            @Override
            public Types.LabelMatcher build() {
                Types.LabelMatcher result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Types.LabelMatcher buildPartial() {
                Types.LabelMatcher result = new Types.LabelMatcher(this);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartial0(Types.LabelMatcher result) {
                int from_bitField0_ = bitField0_;
                if (((from_bitField0_ & 0x00000001) != 0)) {
                    result.type_ = type_;
                }
                if (((from_bitField0_ & 0x00000002) != 0)) {
                    result.name_ = name_;
                }
                if (((from_bitField0_ & 0x00000004) != 0)) {
                    result.value_ = value_;
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
                if (other instanceof Types.LabelMatcher) {
                    return mergeFrom((Types.LabelMatcher) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Types.LabelMatcher other) {
                if (other == Types.LabelMatcher.getDefaultInstance()) {
                    return this;
                }
                if (other.type_ != 0) {
                    setTypeValue(other.getTypeValue());
                }
                if (!other.getName().isEmpty()) {
                    name_ = other.name_;
                    bitField0_ |= 0x00000002;
                    onChanged();
                }
                if (!other.getValue().isEmpty()) {
                    value_ = other.value_;
                    bitField0_ |= 0x00000004;
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
                            case 8:
                                {
                                    type_ = input.readEnum();
                                    bitField0_ |= 0x00000001;
                                    break;
                                } // case 8
                            case 18:
                                {
                                    name_ = input.readStringRequireUtf8();
                                    bitField0_ |= 0x00000002;
                                    break;
                                } // case 18
                            case 26:
                                {
                                    value_ = input.readStringRequireUtf8();
                                    bitField0_ |= 0x00000004;
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

            private int type_ = 0;

            /**
             * <code>.prometheus.LabelMatcher.Type type = 1;</code>
             *
             * @return The enum numeric value on the wire for type.
             */
            @Override
            public int getTypeValue() {
                return type_;
            }

            /**
             * <code>.prometheus.LabelMatcher.Type type = 1;</code>
             *
             * @param value The enum numeric value on the wire for type to set.
             * @return This builder for chaining.
             */
            public Builder setTypeValue(int value) {
                type_ = value;
                bitField0_ |= 0x00000001;
                onChanged();
                return this;
            }

            /**
             * <code>.prometheus.LabelMatcher.Type type = 1;</code>
             *
             * @return The type.
             */
            @Override
            public Types.LabelMatcher.Type getType() {
                Types.LabelMatcher.Type result = Types.LabelMatcher.Type.forNumber(type_);
                return result == null ? Types.LabelMatcher.Type.UNRECOGNIZED : result;
            }

            /**
             * <code>.prometheus.LabelMatcher.Type type = 1;</code>
             *
             * @param value The type to set.
             * @return This builder for chaining.
             */
            public Builder setType(Types.LabelMatcher.Type value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                bitField0_ |= 0x00000001;
                type_ = value.getNumber();
                onChanged();
                return this;
            }

            /**
             * <code>.prometheus.LabelMatcher.Type type = 1;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearType() {
                bitField0_ = (bitField0_ & ~0x00000001);
                type_ = 0;
                onChanged();
                return this;
            }

            private Object name_ = "";

            /**
             * <code>string name = 2;</code>
             *
             * @return The name.
             */
            public String getName() {
                Object ref = name_;
                if (!(ref instanceof String)) {
                    com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                    String s = bs.toStringUtf8();
                    name_ = s;
                    return s;
                } else {
                    return (String) ref;
                }
            }

            /**
             * <code>string name = 2;</code>
             *
             * @return The bytes for name.
             */
            public com.google.protobuf.ByteString getNameBytes() {
                Object ref = name_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                    name_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>string name = 2;</code>
             *
             * @param value The name to set.
             * @return This builder for chaining.
             */
            public Builder setName(String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                name_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            /**
             * <code>string name = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearName() {
                name_ = getDefaultInstance().getName();
                bitField0_ = (bitField0_ & ~0x00000002);
                onChanged();
                return this;
            }

            /**
             * <code>string name = 2;</code>
             *
             * @param value The bytes for name to set.
             * @return This builder for chaining.
             */
            public Builder setNameBytes(com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);
                name_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            private Object value_ = "";

            /**
             * <code>string value = 3;</code>
             *
             * @return The value.
             */
            public String getValue() {
                Object ref = value_;
                if (!(ref instanceof String)) {
                    com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                    String s = bs.toStringUtf8();
                    value_ = s;
                    return s;
                } else {
                    return (String) ref;
                }
            }

            /**
             * <code>string value = 3;</code>
             *
             * @return The bytes for value.
             */
            public com.google.protobuf.ByteString getValueBytes() {
                Object ref = value_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                    value_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>string value = 3;</code>
             *
             * @param value The value to set.
             * @return This builder for chaining.
             */
            public Builder setValue(String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                value_ = value;
                bitField0_ |= 0x00000004;
                onChanged();
                return this;
            }

            /**
             * <code>string value = 3;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearValue() {
                value_ = getDefaultInstance().getValue();
                bitField0_ = (bitField0_ & ~0x00000004);
                onChanged();
                return this;
            }

            /**
             * <code>string value = 3;</code>
             *
             * @param value The bytes for value to set.
             * @return This builder for chaining.
             */
            public Builder setValueBytes(com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);
                value_ = value;
                bitField0_ |= 0x00000004;
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

            // @@protoc_insertion_point(builder_scope:prometheus.LabelMatcher)
        }

        // @@protoc_insertion_point(class_scope:prometheus.LabelMatcher)
        private static final Types.LabelMatcher DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Types.LabelMatcher();
        }

        public static Types.LabelMatcher getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<LabelMatcher> PARSER =
                new com.google.protobuf.AbstractParser<LabelMatcher>() {
                    @Override
                    public LabelMatcher parsePartialFrom(
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

        public static com.google.protobuf.Parser<LabelMatcher> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<LabelMatcher> getParserForType() {
            return PARSER;
        }

        @Override
        public Types.LabelMatcher getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface ReadHintsOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.ReadHints)
            com.google.protobuf.MessageOrBuilder {

        /**
         *
         *
         * <pre>
         * Query step size in milliseconds.
         * </pre>
         *
         * <code>int64 step_ms = 1;</code>
         *
         * @return The stepMs.
         */
        long getStepMs();

        /**
         *
         *
         * <pre>
         * String representation of surrounding function or aggregation.
         * </pre>
         *
         * <code>string func = 2;</code>
         *
         * @return The func.
         */
        String getFunc();

        /**
         *
         *
         * <pre>
         * String representation of surrounding function or aggregation.
         * </pre>
         *
         * <code>string func = 2;</code>
         *
         * @return The bytes for func.
         */
        com.google.protobuf.ByteString getFuncBytes();

        /**
         *
         *
         * <pre>
         * Start time in milliseconds.
         * </pre>
         *
         * <code>int64 start_ms = 3;</code>
         *
         * @return The startMs.
         */
        long getStartMs();

        /**
         *
         *
         * <pre>
         * End time in milliseconds.
         * </pre>
         *
         * <code>int64 end_ms = 4;</code>
         *
         * @return The endMs.
         */
        long getEndMs();

        /**
         *
         *
         * <pre>
         * List of label names used in aggregation.
         * </pre>
         *
         * <code>repeated string grouping = 5;</code>
         *
         * @return A list containing the grouping.
         */
        java.util.List<String> getGroupingList();

        /**
         *
         *
         * <pre>
         * List of label names used in aggregation.
         * </pre>
         *
         * <code>repeated string grouping = 5;</code>
         *
         * @return The count of grouping.
         */
        int getGroupingCount();

        /**
         *
         *
         * <pre>
         * List of label names used in aggregation.
         * </pre>
         *
         * <code>repeated string grouping = 5;</code>
         *
         * @param index The index of the element to return.
         * @return The grouping at the given index.
         */
        String getGrouping(int index);

        /**
         *
         *
         * <pre>
         * List of label names used in aggregation.
         * </pre>
         *
         * <code>repeated string grouping = 5;</code>
         *
         * @param index The index of the value to return.
         * @return The bytes of the grouping at the given index.
         */
        com.google.protobuf.ByteString getGroupingBytes(int index);

        /**
         *
         *
         * <pre>
         * Indicate whether it is without or by.
         * </pre>
         *
         * <code>bool by = 6;</code>
         *
         * @return The by.
         */
        boolean getBy();

        /**
         *
         *
         * <pre>
         * Range vector selector range in milliseconds.
         * </pre>
         *
         * <code>int64 range_ms = 7;</code>
         *
         * @return The rangeMs.
         */
        long getRangeMs();
    }

    /** Protobuf type {@code prometheus.ReadHints} */
    public static final class ReadHints extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.ReadHints)
            ReadHintsOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use ReadHints.newBuilder() to construct.
        private ReadHints(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private ReadHints() {
            func_ = "";
            grouping_ = com.google.protobuf.LazyStringArrayList.emptyList();
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new ReadHints();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Types.internal_static_prometheus_ReadHints_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Types.internal_static_prometheus_ReadHints_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Types.ReadHints.class, Types.ReadHints.Builder.class);
        }

        public static final int STEP_MS_FIELD_NUMBER = 1;
        private long stepMs_ = 0L;

        /**
         *
         *
         * <pre>
         * Query step size in milliseconds.
         * </pre>
         *
         * <code>int64 step_ms = 1;</code>
         *
         * @return The stepMs.
         */
        @Override
        public long getStepMs() {
            return stepMs_;
        }

        public static final int FUNC_FIELD_NUMBER = 2;

        @SuppressWarnings("serial")
        private volatile Object func_ = "";

        /**
         *
         *
         * <pre>
         * String representation of surrounding function or aggregation.
         * </pre>
         *
         * <code>string func = 2;</code>
         *
         * @return The func.
         */
        @Override
        public String getFunc() {
            Object ref = func_;
            if (ref instanceof String) {
                return (String) ref;
            } else {
                com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                func_ = s;
                return s;
            }
        }

        /**
         *
         *
         * <pre>
         * String representation of surrounding function or aggregation.
         * </pre>
         *
         * <code>string func = 2;</code>
         *
         * @return The bytes for func.
         */
        @Override
        public com.google.protobuf.ByteString getFuncBytes() {
            Object ref = func_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                func_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        public static final int START_MS_FIELD_NUMBER = 3;
        private long startMs_ = 0L;

        /**
         *
         *
         * <pre>
         * Start time in milliseconds.
         * </pre>
         *
         * <code>int64 start_ms = 3;</code>
         *
         * @return The startMs.
         */
        @Override
        public long getStartMs() {
            return startMs_;
        }

        public static final int END_MS_FIELD_NUMBER = 4;
        private long endMs_ = 0L;

        /**
         *
         *
         * <pre>
         * End time in milliseconds.
         * </pre>
         *
         * <code>int64 end_ms = 4;</code>
         *
         * @return The endMs.
         */
        @Override
        public long getEndMs() {
            return endMs_;
        }

        public static final int GROUPING_FIELD_NUMBER = 5;

        @SuppressWarnings("serial")
        private com.google.protobuf.LazyStringArrayList grouping_ =
                com.google.protobuf.LazyStringArrayList.emptyList();

        /**
         *
         *
         * <pre>
         * List of label names used in aggregation.
         * </pre>
         *
         * <code>repeated string grouping = 5;</code>
         *
         * @return A list containing the grouping.
         */
        public com.google.protobuf.ProtocolStringList getGroupingList() {
            return grouping_;
        }

        /**
         *
         *
         * <pre>
         * List of label names used in aggregation.
         * </pre>
         *
         * <code>repeated string grouping = 5;</code>
         *
         * @return The count of grouping.
         */
        public int getGroupingCount() {
            return grouping_.size();
        }

        /**
         *
         *
         * <pre>
         * List of label names used in aggregation.
         * </pre>
         *
         * <code>repeated string grouping = 5;</code>
         *
         * @param index The index of the element to return.
         * @return The grouping at the given index.
         */
        public String getGrouping(int index) {
            return grouping_.get(index);
        }

        /**
         *
         *
         * <pre>
         * List of label names used in aggregation.
         * </pre>
         *
         * <code>repeated string grouping = 5;</code>
         *
         * @param index The index of the value to return.
         * @return The bytes of the grouping at the given index.
         */
        public com.google.protobuf.ByteString getGroupingBytes(int index) {
            return grouping_.getByteString(index);
        }

        public static final int BY_FIELD_NUMBER = 6;
        private boolean by_ = false;

        /**
         *
         *
         * <pre>
         * Indicate whether it is without or by.
         * </pre>
         *
         * <code>bool by = 6;</code>
         *
         * @return The by.
         */
        @Override
        public boolean getBy() {
            return by_;
        }

        public static final int RANGE_MS_FIELD_NUMBER = 7;
        private long rangeMs_ = 0L;

        /**
         *
         *
         * <pre>
         * Range vector selector range in milliseconds.
         * </pre>
         *
         * <code>int64 range_ms = 7;</code>
         *
         * @return The rangeMs.
         */
        @Override
        public long getRangeMs() {
            return rangeMs_;
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
            if (stepMs_ != 0L) {
                output.writeInt64(1, stepMs_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(func_)) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 2, func_);
            }
            if (startMs_ != 0L) {
                output.writeInt64(3, startMs_);
            }
            if (endMs_ != 0L) {
                output.writeInt64(4, endMs_);
            }
            for (int i = 0; i < grouping_.size(); i++) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 5, grouping_.getRaw(i));
            }
            if (by_ != false) {
                output.writeBool(6, by_);
            }
            if (rangeMs_ != 0L) {
                output.writeInt64(7, rangeMs_);
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
            if (stepMs_ != 0L) {
                size += com.google.protobuf.CodedOutputStream.computeInt64Size(1, stepMs_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(func_)) {
                size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, func_);
            }
            if (startMs_ != 0L) {
                size += com.google.protobuf.CodedOutputStream.computeInt64Size(3, startMs_);
            }
            if (endMs_ != 0L) {
                size += com.google.protobuf.CodedOutputStream.computeInt64Size(4, endMs_);
            }
            {
                int dataSize = 0;
                for (int i = 0; i < grouping_.size(); i++) {
                    dataSize += computeStringSizeNoTag(grouping_.getRaw(i));
                }
                size += dataSize;
                size += 1 * getGroupingList().size();
            }
            if (by_ != false) {
                size += com.google.protobuf.CodedOutputStream.computeBoolSize(6, by_);
            }
            if (rangeMs_ != 0L) {
                size += com.google.protobuf.CodedOutputStream.computeInt64Size(7, rangeMs_);
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
            if (!(obj instanceof Types.ReadHints)) {
                return super.equals(obj);
            }
            Types.ReadHints other = (Types.ReadHints) obj;

            if (getStepMs() != other.getStepMs()) {
                return false;
            }
            if (!getFunc().equals(other.getFunc())) {
                return false;
            }
            if (getStartMs() != other.getStartMs()) {
                return false;
            }
            if (getEndMs() != other.getEndMs()) {
                return false;
            }
            if (!getGroupingList().equals(other.getGroupingList())) {
                return false;
            }
            if (getBy() != other.getBy()) {
                return false;
            }
            if (getRangeMs() != other.getRangeMs()) {
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
            hash = (37 * hash) + STEP_MS_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getStepMs());
            hash = (37 * hash) + FUNC_FIELD_NUMBER;
            hash = (53 * hash) + getFunc().hashCode();
            hash = (37 * hash) + START_MS_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getStartMs());
            hash = (37 * hash) + END_MS_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getEndMs());
            if (getGroupingCount() > 0) {
                hash = (37 * hash) + GROUPING_FIELD_NUMBER;
                hash = (53 * hash) + getGroupingList().hashCode();
            }
            hash = (37 * hash) + BY_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(getBy());
            hash = (37 * hash) + RANGE_MS_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getRangeMs());
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Types.ReadHints parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.ReadHints parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.ReadHints parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.ReadHints parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.ReadHints parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.ReadHints parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.ReadHints parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.ReadHints parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.ReadHints parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Types.ReadHints parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.ReadHints parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.ReadHints parseFrom(
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

        public static Builder newBuilder(Types.ReadHints prototype) {
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

        /** Protobuf type {@code prometheus.ReadHints} */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.ReadHints)
                Types.ReadHintsOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Types.internal_static_prometheus_ReadHints_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Types.internal_static_prometheus_ReadHints_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Types.ReadHints.class, Types.ReadHints.Builder.class);
            }

            // Construct using Types.ReadHints.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                stepMs_ = 0L;
                func_ = "";
                startMs_ = 0L;
                endMs_ = 0L;
                grouping_ = com.google.protobuf.LazyStringArrayList.emptyList();
                by_ = false;
                rangeMs_ = 0L;
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Types.internal_static_prometheus_ReadHints_descriptor;
            }

            @Override
            public Types.ReadHints getDefaultInstanceForType() {
                return Types.ReadHints.getDefaultInstance();
            }

            @Override
            public Types.ReadHints build() {
                Types.ReadHints result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Types.ReadHints buildPartial() {
                Types.ReadHints result = new Types.ReadHints(this);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartial0(Types.ReadHints result) {
                int from_bitField0_ = bitField0_;
                if (((from_bitField0_ & 0x00000001) != 0)) {
                    result.stepMs_ = stepMs_;
                }
                if (((from_bitField0_ & 0x00000002) != 0)) {
                    result.func_ = func_;
                }
                if (((from_bitField0_ & 0x00000004) != 0)) {
                    result.startMs_ = startMs_;
                }
                if (((from_bitField0_ & 0x00000008) != 0)) {
                    result.endMs_ = endMs_;
                }
                if (((from_bitField0_ & 0x00000010) != 0)) {
                    grouping_.makeImmutable();
                    result.grouping_ = grouping_;
                }
                if (((from_bitField0_ & 0x00000020) != 0)) {
                    result.by_ = by_;
                }
                if (((from_bitField0_ & 0x00000040) != 0)) {
                    result.rangeMs_ = rangeMs_;
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
                if (other instanceof Types.ReadHints) {
                    return mergeFrom((Types.ReadHints) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Types.ReadHints other) {
                if (other == Types.ReadHints.getDefaultInstance()) {
                    return this;
                }
                if (other.getStepMs() != 0L) {
                    setStepMs(other.getStepMs());
                }
                if (!other.getFunc().isEmpty()) {
                    func_ = other.func_;
                    bitField0_ |= 0x00000002;
                    onChanged();
                }
                if (other.getStartMs() != 0L) {
                    setStartMs(other.getStartMs());
                }
                if (other.getEndMs() != 0L) {
                    setEndMs(other.getEndMs());
                }
                if (!other.grouping_.isEmpty()) {
                    if (grouping_.isEmpty()) {
                        grouping_ = other.grouping_;
                        bitField0_ |= 0x00000010;
                    } else {
                        ensureGroupingIsMutable();
                        grouping_.addAll(other.grouping_);
                    }
                    onChanged();
                }
                if (other.getBy() != false) {
                    setBy(other.getBy());
                }
                if (other.getRangeMs() != 0L) {
                    setRangeMs(other.getRangeMs());
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
                                    stepMs_ = input.readInt64();
                                    bitField0_ |= 0x00000001;
                                    break;
                                } // case 8
                            case 18:
                                {
                                    func_ = input.readStringRequireUtf8();
                                    bitField0_ |= 0x00000002;
                                    break;
                                } // case 18
                            case 24:
                                {
                                    startMs_ = input.readInt64();
                                    bitField0_ |= 0x00000004;
                                    break;
                                } // case 24
                            case 32:
                                {
                                    endMs_ = input.readInt64();
                                    bitField0_ |= 0x00000008;
                                    break;
                                } // case 32
                            case 42:
                                {
                                    String s = input.readStringRequireUtf8();
                                    ensureGroupingIsMutable();
                                    grouping_.add(s);
                                    break;
                                } // case 42
                            case 48:
                                {
                                    by_ = input.readBool();
                                    bitField0_ |= 0x00000020;
                                    break;
                                } // case 48
                            case 56:
                                {
                                    rangeMs_ = input.readInt64();
                                    bitField0_ |= 0x00000040;
                                    break;
                                } // case 56
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

            private long stepMs_;

            /**
             *
             *
             * <pre>
             * Query step size in milliseconds.
             * </pre>
             *
             * <code>int64 step_ms = 1;</code>
             *
             * @return The stepMs.
             */
            @Override
            public long getStepMs() {
                return stepMs_;
            }

            /**
             *
             *
             * <pre>
             * Query step size in milliseconds.
             * </pre>
             *
             * <code>int64 step_ms = 1;</code>
             *
             * @param value The stepMs to set.
             * @return This builder for chaining.
             */
            public Builder setStepMs(long value) {

                stepMs_ = value;
                bitField0_ |= 0x00000001;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Query step size in milliseconds.
             * </pre>
             *
             * <code>int64 step_ms = 1;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearStepMs() {
                bitField0_ = (bitField0_ & ~0x00000001);
                stepMs_ = 0L;
                onChanged();
                return this;
            }

            private Object func_ = "";

            /**
             *
             *
             * <pre>
             * String representation of surrounding function or aggregation.
             * </pre>
             *
             * <code>string func = 2;</code>
             *
             * @return The func.
             */
            public String getFunc() {
                Object ref = func_;
                if (!(ref instanceof String)) {
                    com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                    String s = bs.toStringUtf8();
                    func_ = s;
                    return s;
                } else {
                    return (String) ref;
                }
            }

            /**
             *
             *
             * <pre>
             * String representation of surrounding function or aggregation.
             * </pre>
             *
             * <code>string func = 2;</code>
             *
             * @return The bytes for func.
             */
            public com.google.protobuf.ByteString getFuncBytes() {
                Object ref = func_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8((String) ref);
                    func_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             *
             *
             * <pre>
             * String representation of surrounding function or aggregation.
             * </pre>
             *
             * <code>string func = 2;</code>
             *
             * @param value The func to set.
             * @return This builder for chaining.
             */
            public Builder setFunc(String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                func_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * String representation of surrounding function or aggregation.
             * </pre>
             *
             * <code>string func = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearFunc() {
                func_ = getDefaultInstance().getFunc();
                bitField0_ = (bitField0_ & ~0x00000002);
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * String representation of surrounding function or aggregation.
             * </pre>
             *
             * <code>string func = 2;</code>
             *
             * @param value The bytes for func to set.
             * @return This builder for chaining.
             */
            public Builder setFuncBytes(com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);
                func_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            private long startMs_;

            /**
             *
             *
             * <pre>
             * Start time in milliseconds.
             * </pre>
             *
             * <code>int64 start_ms = 3;</code>
             *
             * @return The startMs.
             */
            @Override
            public long getStartMs() {
                return startMs_;
            }

            /**
             *
             *
             * <pre>
             * Start time in milliseconds.
             * </pre>
             *
             * <code>int64 start_ms = 3;</code>
             *
             * @param value The startMs to set.
             * @return This builder for chaining.
             */
            public Builder setStartMs(long value) {

                startMs_ = value;
                bitField0_ |= 0x00000004;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Start time in milliseconds.
             * </pre>
             *
             * <code>int64 start_ms = 3;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearStartMs() {
                bitField0_ = (bitField0_ & ~0x00000004);
                startMs_ = 0L;
                onChanged();
                return this;
            }

            private long endMs_;

            /**
             *
             *
             * <pre>
             * End time in milliseconds.
             * </pre>
             *
             * <code>int64 end_ms = 4;</code>
             *
             * @return The endMs.
             */
            @Override
            public long getEndMs() {
                return endMs_;
            }

            /**
             *
             *
             * <pre>
             * End time in milliseconds.
             * </pre>
             *
             * <code>int64 end_ms = 4;</code>
             *
             * @param value The endMs to set.
             * @return This builder for chaining.
             */
            public Builder setEndMs(long value) {

                endMs_ = value;
                bitField0_ |= 0x00000008;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * End time in milliseconds.
             * </pre>
             *
             * <code>int64 end_ms = 4;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearEndMs() {
                bitField0_ = (bitField0_ & ~0x00000008);
                endMs_ = 0L;
                onChanged();
                return this;
            }

            private com.google.protobuf.LazyStringArrayList grouping_ =
                    com.google.protobuf.LazyStringArrayList.emptyList();

            private void ensureGroupingIsMutable() {
                if (!grouping_.isModifiable()) {
                    grouping_ = new com.google.protobuf.LazyStringArrayList(grouping_);
                }
                bitField0_ |= 0x00000010;
            }

            /**
             *
             *
             * <pre>
             * List of label names used in aggregation.
             * </pre>
             *
             * <code>repeated string grouping = 5;</code>
             *
             * @return A list containing the grouping.
             */
            public com.google.protobuf.ProtocolStringList getGroupingList() {
                grouping_.makeImmutable();
                return grouping_;
            }

            /**
             *
             *
             * <pre>
             * List of label names used in aggregation.
             * </pre>
             *
             * <code>repeated string grouping = 5;</code>
             *
             * @return The count of grouping.
             */
            public int getGroupingCount() {
                return grouping_.size();
            }

            /**
             *
             *
             * <pre>
             * List of label names used in aggregation.
             * </pre>
             *
             * <code>repeated string grouping = 5;</code>
             *
             * @param index The index of the element to return.
             * @return The grouping at the given index.
             */
            public String getGrouping(int index) {
                return grouping_.get(index);
            }

            /**
             *
             *
             * <pre>
             * List of label names used in aggregation.
             * </pre>
             *
             * <code>repeated string grouping = 5;</code>
             *
             * @param index The index of the value to return.
             * @return The bytes of the grouping at the given index.
             */
            public com.google.protobuf.ByteString getGroupingBytes(int index) {
                return grouping_.getByteString(index);
            }

            /**
             *
             *
             * <pre>
             * List of label names used in aggregation.
             * </pre>
             *
             * <code>repeated string grouping = 5;</code>
             *
             * @param index The index to set the value at.
             * @param value The grouping to set.
             * @return This builder for chaining.
             */
            public Builder setGrouping(int index, String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                ensureGroupingIsMutable();
                grouping_.set(index, value);
                bitField0_ |= 0x00000010;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * List of label names used in aggregation.
             * </pre>
             *
             * <code>repeated string grouping = 5;</code>
             *
             * @param value The grouping to add.
             * @return This builder for chaining.
             */
            public Builder addGrouping(String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                ensureGroupingIsMutable();
                grouping_.add(value);
                bitField0_ |= 0x00000010;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * List of label names used in aggregation.
             * </pre>
             *
             * <code>repeated string grouping = 5;</code>
             *
             * @param values The grouping to add.
             * @return This builder for chaining.
             */
            public Builder addAllGrouping(Iterable<String> values) {
                ensureGroupingIsMutable();
                com.google.protobuf.AbstractMessageLite.Builder.addAll(values, grouping_);
                bitField0_ |= 0x00000010;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * List of label names used in aggregation.
             * </pre>
             *
             * <code>repeated string grouping = 5;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearGrouping() {
                grouping_ = com.google.protobuf.LazyStringArrayList.emptyList();
                bitField0_ = (bitField0_ & ~0x00000010);
                ;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * List of label names used in aggregation.
             * </pre>
             *
             * <code>repeated string grouping = 5;</code>
             *
             * @param value The bytes of the grouping to add.
             * @return This builder for chaining.
             */
            public Builder addGroupingBytes(com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);
                ensureGroupingIsMutable();
                grouping_.add(value);
                bitField0_ |= 0x00000010;
                onChanged();
                return this;
            }

            private boolean by_;

            /**
             *
             *
             * <pre>
             * Indicate whether it is without or by.
             * </pre>
             *
             * <code>bool by = 6;</code>
             *
             * @return The by.
             */
            @Override
            public boolean getBy() {
                return by_;
            }

            /**
             *
             *
             * <pre>
             * Indicate whether it is without or by.
             * </pre>
             *
             * <code>bool by = 6;</code>
             *
             * @param value The by to set.
             * @return This builder for chaining.
             */
            public Builder setBy(boolean value) {

                by_ = value;
                bitField0_ |= 0x00000020;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Indicate whether it is without or by.
             * </pre>
             *
             * <code>bool by = 6;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearBy() {
                bitField0_ = (bitField0_ & ~0x00000020);
                by_ = false;
                onChanged();
                return this;
            }

            private long rangeMs_;

            /**
             *
             *
             * <pre>
             * Range vector selector range in milliseconds.
             * </pre>
             *
             * <code>int64 range_ms = 7;</code>
             *
             * @return The rangeMs.
             */
            @Override
            public long getRangeMs() {
                return rangeMs_;
            }

            /**
             *
             *
             * <pre>
             * Range vector selector range in milliseconds.
             * </pre>
             *
             * <code>int64 range_ms = 7;</code>
             *
             * @param value The rangeMs to set.
             * @return This builder for chaining.
             */
            public Builder setRangeMs(long value) {

                rangeMs_ = value;
                bitField0_ |= 0x00000040;
                onChanged();
                return this;
            }

            /**
             *
             *
             * <pre>
             * Range vector selector range in milliseconds.
             * </pre>
             *
             * <code>int64 range_ms = 7;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearRangeMs() {
                bitField0_ = (bitField0_ & ~0x00000040);
                rangeMs_ = 0L;
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

            // @@protoc_insertion_point(builder_scope:prometheus.ReadHints)
        }

        // @@protoc_insertion_point(class_scope:prometheus.ReadHints)
        private static final Types.ReadHints DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Types.ReadHints();
        }

        public static Types.ReadHints getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<ReadHints> PARSER =
                new com.google.protobuf.AbstractParser<ReadHints>() {
                    @Override
                    public ReadHints parsePartialFrom(
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

        public static com.google.protobuf.Parser<ReadHints> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<ReadHints> getParserForType() {
            return PARSER;
        }

        @Override
        public Types.ReadHints getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface ChunkOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.Chunk)
            com.google.protobuf.MessageOrBuilder {

        /**
         * <code>int64 min_time_ms = 1;</code>
         *
         * @return The minTimeMs.
         */
        long getMinTimeMs();

        /**
         * <code>int64 max_time_ms = 2;</code>
         *
         * @return The maxTimeMs.
         */
        long getMaxTimeMs();

        /**
         * <code>.prometheus.Chunk.Encoding type = 3;</code>
         *
         * @return The enum numeric value on the wire for type.
         */
        int getTypeValue();

        /**
         * <code>.prometheus.Chunk.Encoding type = 3;</code>
         *
         * @return The type.
         */
        Types.Chunk.Encoding getType();

        /**
         * <code>bytes data = 4;</code>
         *
         * @return The data.
         */
        com.google.protobuf.ByteString getData();
    }

    /**
     *
     *
     * <pre>
     * Chunk represents a TSDB chunk.
     * Time range [min, max] is inclusive.
     * </pre>
     *
     * <p>Protobuf type {@code prometheus.Chunk}
     */
    public static final class Chunk extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.Chunk)
            ChunkOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use Chunk.newBuilder() to construct.
        private Chunk(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private Chunk() {
            type_ = 0;
            data_ = com.google.protobuf.ByteString.EMPTY;
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new Chunk();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Types.internal_static_prometheus_Chunk_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Types.internal_static_prometheus_Chunk_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(Types.Chunk.class, Types.Chunk.Builder.class);
        }

        /**
         *
         *
         * <pre>
         * We require this to match chunkenc.Encoding.
         * </pre>
         *
         * <p>Protobuf enum {@code prometheus.Chunk.Encoding}
         */
        public enum Encoding implements com.google.protobuf.ProtocolMessageEnum {
            /** <code>UNKNOWN = 0;</code> */
            UNKNOWN(0),
            /** <code>XOR = 1;</code> */
            XOR(1),
            /** <code>HISTOGRAM = 2;</code> */
            HISTOGRAM(2),
            /** <code>FLOAT_HISTOGRAM = 3;</code> */
            FLOAT_HISTOGRAM(3),
            UNRECOGNIZED(-1),
            ;

            /** <code>UNKNOWN = 0;</code> */
            public static final int UNKNOWN_VALUE = 0;
            /** <code>XOR = 1;</code> */
            public static final int XOR_VALUE = 1;
            /** <code>HISTOGRAM = 2;</code> */
            public static final int HISTOGRAM_VALUE = 2;
            /** <code>FLOAT_HISTOGRAM = 3;</code> */
            public static final int FLOAT_HISTOGRAM_VALUE = 3;

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
            public static Encoding valueOf(int value) {
                return forNumber(value);
            }

            /**
             * @param value The numeric wire value of the corresponding enum entry.
             * @return The enum associated with the given numeric wire value.
             */
            public static Encoding forNumber(int value) {
                switch (value) {
                    case 0:
                        return UNKNOWN;
                    case 1:
                        return XOR;
                    case 2:
                        return HISTOGRAM;
                    case 3:
                        return FLOAT_HISTOGRAM;
                    default:
                        return null;
                }
            }

            public static com.google.protobuf.Internal.EnumLiteMap<Encoding> internalGetValueMap() {
                return internalValueMap;
            }

            private static final com.google.protobuf.Internal.EnumLiteMap<Encoding>
                    internalValueMap =
                            new com.google.protobuf.Internal.EnumLiteMap<Encoding>() {
                                public Encoding findValueByNumber(int number) {
                                    return Encoding.forNumber(number);
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
                return Types.Chunk.getDescriptor().getEnumTypes().get(0);
            }

            private static final Encoding[] VALUES = values();

            public static Encoding valueOf(
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

            private Encoding(int value) {
                this.value = value;
            }

            // @@protoc_insertion_point(enum_scope:prometheus.Chunk.Encoding)
        }

        public static final int MIN_TIME_MS_FIELD_NUMBER = 1;
        private long minTimeMs_ = 0L;

        /**
         * <code>int64 min_time_ms = 1;</code>
         *
         * @return The minTimeMs.
         */
        @Override
        public long getMinTimeMs() {
            return minTimeMs_;
        }

        public static final int MAX_TIME_MS_FIELD_NUMBER = 2;
        private long maxTimeMs_ = 0L;

        /**
         * <code>int64 max_time_ms = 2;</code>
         *
         * @return The maxTimeMs.
         */
        @Override
        public long getMaxTimeMs() {
            return maxTimeMs_;
        }

        public static final int TYPE_FIELD_NUMBER = 3;
        private int type_ = 0;

        /**
         * <code>.prometheus.Chunk.Encoding type = 3;</code>
         *
         * @return The enum numeric value on the wire for type.
         */
        @Override
        public int getTypeValue() {
            return type_;
        }

        /**
         * <code>.prometheus.Chunk.Encoding type = 3;</code>
         *
         * @return The type.
         */
        @Override
        public Types.Chunk.Encoding getType() {
            Types.Chunk.Encoding result = Types.Chunk.Encoding.forNumber(type_);
            return result == null ? Types.Chunk.Encoding.UNRECOGNIZED : result;
        }

        public static final int DATA_FIELD_NUMBER = 4;
        private com.google.protobuf.ByteString data_ = com.google.protobuf.ByteString.EMPTY;

        /**
         * <code>bytes data = 4;</code>
         *
         * @return The data.
         */
        @Override
        public com.google.protobuf.ByteString getData() {
            return data_;
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
            if (minTimeMs_ != 0L) {
                output.writeInt64(1, minTimeMs_);
            }
            if (maxTimeMs_ != 0L) {
                output.writeInt64(2, maxTimeMs_);
            }
            if (type_ != Types.Chunk.Encoding.UNKNOWN.getNumber()) {
                output.writeEnum(3, type_);
            }
            if (!data_.isEmpty()) {
                output.writeBytes(4, data_);
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
            if (minTimeMs_ != 0L) {
                size += com.google.protobuf.CodedOutputStream.computeInt64Size(1, minTimeMs_);
            }
            if (maxTimeMs_ != 0L) {
                size += com.google.protobuf.CodedOutputStream.computeInt64Size(2, maxTimeMs_);
            }
            if (type_ != Types.Chunk.Encoding.UNKNOWN.getNumber()) {
                size += com.google.protobuf.CodedOutputStream.computeEnumSize(3, type_);
            }
            if (!data_.isEmpty()) {
                size += com.google.protobuf.CodedOutputStream.computeBytesSize(4, data_);
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
            if (!(obj instanceof Types.Chunk)) {
                return super.equals(obj);
            }
            Types.Chunk other = (Types.Chunk) obj;

            if (getMinTimeMs() != other.getMinTimeMs()) {
                return false;
            }
            if (getMaxTimeMs() != other.getMaxTimeMs()) {
                return false;
            }
            if (type_ != other.type_) {
                return false;
            }
            if (!getData().equals(other.getData())) {
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
            hash = (37 * hash) + MIN_TIME_MS_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getMinTimeMs());
            hash = (37 * hash) + MAX_TIME_MS_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getMaxTimeMs());
            hash = (37 * hash) + TYPE_FIELD_NUMBER;
            hash = (53 * hash) + type_;
            hash = (37 * hash) + DATA_FIELD_NUMBER;
            hash = (53 * hash) + getData().hashCode();
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Types.Chunk parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Chunk parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Chunk parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Chunk parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Chunk parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.Chunk parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.Chunk parseFrom(java.io.InputStream input) throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.Chunk parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.Chunk parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Types.Chunk parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.Chunk parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.Chunk parseFrom(
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

        public static Builder newBuilder(Types.Chunk prototype) {
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
         * Chunk represents a TSDB chunk.
         * Time range [min, max] is inclusive.
         * </pre>
         *
         * <p>Protobuf type {@code prometheus.Chunk}
         */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.Chunk)
                Types.ChunkOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Types.internal_static_prometheus_Chunk_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Types.internal_static_prometheus_Chunk_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Types.Chunk.class, Types.Chunk.Builder.class);
            }

            // Construct using Types.Chunk.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                minTimeMs_ = 0L;
                maxTimeMs_ = 0L;
                type_ = 0;
                data_ = com.google.protobuf.ByteString.EMPTY;
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Types.internal_static_prometheus_Chunk_descriptor;
            }

            @Override
            public Types.Chunk getDefaultInstanceForType() {
                return Types.Chunk.getDefaultInstance();
            }

            @Override
            public Types.Chunk build() {
                Types.Chunk result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Types.Chunk buildPartial() {
                Types.Chunk result = new Types.Chunk(this);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartial0(Types.Chunk result) {
                int from_bitField0_ = bitField0_;
                if (((from_bitField0_ & 0x00000001) != 0)) {
                    result.minTimeMs_ = minTimeMs_;
                }
                if (((from_bitField0_ & 0x00000002) != 0)) {
                    result.maxTimeMs_ = maxTimeMs_;
                }
                if (((from_bitField0_ & 0x00000004) != 0)) {
                    result.type_ = type_;
                }
                if (((from_bitField0_ & 0x00000008) != 0)) {
                    result.data_ = data_;
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
                if (other instanceof Types.Chunk) {
                    return mergeFrom((Types.Chunk) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Types.Chunk other) {
                if (other == Types.Chunk.getDefaultInstance()) {
                    return this;
                }
                if (other.getMinTimeMs() != 0L) {
                    setMinTimeMs(other.getMinTimeMs());
                }
                if (other.getMaxTimeMs() != 0L) {
                    setMaxTimeMs(other.getMaxTimeMs());
                }
                if (other.type_ != 0) {
                    setTypeValue(other.getTypeValue());
                }
                if (other.getData() != com.google.protobuf.ByteString.EMPTY) {
                    setData(other.getData());
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
                                    minTimeMs_ = input.readInt64();
                                    bitField0_ |= 0x00000001;
                                    break;
                                } // case 8
                            case 16:
                                {
                                    maxTimeMs_ = input.readInt64();
                                    bitField0_ |= 0x00000002;
                                    break;
                                } // case 16
                            case 24:
                                {
                                    type_ = input.readEnum();
                                    bitField0_ |= 0x00000004;
                                    break;
                                } // case 24
                            case 34:
                                {
                                    data_ = input.readBytes();
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

            private long minTimeMs_;

            /**
             * <code>int64 min_time_ms = 1;</code>
             *
             * @return The minTimeMs.
             */
            @Override
            public long getMinTimeMs() {
                return minTimeMs_;
            }

            /**
             * <code>int64 min_time_ms = 1;</code>
             *
             * @param value The minTimeMs to set.
             * @return This builder for chaining.
             */
            public Builder setMinTimeMs(long value) {

                minTimeMs_ = value;
                bitField0_ |= 0x00000001;
                onChanged();
                return this;
            }

            /**
             * <code>int64 min_time_ms = 1;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearMinTimeMs() {
                bitField0_ = (bitField0_ & ~0x00000001);
                minTimeMs_ = 0L;
                onChanged();
                return this;
            }

            private long maxTimeMs_;

            /**
             * <code>int64 max_time_ms = 2;</code>
             *
             * @return The maxTimeMs.
             */
            @Override
            public long getMaxTimeMs() {
                return maxTimeMs_;
            }

            /**
             * <code>int64 max_time_ms = 2;</code>
             *
             * @param value The maxTimeMs to set.
             * @return This builder for chaining.
             */
            public Builder setMaxTimeMs(long value) {

                maxTimeMs_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            /**
             * <code>int64 max_time_ms = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearMaxTimeMs() {
                bitField0_ = (bitField0_ & ~0x00000002);
                maxTimeMs_ = 0L;
                onChanged();
                return this;
            }

            private int type_ = 0;

            /**
             * <code>.prometheus.Chunk.Encoding type = 3;</code>
             *
             * @return The enum numeric value on the wire for type.
             */
            @Override
            public int getTypeValue() {
                return type_;
            }

            /**
             * <code>.prometheus.Chunk.Encoding type = 3;</code>
             *
             * @param value The enum numeric value on the wire for type to set.
             * @return This builder for chaining.
             */
            public Builder setTypeValue(int value) {
                type_ = value;
                bitField0_ |= 0x00000004;
                onChanged();
                return this;
            }

            /**
             * <code>.prometheus.Chunk.Encoding type = 3;</code>
             *
             * @return The type.
             */
            @Override
            public Types.Chunk.Encoding getType() {
                Types.Chunk.Encoding result = Types.Chunk.Encoding.forNumber(type_);
                return result == null ? Types.Chunk.Encoding.UNRECOGNIZED : result;
            }

            /**
             * <code>.prometheus.Chunk.Encoding type = 3;</code>
             *
             * @param value The type to set.
             * @return This builder for chaining.
             */
            public Builder setType(Types.Chunk.Encoding value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                bitField0_ |= 0x00000004;
                type_ = value.getNumber();
                onChanged();
                return this;
            }

            /**
             * <code>.prometheus.Chunk.Encoding type = 3;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearType() {
                bitField0_ = (bitField0_ & ~0x00000004);
                type_ = 0;
                onChanged();
                return this;
            }

            private com.google.protobuf.ByteString data_ = com.google.protobuf.ByteString.EMPTY;

            /**
             * <code>bytes data = 4;</code>
             *
             * @return The data.
             */
            @Override
            public com.google.protobuf.ByteString getData() {
                return data_;
            }

            /**
             * <code>bytes data = 4;</code>
             *
             * @param value The data to set.
             * @return This builder for chaining.
             */
            public Builder setData(com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                data_ = value;
                bitField0_ |= 0x00000008;
                onChanged();
                return this;
            }

            /**
             * <code>bytes data = 4;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearData() {
                bitField0_ = (bitField0_ & ~0x00000008);
                data_ = getDefaultInstance().getData();
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

            // @@protoc_insertion_point(builder_scope:prometheus.Chunk)
        }

        // @@protoc_insertion_point(class_scope:prometheus.Chunk)
        private static final Types.Chunk DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Types.Chunk();
        }

        public static Types.Chunk getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<Chunk> PARSER =
                new com.google.protobuf.AbstractParser<Chunk>() {
                    @Override
                    public Chunk parsePartialFrom(
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

        public static com.google.protobuf.Parser<Chunk> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<Chunk> getParserForType() {
            return PARSER;
        }

        @Override
        public Types.Chunk getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    public interface ChunkedSeriesOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:prometheus.ChunkedSeries)
            com.google.protobuf.MessageOrBuilder {

        /**
         *
         *
         * <pre>
         * Labels should be sorted.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        java.util.List<Types.Label> getLabelsList();

        /**
         *
         *
         * <pre>
         * Labels should be sorted.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        Types.Label getLabels(int index);

        /**
         *
         *
         * <pre>
         * Labels should be sorted.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        int getLabelsCount();

        /**
         *
         *
         * <pre>
         * Labels should be sorted.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        java.util.List<? extends Types.LabelOrBuilder> getLabelsOrBuilderList();

        /**
         *
         *
         * <pre>
         * Labels should be sorted.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        Types.LabelOrBuilder getLabelsOrBuilder(int index);

        /**
         *
         *
         * <pre>
         * Chunks will be in start time order and may overlap.
         * </pre>
         *
         * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
         */
        java.util.List<Types.Chunk> getChunksList();

        /**
         *
         *
         * <pre>
         * Chunks will be in start time order and may overlap.
         * </pre>
         *
         * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
         */
        Types.Chunk getChunks(int index);

        /**
         *
         *
         * <pre>
         * Chunks will be in start time order and may overlap.
         * </pre>
         *
         * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
         */
        int getChunksCount();

        /**
         *
         *
         * <pre>
         * Chunks will be in start time order and may overlap.
         * </pre>
         *
         * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
         */
        java.util.List<? extends Types.ChunkOrBuilder> getChunksOrBuilderList();

        /**
         *
         *
         * <pre>
         * Chunks will be in start time order and may overlap.
         * </pre>
         *
         * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
         */
        Types.ChunkOrBuilder getChunksOrBuilder(int index);
    }

    /**
     *
     *
     * <pre>
     * ChunkedSeries represents single, encoded time series.
     * </pre>
     *
     * <p>Protobuf type {@code prometheus.ChunkedSeries}
     */
    public static final class ChunkedSeries extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:prometheus.ChunkedSeries)
            ChunkedSeriesOrBuilder {
        private static final long serialVersionUID = 0L;

        // Use ChunkedSeries.newBuilder() to construct.
        private ChunkedSeries(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private ChunkedSeries() {
            labels_ = java.util.Collections.emptyList();
            chunks_ = java.util.Collections.emptyList();
        }

        @Override
        @SuppressWarnings({"unused"})
        protected Object newInstance(UnusedPrivateParameter unused) {
            return new ChunkedSeries();
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return Types.internal_static_prometheus_ChunkedSeries_descriptor;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return Types.internal_static_prometheus_ChunkedSeries_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            Types.ChunkedSeries.class, Types.ChunkedSeries.Builder.class);
        }

        public static final int LABELS_FIELD_NUMBER = 1;

        @SuppressWarnings("serial")
        private java.util.List<Types.Label> labels_;

        /**
         *
         *
         * <pre>
         * Labels should be sorted.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public java.util.List<Types.Label> getLabelsList() {
            return labels_;
        }

        /**
         *
         *
         * <pre>
         * Labels should be sorted.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public java.util.List<? extends Types.LabelOrBuilder> getLabelsOrBuilderList() {
            return labels_;
        }

        /**
         *
         *
         * <pre>
         * Labels should be sorted.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public int getLabelsCount() {
            return labels_.size();
        }

        /**
         *
         *
         * <pre>
         * Labels should be sorted.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public Types.Label getLabels(int index) {
            return labels_.get(index);
        }

        /**
         *
         *
         * <pre>
         * Labels should be sorted.
         * </pre>
         *
         * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public Types.LabelOrBuilder getLabelsOrBuilder(int index) {
            return labels_.get(index);
        }

        public static final int CHUNKS_FIELD_NUMBER = 2;

        @SuppressWarnings("serial")
        private java.util.List<Types.Chunk> chunks_;

        /**
         *
         *
         * <pre>
         * Chunks will be in start time order and may overlap.
         * </pre>
         *
         * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public java.util.List<Types.Chunk> getChunksList() {
            return chunks_;
        }

        /**
         *
         *
         * <pre>
         * Chunks will be in start time order and may overlap.
         * </pre>
         *
         * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public java.util.List<? extends Types.ChunkOrBuilder> getChunksOrBuilderList() {
            return chunks_;
        }

        /**
         *
         *
         * <pre>
         * Chunks will be in start time order and may overlap.
         * </pre>
         *
         * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public int getChunksCount() {
            return chunks_.size();
        }

        /**
         *
         *
         * <pre>
         * Chunks will be in start time order and may overlap.
         * </pre>
         *
         * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public Types.Chunk getChunks(int index) {
            return chunks_.get(index);
        }

        /**
         *
         *
         * <pre>
         * Chunks will be in start time order and may overlap.
         * </pre>
         *
         * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
         */
        @Override
        public Types.ChunkOrBuilder getChunksOrBuilder(int index) {
            return chunks_.get(index);
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
            for (int i = 0; i < labels_.size(); i++) {
                output.writeMessage(1, labels_.get(i));
            }
            for (int i = 0; i < chunks_.size(); i++) {
                output.writeMessage(2, chunks_.get(i));
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
            for (int i = 0; i < labels_.size(); i++) {
                size += com.google.protobuf.CodedOutputStream.computeMessageSize(1, labels_.get(i));
            }
            for (int i = 0; i < chunks_.size(); i++) {
                size += com.google.protobuf.CodedOutputStream.computeMessageSize(2, chunks_.get(i));
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
            if (!(obj instanceof Types.ChunkedSeries)) {
                return super.equals(obj);
            }
            Types.ChunkedSeries other = (Types.ChunkedSeries) obj;

            if (!getLabelsList().equals(other.getLabelsList())) {
                return false;
            }
            if (!getChunksList().equals(other.getChunksList())) {
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
            if (getLabelsCount() > 0) {
                hash = (37 * hash) + LABELS_FIELD_NUMBER;
                hash = (53 * hash) + getLabelsList().hashCode();
            }
            if (getChunksCount() > 0) {
                hash = (37 * hash) + CHUNKS_FIELD_NUMBER;
                hash = (53 * hash) + getChunksList().hashCode();
            }
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static Types.ChunkedSeries parseFrom(java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.ChunkedSeries parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.ChunkedSeries parseFrom(com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.ChunkedSeries parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.ChunkedSeries parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static Types.ChunkedSeries parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static Types.ChunkedSeries parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.ChunkedSeries parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.ChunkedSeries parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static Types.ChunkedSeries parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static Types.ChunkedSeries parseFrom(com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static Types.ChunkedSeries parseFrom(
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

        public static Builder newBuilder(Types.ChunkedSeries prototype) {
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
         * ChunkedSeries represents single, encoded time series.
         * </pre>
         *
         * <p>Protobuf type {@code prometheus.ChunkedSeries}
         */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:prometheus.ChunkedSeries)
                Types.ChunkedSeriesOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return Types.internal_static_prometheus_ChunkedSeries_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable() {
                return Types.internal_static_prometheus_ChunkedSeries_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                Types.ChunkedSeries.class, Types.ChunkedSeries.Builder.class);
            }

            // Construct using Types.ChunkedSeries.newBuilder()
            private Builder() {}

            private Builder(BuilderParent parent) {
                super(parent);
            }

            @Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                if (labelsBuilder_ == null) {
                    labels_ = java.util.Collections.emptyList();
                } else {
                    labels_ = null;
                    labelsBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000001);
                if (chunksBuilder_ == null) {
                    chunks_ = java.util.Collections.emptyList();
                } else {
                    chunks_ = null;
                    chunksBuilder_.clear();
                }
                bitField0_ = (bitField0_ & ~0x00000002);
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return Types.internal_static_prometheus_ChunkedSeries_descriptor;
            }

            @Override
            public Types.ChunkedSeries getDefaultInstanceForType() {
                return Types.ChunkedSeries.getDefaultInstance();
            }

            @Override
            public Types.ChunkedSeries build() {
                Types.ChunkedSeries result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public Types.ChunkedSeries buildPartial() {
                Types.ChunkedSeries result = new Types.ChunkedSeries(this);
                buildPartialRepeatedFields(result);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartialRepeatedFields(Types.ChunkedSeries result) {
                if (labelsBuilder_ == null) {
                    if (((bitField0_ & 0x00000001) != 0)) {
                        labels_ = java.util.Collections.unmodifiableList(labels_);
                        bitField0_ = (bitField0_ & ~0x00000001);
                    }
                    result.labels_ = labels_;
                } else {
                    result.labels_ = labelsBuilder_.build();
                }
                if (chunksBuilder_ == null) {
                    if (((bitField0_ & 0x00000002) != 0)) {
                        chunks_ = java.util.Collections.unmodifiableList(chunks_);
                        bitField0_ = (bitField0_ & ~0x00000002);
                    }
                    result.chunks_ = chunks_;
                } else {
                    result.chunks_ = chunksBuilder_.build();
                }
            }

            private void buildPartial0(Types.ChunkedSeries result) {
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
                if (other instanceof Types.ChunkedSeries) {
                    return mergeFrom((Types.ChunkedSeries) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(Types.ChunkedSeries other) {
                if (other == Types.ChunkedSeries.getDefaultInstance()) {
                    return this;
                }
                if (labelsBuilder_ == null) {
                    if (!other.labels_.isEmpty()) {
                        if (labels_.isEmpty()) {
                            labels_ = other.labels_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                        } else {
                            ensureLabelsIsMutable();
                            labels_.addAll(other.labels_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.labels_.isEmpty()) {
                        if (labelsBuilder_.isEmpty()) {
                            labelsBuilder_.dispose();
                            labelsBuilder_ = null;
                            labels_ = other.labels_;
                            bitField0_ = (bitField0_ & ~0x00000001);
                            labelsBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getLabelsFieldBuilder()
                                            : null;
                        } else {
                            labelsBuilder_.addAllMessages(other.labels_);
                        }
                    }
                }
                if (chunksBuilder_ == null) {
                    if (!other.chunks_.isEmpty()) {
                        if (chunks_.isEmpty()) {
                            chunks_ = other.chunks_;
                            bitField0_ = (bitField0_ & ~0x00000002);
                        } else {
                            ensureChunksIsMutable();
                            chunks_.addAll(other.chunks_);
                        }
                        onChanged();
                    }
                } else {
                    if (!other.chunks_.isEmpty()) {
                        if (chunksBuilder_.isEmpty()) {
                            chunksBuilder_.dispose();
                            chunksBuilder_ = null;
                            chunks_ = other.chunks_;
                            bitField0_ = (bitField0_ & ~0x00000002);
                            chunksBuilder_ =
                                    com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                                            ? getChunksFieldBuilder()
                                            : null;
                        } else {
                            chunksBuilder_.addAllMessages(other.chunks_);
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
                                    Types.Label m =
                                            input.readMessage(
                                                    Types.Label.parser(), extensionRegistry);
                                    if (labelsBuilder_ == null) {
                                        ensureLabelsIsMutable();
                                        labels_.add(m);
                                    } else {
                                        labelsBuilder_.addMessage(m);
                                    }
                                    break;
                                } // case 10
                            case 18:
                                {
                                    Types.Chunk m =
                                            input.readMessage(
                                                    Types.Chunk.parser(), extensionRegistry);
                                    if (chunksBuilder_ == null) {
                                        ensureChunksIsMutable();
                                        chunks_.add(m);
                                    } else {
                                        chunksBuilder_.addMessage(m);
                                    }
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

            private java.util.List<Types.Label> labels_ = java.util.Collections.emptyList();

            private void ensureLabelsIsMutable() {
                if (!((bitField0_ & 0x00000001) != 0)) {
                    labels_ = new java.util.ArrayList<Types.Label>(labels_);
                    bitField0_ |= 0x00000001;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Label, Types.Label.Builder, Types.LabelOrBuilder>
                    labelsBuilder_;

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<Types.Label> getLabelsList() {
                if (labelsBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(labels_);
                } else {
                    return labelsBuilder_.getMessageList();
                }
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public int getLabelsCount() {
                if (labelsBuilder_ == null) {
                    return labels_.size();
                } else {
                    return labelsBuilder_.getCount();
                }
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label getLabels(int index) {
                if (labelsBuilder_ == null) {
                    return labels_.get(index);
                } else {
                    return labelsBuilder_.getMessage(index);
                }
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder setLabels(int index, Types.Label value) {
                if (labelsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureLabelsIsMutable();
                    labels_.set(index, value);
                    onChanged();
                } else {
                    labelsBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder setLabels(int index, Types.Label.Builder builderForValue) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    labelsBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(Types.Label value) {
                if (labelsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureLabelsIsMutable();
                    labels_.add(value);
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(int index, Types.Label value) {
                if (labelsBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureLabelsIsMutable();
                    labels_.add(index, value);
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(Types.Label.Builder builderForValue) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.add(builderForValue.build());
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addLabels(int index, Types.Label.Builder builderForValue) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    labelsBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addAllLabels(Iterable<? extends Types.Label> values) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, labels_);
                    onChanged();
                } else {
                    labelsBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder clearLabels() {
                if (labelsBuilder_ == null) {
                    labels_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000001);
                    onChanged();
                } else {
                    labelsBuilder_.clear();
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Builder removeLabels(int index) {
                if (labelsBuilder_ == null) {
                    ensureLabelsIsMutable();
                    labels_.remove(index);
                    onChanged();
                } else {
                    labelsBuilder_.remove(index);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label.Builder getLabelsBuilder(int index) {
                return getLabelsFieldBuilder().getBuilder(index);
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.LabelOrBuilder getLabelsOrBuilder(int index) {
                if (labelsBuilder_ == null) {
                    return labels_.get(index);
                } else {
                    return labelsBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<? extends Types.LabelOrBuilder> getLabelsOrBuilderList() {
                if (labelsBuilder_ != null) {
                    return labelsBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(labels_);
                }
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label.Builder addLabelsBuilder() {
                return getLabelsFieldBuilder().addBuilder(Types.Label.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Label.Builder addLabelsBuilder(int index) {
                return getLabelsFieldBuilder().addBuilder(index, Types.Label.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * Labels should be sorted.
             * </pre>
             *
             * <code>repeated .prometheus.Label labels = 1 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<Types.Label.Builder> getLabelsBuilderList() {
                return getLabelsFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Label, Types.Label.Builder, Types.LabelOrBuilder>
                    getLabelsFieldBuilder() {
                if (labelsBuilder_ == null) {
                    labelsBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.Label, Types.Label.Builder, Types.LabelOrBuilder>(
                                    labels_,
                                    ((bitField0_ & 0x00000001) != 0),
                                    getParentForChildren(),
                                    isClean());
                    labels_ = null;
                }
                return labelsBuilder_;
            }

            private java.util.List<Types.Chunk> chunks_ = java.util.Collections.emptyList();

            private void ensureChunksIsMutable() {
                if (!((bitField0_ & 0x00000002) != 0)) {
                    chunks_ = new java.util.ArrayList<Types.Chunk>(chunks_);
                    bitField0_ |= 0x00000002;
                }
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Chunk, Types.Chunk.Builder, Types.ChunkOrBuilder>
                    chunksBuilder_;

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<Types.Chunk> getChunksList() {
                if (chunksBuilder_ == null) {
                    return java.util.Collections.unmodifiableList(chunks_);
                } else {
                    return chunksBuilder_.getMessageList();
                }
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public int getChunksCount() {
                if (chunksBuilder_ == null) {
                    return chunks_.size();
                } else {
                    return chunksBuilder_.getCount();
                }
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Chunk getChunks(int index) {
                if (chunksBuilder_ == null) {
                    return chunks_.get(index);
                } else {
                    return chunksBuilder_.getMessage(index);
                }
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder setChunks(int index, Types.Chunk value) {
                if (chunksBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureChunksIsMutable();
                    chunks_.set(index, value);
                    onChanged();
                } else {
                    chunksBuilder_.setMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder setChunks(int index, Types.Chunk.Builder builderForValue) {
                if (chunksBuilder_ == null) {
                    ensureChunksIsMutable();
                    chunks_.set(index, builderForValue.build());
                    onChanged();
                } else {
                    chunksBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addChunks(Types.Chunk value) {
                if (chunksBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureChunksIsMutable();
                    chunks_.add(value);
                    onChanged();
                } else {
                    chunksBuilder_.addMessage(value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addChunks(int index, Types.Chunk value) {
                if (chunksBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureChunksIsMutable();
                    chunks_.add(index, value);
                    onChanged();
                } else {
                    chunksBuilder_.addMessage(index, value);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addChunks(Types.Chunk.Builder builderForValue) {
                if (chunksBuilder_ == null) {
                    ensureChunksIsMutable();
                    chunks_.add(builderForValue.build());
                    onChanged();
                } else {
                    chunksBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addChunks(int index, Types.Chunk.Builder builderForValue) {
                if (chunksBuilder_ == null) {
                    ensureChunksIsMutable();
                    chunks_.add(index, builderForValue.build());
                    onChanged();
                } else {
                    chunksBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder addAllChunks(Iterable<? extends Types.Chunk> values) {
                if (chunksBuilder_ == null) {
                    ensureChunksIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(values, chunks_);
                    onChanged();
                } else {
                    chunksBuilder_.addAllMessages(values);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder clearChunks() {
                if (chunksBuilder_ == null) {
                    chunks_ = java.util.Collections.emptyList();
                    bitField0_ = (bitField0_ & ~0x00000002);
                    onChanged();
                } else {
                    chunksBuilder_.clear();
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Builder removeChunks(int index) {
                if (chunksBuilder_ == null) {
                    ensureChunksIsMutable();
                    chunks_.remove(index);
                    onChanged();
                } else {
                    chunksBuilder_.remove(index);
                }
                return this;
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Chunk.Builder getChunksBuilder(int index) {
                return getChunksFieldBuilder().getBuilder(index);
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Types.ChunkOrBuilder getChunksOrBuilder(int index) {
                if (chunksBuilder_ == null) {
                    return chunks_.get(index);
                } else {
                    return chunksBuilder_.getMessageOrBuilder(index);
                }
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<? extends Types.ChunkOrBuilder> getChunksOrBuilderList() {
                if (chunksBuilder_ != null) {
                    return chunksBuilder_.getMessageOrBuilderList();
                } else {
                    return java.util.Collections.unmodifiableList(chunks_);
                }
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Chunk.Builder addChunksBuilder() {
                return getChunksFieldBuilder().addBuilder(Types.Chunk.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public Types.Chunk.Builder addChunksBuilder(int index) {
                return getChunksFieldBuilder().addBuilder(index, Types.Chunk.getDefaultInstance());
            }

            /**
             *
             *
             * <pre>
             * Chunks will be in start time order and may overlap.
             * </pre>
             *
             * <code>repeated .prometheus.Chunk chunks = 2 [(.gogoproto.nullable) = false];</code>
             */
            public java.util.List<Types.Chunk.Builder> getChunksBuilderList() {
                return getChunksFieldBuilder().getBuilderList();
            }

            private com.google.protobuf.RepeatedFieldBuilderV3<
                            Types.Chunk, Types.Chunk.Builder, Types.ChunkOrBuilder>
                    getChunksFieldBuilder() {
                if (chunksBuilder_ == null) {
                    chunksBuilder_ =
                            new com.google.protobuf.RepeatedFieldBuilderV3<
                                    Types.Chunk, Types.Chunk.Builder, Types.ChunkOrBuilder>(
                                    chunks_,
                                    ((bitField0_ & 0x00000002) != 0),
                                    getParentForChildren(),
                                    isClean());
                    chunks_ = null;
                }
                return chunksBuilder_;
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

            // @@protoc_insertion_point(builder_scope:prometheus.ChunkedSeries)
        }

        // @@protoc_insertion_point(class_scope:prometheus.ChunkedSeries)
        private static final Types.ChunkedSeries DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE = new Types.ChunkedSeries();
        }

        public static Types.ChunkedSeries getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<ChunkedSeries> PARSER =
                new com.google.protobuf.AbstractParser<ChunkedSeries>() {
                    @Override
                    public ChunkedSeries parsePartialFrom(
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

        public static com.google.protobuf.Parser<ChunkedSeries> parser() {
            return PARSER;
        }

        @Override
        public com.google.protobuf.Parser<ChunkedSeries> getParserForType() {
            return PARSER;
        }

        @Override
        public Types.ChunkedSeries getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_MetricMetadata_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_MetricMetadata_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_Sample_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_Sample_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_Exemplar_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_Exemplar_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_Histogram_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_Histogram_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_BucketSpan_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_BucketSpan_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_TimeSeries_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_TimeSeries_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_Label_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_Label_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_Labels_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_Labels_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_LabelMatcher_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_LabelMatcher_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_ReadHints_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_ReadHints_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_Chunk_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_Chunk_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_prometheus_ChunkedSeries_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_prometheus_ChunkedSeries_fieldAccessorTable;

    public static com.google.protobuf.Descriptors.FileDescriptor getDescriptor() {
        return descriptor;
    }

    private static com.google.protobuf.Descriptors.FileDescriptor descriptor;

    static {
        String[] descriptorData = {
            "\n\013types.proto\022\nprometheus\032\ngogo.proto\"\370\001"
                    + "\n\016MetricMetadata\0223\n\004type\030\001 \001(\0162%.prometh"
                    + "eus.MetricMetadata.MetricType\022\032\n\022metric_"
                    + "family_name\030\002 \001(\t\022\014\n\004help\030\004 \001(\t\022\014\n\004unit\030"
                    + "\005 \001(\t\"y\n\nMetricType\022\013\n\007UNKNOWN\020\000\022\013\n\007COUN"
                    + "TER\020\001\022\t\n\005GAUGE\020\002\022\r\n\tHISTOGRAM\020\003\022\022\n\016GAUGE"
                    + "HISTOGRAM\020\004\022\013\n\007SUMMARY\020\005\022\010\n\004INFO\020\006\022\014\n\010ST"
                    + "ATESET\020\007\"*\n\006Sample\022\r\n\005value\030\001 \001(\001\022\021\n\ttim"
                    + "estamp\030\002 \001(\003\"U\n\010Exemplar\022\'\n\006labels\030\001 \003(\013"
                    + "2\021.prometheus.LabelB\004\310\336\037\000\022\r\n\005value\030\002 \001(\001"
                    + "\022\021\n\ttimestamp\030\003 \001(\003\"\207\004\n\tHistogram\022\023\n\tcou"
                    + "nt_int\030\001 \001(\004H\000\022\025\n\013count_float\030\002 \001(\001H\000\022\013\n"
                    + "\003sum\030\003 \001(\001\022\016\n\006schema\030\004 \001(\021\022\026\n\016zero_thres"
                    + "hold\030\005 \001(\001\022\030\n\016zero_count_int\030\006 \001(\004H\001\022\032\n\020"
                    + "zero_count_float\030\007 \001(\001H\001\0224\n\016negative_spa"
                    + "ns\030\010 \003(\0132\026.prometheus.BucketSpanB\004\310\336\037\000\022\027"
                    + "\n\017negative_deltas\030\t \003(\022\022\027\n\017negative_coun"
                    + "ts\030\n \003(\001\0224\n\016positive_spans\030\013 \003(\0132\026.prome"
                    + "theus.BucketSpanB\004\310\336\037\000\022\027\n\017positive_delta"
                    + "s\030\014 \003(\022\022\027\n\017positive_counts\030\r \003(\001\0223\n\nrese"
                    + "t_hint\030\016 \001(\0162\037.prometheus.Histogram.Rese"
                    + "tHint\022\021\n\ttimestamp\030\017 \001(\003\"4\n\tResetHint\022\013\n"
                    + "\007UNKNOWN\020\000\022\007\n\003YES\020\001\022\006\n\002NO\020\002\022\t\n\005GAUGE\020\003B\007"
                    + "\n\005countB\014\n\nzero_count\",\n\nBucketSpan\022\016\n\006o"
                    + "ffset\030\001 \001(\021\022\016\n\006length\030\002 \001(\r\"\300\001\n\nTimeSeri"
                    + "es\022\'\n\006labels\030\001 \003(\0132\021.prometheus.LabelB\004\310"
                    + "\336\037\000\022)\n\007samples\030\002 \003(\0132\022.prometheus.Sample"
                    + "B\004\310\336\037\000\022-\n\texemplars\030\003 \003(\0132\024.prometheus.E"
                    + "xemplarB\004\310\336\037\000\022/\n\nhistograms\030\004 \003(\0132\025.prom"
                    + "etheus.HistogramB\004\310\336\037\000\"$\n\005Label\022\014\n\004name\030"
                    + "\001 \001(\t\022\r\n\005value\030\002 \001(\t\"1\n\006Labels\022\'\n\006labels"
                    + "\030\001 \003(\0132\021.prometheus.LabelB\004\310\336\037\000\"\202\001\n\014Labe"
                    + "lMatcher\022+\n\004type\030\001 \001(\0162\035.prometheus.Labe"
                    + "lMatcher.Type\022\014\n\004name\030\002 \001(\t\022\r\n\005value\030\003 \001"
                    + "(\t\"(\n\004Type\022\006\n\002EQ\020\000\022\007\n\003NEQ\020\001\022\006\n\002RE\020\002\022\007\n\003N"
                    + "RE\020\003\"|\n\tReadHints\022\017\n\007step_ms\030\001 \001(\003\022\014\n\004fu"
                    + "nc\030\002 \001(\t\022\020\n\010start_ms\030\003 \001(\003\022\016\n\006end_ms\030\004 \001"
                    + "(\003\022\020\n\010grouping\030\005 \003(\t\022\n\n\002by\030\006 \001(\010\022\020\n\010rang"
                    + "e_ms\030\007 \001(\003\"\257\001\n\005Chunk\022\023\n\013min_time_ms\030\001 \001("
                    + "\003\022\023\n\013max_time_ms\030\002 \001(\003\022(\n\004type\030\003 \001(\0162\032.p"
                    + "rometheus.Chunk.Encoding\022\014\n\004data\030\004 \001(\014\"D"
                    + "\n\010Encoding\022\013\n\007UNKNOWN\020\000\022\007\n\003XOR\020\001\022\r\n\tHIST"
                    + "OGRAM\020\002\022\023\n\017FLOAT_HISTOGRAM\020\003\"a\n\rChunkedS"
                    + "eries\022\'\n\006labels\030\001 \003(\0132\021.prometheus.Label"
                    + "B\004\310\336\037\000\022\'\n\006chunks\030\002 \003(\0132\021.prometheus.Chun"
                    + "kB\004\310\336\037\000B\010Z\006prompbb\006proto3"
        };
        descriptor =
                com.google.protobuf.Descriptors.FileDescriptor.internalBuildGeneratedFileFrom(
                        descriptorData,
                        new com.google.protobuf.Descriptors.FileDescriptor[] {
                            GoGoProtos.getDescriptor(),
                        });
        internal_static_prometheus_MetricMetadata_descriptor =
                getDescriptor().getMessageTypes().get(0);
        internal_static_prometheus_MetricMetadata_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_MetricMetadata_descriptor,
                        new String[] {
                            "Type", "MetricFamilyName", "Help", "Unit",
                        });
        internal_static_prometheus_Sample_descriptor = getDescriptor().getMessageTypes().get(1);
        internal_static_prometheus_Sample_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_Sample_descriptor,
                        new String[] {
                            "Value", "Timestamp",
                        });
        internal_static_prometheus_Exemplar_descriptor = getDescriptor().getMessageTypes().get(2);
        internal_static_prometheus_Exemplar_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_Exemplar_descriptor,
                        new String[] {
                            "Labels", "Value", "Timestamp",
                        });
        internal_static_prometheus_Histogram_descriptor = getDescriptor().getMessageTypes().get(3);
        internal_static_prometheus_Histogram_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_Histogram_descriptor,
                        new String[] {
                            "CountInt",
                            "CountFloat",
                            "Sum",
                            "Schema",
                            "ZeroThreshold",
                            "ZeroCountInt",
                            "ZeroCountFloat",
                            "NegativeSpans",
                            "NegativeDeltas",
                            "NegativeCounts",
                            "PositiveSpans",
                            "PositiveDeltas",
                            "PositiveCounts",
                            "ResetHint",
                            "Timestamp",
                            "Count",
                            "ZeroCount",
                        });
        internal_static_prometheus_BucketSpan_descriptor = getDescriptor().getMessageTypes().get(4);
        internal_static_prometheus_BucketSpan_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_BucketSpan_descriptor,
                        new String[] {
                            "Offset", "Length",
                        });
        internal_static_prometheus_TimeSeries_descriptor = getDescriptor().getMessageTypes().get(5);
        internal_static_prometheus_TimeSeries_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_TimeSeries_descriptor,
                        new String[] {
                            "Labels", "Samples", "Exemplars", "Histograms",
                        });
        internal_static_prometheus_Label_descriptor = getDescriptor().getMessageTypes().get(6);
        internal_static_prometheus_Label_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_Label_descriptor,
                        new String[] {
                            "Name", "Value",
                        });
        internal_static_prometheus_Labels_descriptor = getDescriptor().getMessageTypes().get(7);
        internal_static_prometheus_Labels_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_Labels_descriptor,
                        new String[] {
                            "Labels",
                        });
        internal_static_prometheus_LabelMatcher_descriptor =
                getDescriptor().getMessageTypes().get(8);
        internal_static_prometheus_LabelMatcher_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_LabelMatcher_descriptor,
                        new String[] {
                            "Type", "Name", "Value",
                        });
        internal_static_prometheus_ReadHints_descriptor = getDescriptor().getMessageTypes().get(9);
        internal_static_prometheus_ReadHints_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_ReadHints_descriptor,
                        new String[] {
                            "StepMs", "Func", "StartMs", "EndMs", "Grouping", "By", "RangeMs",
                        });
        internal_static_prometheus_Chunk_descriptor = getDescriptor().getMessageTypes().get(10);
        internal_static_prometheus_Chunk_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_Chunk_descriptor,
                        new String[] {
                            "MinTimeMs", "MaxTimeMs", "Type", "Data",
                        });
        internal_static_prometheus_ChunkedSeries_descriptor =
                getDescriptor().getMessageTypes().get(11);
        internal_static_prometheus_ChunkedSeries_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_prometheus_ChunkedSeries_descriptor,
                        new String[] {
                            "Labels", "Chunks",
                        });
        com.google.protobuf.ExtensionRegistry registry =
                com.google.protobuf.ExtensionRegistry.newInstance();
        registry.add(GoGoProtos.nullable);
        com.google.protobuf.Descriptors.FileDescriptor.internalUpdateFileDescriptor(
                descriptor, registry);
        GoGoProtos.getDescriptor();
    }

    // @@protoc_insertion_point(outer_class_scope)
}
