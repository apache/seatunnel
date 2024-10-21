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

public final class GoGoProtos {
    private GoGoProtos() {}

    public static void registerAllExtensions(com.google.protobuf.ExtensionRegistryLite registry) {
        registry.add(GoGoProtos.goprotoEnumPrefix);
        registry.add(GoGoProtos.goprotoEnumStringer);
        registry.add(GoGoProtos.enumStringer);
        registry.add(GoGoProtos.enumCustomname);
        registry.add(GoGoProtos.enumdecl);
        registry.add(GoGoProtos.enumvalueCustomname);
        registry.add(GoGoProtos.goprotoGettersAll);
        registry.add(GoGoProtos.goprotoEnumPrefixAll);
        registry.add(GoGoProtos.goprotoStringerAll);
        registry.add(GoGoProtos.verboseEqualAll);
        registry.add(GoGoProtos.faceAll);
        registry.add(GoGoProtos.gostringAll);
        registry.add(GoGoProtos.populateAll);
        registry.add(GoGoProtos.stringerAll);
        registry.add(GoGoProtos.onlyoneAll);
        registry.add(GoGoProtos.equalAll);
        registry.add(GoGoProtos.descriptionAll);
        registry.add(GoGoProtos.testgenAll);
        registry.add(GoGoProtos.benchgenAll);
        registry.add(GoGoProtos.marshalerAll);
        registry.add(GoGoProtos.unmarshalerAll);
        registry.add(GoGoProtos.stableMarshalerAll);
        registry.add(GoGoProtos.sizerAll);
        registry.add(GoGoProtos.goprotoEnumStringerAll);
        registry.add(GoGoProtos.enumStringerAll);
        registry.add(GoGoProtos.unsafeMarshalerAll);
        registry.add(GoGoProtos.unsafeUnmarshalerAll);
        registry.add(GoGoProtos.goprotoExtensionsMapAll);
        registry.add(GoGoProtos.goprotoUnrecognizedAll);
        registry.add(GoGoProtos.gogoprotoImport);
        registry.add(GoGoProtos.protosizerAll);
        registry.add(GoGoProtos.compareAll);
        registry.add(GoGoProtos.typedeclAll);
        registry.add(GoGoProtos.enumdeclAll);
        registry.add(GoGoProtos.goprotoRegistration);
        registry.add(GoGoProtos.messagenameAll);
        registry.add(GoGoProtos.goprotoSizecacheAll);
        registry.add(GoGoProtos.goprotoUnkeyedAll);
        registry.add(GoGoProtos.goprotoGetters);
        registry.add(GoGoProtos.goprotoStringer);
        registry.add(GoGoProtos.verboseEqual);
        registry.add(GoGoProtos.face);
        registry.add(GoGoProtos.gostring);
        registry.add(GoGoProtos.populate);
        registry.add(GoGoProtos.stringer);
        registry.add(GoGoProtos.onlyone);
        registry.add(GoGoProtos.equal);
        registry.add(GoGoProtos.description);
        registry.add(GoGoProtos.testgen);
        registry.add(GoGoProtos.benchgen);
        registry.add(GoGoProtos.marshaler);
        registry.add(GoGoProtos.unmarshaler);
        registry.add(GoGoProtos.stableMarshaler);
        registry.add(GoGoProtos.sizer);
        registry.add(GoGoProtos.unsafeMarshaler);
        registry.add(GoGoProtos.unsafeUnmarshaler);
        registry.add(GoGoProtos.goprotoExtensionsMap);
        registry.add(GoGoProtos.goprotoUnrecognized);
        registry.add(GoGoProtos.protosizer);
        registry.add(GoGoProtos.compare);
        registry.add(GoGoProtos.typedecl);
        registry.add(GoGoProtos.messagename);
        registry.add(GoGoProtos.goprotoSizecache);
        registry.add(GoGoProtos.goprotoUnkeyed);
        registry.add(GoGoProtos.nullable);
        registry.add(GoGoProtos.embed);
        registry.add(GoGoProtos.customtype);
        registry.add(GoGoProtos.customname);
        registry.add(GoGoProtos.jsontag);
        registry.add(GoGoProtos.moretags);
        registry.add(GoGoProtos.casttype);
        registry.add(GoGoProtos.castkey);
        registry.add(GoGoProtos.castvalue);
        registry.add(GoGoProtos.stdtime);
        registry.add(GoGoProtos.stdduration);
        registry.add(GoGoProtos.wktpointer);
    }

    public static void registerAllExtensions(com.google.protobuf.ExtensionRegistry registry) {
        registerAllExtensions((com.google.protobuf.ExtensionRegistryLite) registry);
    }

    public static final int GOPROTO_ENUM_PREFIX_FIELD_NUMBER = 62001;
    /** <code>extend .google.protobuf.EnumOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.EnumOptions, Boolean>
            goprotoEnumPrefix =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_ENUM_STRINGER_FIELD_NUMBER = 62021;
    /** <code>extend .google.protobuf.EnumOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.EnumOptions, Boolean>
            goprotoEnumStringer =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int ENUM_STRINGER_FIELD_NUMBER = 62022;
    /** <code>extend .google.protobuf.EnumOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.EnumOptions, Boolean>
            enumStringer =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int ENUM_CUSTOMNAME_FIELD_NUMBER = 62023;
    /** <code>extend .google.protobuf.EnumOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.EnumOptions, String>
            enumCustomname =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            String.class, null);

    public static final int ENUMDECL_FIELD_NUMBER = 62024;
    /** <code>extend .google.protobuf.EnumOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.EnumOptions, Boolean>
            enumdecl =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int ENUMVALUE_CUSTOMNAME_FIELD_NUMBER = 66001;
    /** <code>extend .google.protobuf.EnumValueOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.EnumValueOptions, String>
            enumvalueCustomname =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            String.class, null);

    public static final int GOPROTO_GETTERS_ALL_FIELD_NUMBER = 63001;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            goprotoGettersAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_ENUM_PREFIX_ALL_FIELD_NUMBER = 63002;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            goprotoEnumPrefixAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_STRINGER_ALL_FIELD_NUMBER = 63003;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            goprotoStringerAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int VERBOSE_EQUAL_ALL_FIELD_NUMBER = 63004;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            verboseEqualAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int FACE_ALL_FIELD_NUMBER = 63005;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            faceAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOSTRING_ALL_FIELD_NUMBER = 63006;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            gostringAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int POPULATE_ALL_FIELD_NUMBER = 63007;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            populateAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int STRINGER_ALL_FIELD_NUMBER = 63008;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            stringerAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int ONLYONE_ALL_FIELD_NUMBER = 63009;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            onlyoneAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int EQUAL_ALL_FIELD_NUMBER = 63013;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            equalAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int DESCRIPTION_ALL_FIELD_NUMBER = 63014;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            descriptionAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int TESTGEN_ALL_FIELD_NUMBER = 63015;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            testgenAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int BENCHGEN_ALL_FIELD_NUMBER = 63016;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            benchgenAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int MARSHALER_ALL_FIELD_NUMBER = 63017;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            marshalerAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int UNMARSHALER_ALL_FIELD_NUMBER = 63018;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            unmarshalerAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int STABLE_MARSHALER_ALL_FIELD_NUMBER = 63019;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            stableMarshalerAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int SIZER_ALL_FIELD_NUMBER = 63020;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            sizerAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_ENUM_STRINGER_ALL_FIELD_NUMBER = 63021;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            goprotoEnumStringerAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int ENUM_STRINGER_ALL_FIELD_NUMBER = 63022;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            enumStringerAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int UNSAFE_MARSHALER_ALL_FIELD_NUMBER = 63023;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            unsafeMarshalerAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int UNSAFE_UNMARSHALER_ALL_FIELD_NUMBER = 63024;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            unsafeUnmarshalerAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_EXTENSIONS_MAP_ALL_FIELD_NUMBER = 63025;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            goprotoExtensionsMapAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_UNRECOGNIZED_ALL_FIELD_NUMBER = 63026;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            goprotoUnrecognizedAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOGOPROTO_IMPORT_FIELD_NUMBER = 63027;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            gogoprotoImport =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int PROTOSIZER_ALL_FIELD_NUMBER = 63028;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            protosizerAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int COMPARE_ALL_FIELD_NUMBER = 63029;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            compareAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int TYPEDECL_ALL_FIELD_NUMBER = 63030;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            typedeclAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int ENUMDECL_ALL_FIELD_NUMBER = 63031;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            enumdeclAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_REGISTRATION_FIELD_NUMBER = 63032;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            goprotoRegistration =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int MESSAGENAME_ALL_FIELD_NUMBER = 63033;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            messagenameAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_SIZECACHE_ALL_FIELD_NUMBER = 63034;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            goprotoSizecacheAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_UNKEYED_ALL_FIELD_NUMBER = 63035;
    /** <code>extend .google.protobuf.FileOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FileOptions, Boolean>
            goprotoUnkeyedAll =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_GETTERS_FIELD_NUMBER = 64001;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            goprotoGetters =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_STRINGER_FIELD_NUMBER = 64003;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            goprotoStringer =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int VERBOSE_EQUAL_FIELD_NUMBER = 64004;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            verboseEqual =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int FACE_FIELD_NUMBER = 64005;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            face =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOSTRING_FIELD_NUMBER = 64006;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            gostring =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int POPULATE_FIELD_NUMBER = 64007;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            populate =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int STRINGER_FIELD_NUMBER = 67008;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            stringer =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int ONLYONE_FIELD_NUMBER = 64009;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            onlyone =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int EQUAL_FIELD_NUMBER = 64013;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            equal =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int DESCRIPTION_FIELD_NUMBER = 64014;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            description =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int TESTGEN_FIELD_NUMBER = 64015;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            testgen =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int BENCHGEN_FIELD_NUMBER = 64016;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            benchgen =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int MARSHALER_FIELD_NUMBER = 64017;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            marshaler =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int UNMARSHALER_FIELD_NUMBER = 64018;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            unmarshaler =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int STABLE_MARSHALER_FIELD_NUMBER = 64019;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            stableMarshaler =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int SIZER_FIELD_NUMBER = 64020;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            sizer =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int UNSAFE_MARSHALER_FIELD_NUMBER = 64023;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            unsafeMarshaler =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int UNSAFE_UNMARSHALER_FIELD_NUMBER = 64024;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            unsafeUnmarshaler =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_EXTENSIONS_MAP_FIELD_NUMBER = 64025;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            goprotoExtensionsMap =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_UNRECOGNIZED_FIELD_NUMBER = 64026;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            goprotoUnrecognized =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int PROTOSIZER_FIELD_NUMBER = 64028;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            protosizer =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int COMPARE_FIELD_NUMBER = 64029;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            compare =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int TYPEDECL_FIELD_NUMBER = 64030;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            typedecl =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int MESSAGENAME_FIELD_NUMBER = 64033;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            messagename =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_SIZECACHE_FIELD_NUMBER = 64034;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            goprotoSizecache =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int GOPROTO_UNKEYED_FIELD_NUMBER = 64035;
    /** <code>extend .google.protobuf.MessageOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.MessageOptions, Boolean>
            goprotoUnkeyed =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int NULLABLE_FIELD_NUMBER = 65001;
    /** <code>extend .google.protobuf.FieldOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FieldOptions, Boolean>
            nullable =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int EMBED_FIELD_NUMBER = 65002;
    /** <code>extend .google.protobuf.FieldOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FieldOptions, Boolean>
            embed =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int CUSTOMTYPE_FIELD_NUMBER = 65003;
    /** <code>extend .google.protobuf.FieldOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FieldOptions, String>
            customtype =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            String.class, null);

    public static final int CUSTOMNAME_FIELD_NUMBER = 65004;
    /** <code>extend .google.protobuf.FieldOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FieldOptions, String>
            customname =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            String.class, null);

    public static final int JSONTAG_FIELD_NUMBER = 65005;
    /** <code>extend .google.protobuf.FieldOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FieldOptions, String>
            jsontag =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            String.class, null);

    public static final int MORETAGS_FIELD_NUMBER = 65006;
    /** <code>extend .google.protobuf.FieldOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FieldOptions, String>
            moretags =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            String.class, null);

    public static final int CASTTYPE_FIELD_NUMBER = 65007;
    /** <code>extend .google.protobuf.FieldOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FieldOptions, String>
            casttype =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            String.class, null);

    public static final int CASTKEY_FIELD_NUMBER = 65008;
    /** <code>extend .google.protobuf.FieldOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FieldOptions, String>
            castkey =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            String.class, null);

    public static final int CASTVALUE_FIELD_NUMBER = 65009;
    /** <code>extend .google.protobuf.FieldOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FieldOptions, String>
            castvalue =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            String.class, null);

    public static final int STDTIME_FIELD_NUMBER = 65010;
    /** <code>extend .google.protobuf.FieldOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FieldOptions, Boolean>
            stdtime =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int STDDURATION_FIELD_NUMBER = 65011;
    /** <code>extend .google.protobuf.FieldOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FieldOptions, Boolean>
            stdduration =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static final int WKTPOINTER_FIELD_NUMBER = 65012;
    /** <code>extend .google.protobuf.FieldOptions { ... }</code> */
    public static final com.google.protobuf.GeneratedMessage.GeneratedExtension<
                    com.google.protobuf.DescriptorProtos.FieldOptions, Boolean>
            wktpointer =
                    com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(
                            Boolean.class, null);

    public static com.google.protobuf.Descriptors.FileDescriptor getDescriptor() {
        return descriptor;
    }

    private static com.google.protobuf.Descriptors.FileDescriptor descriptor;

    static {
        String[] descriptorData = {
            "\n\ngogo.proto\022\tgogoproto\032 google/protobuf"
                    + "/descriptor.proto:;\n\023goproto_enum_prefix"
                    + "\022\034.google.protobuf.EnumOptions\030\261\344\003 \001(\010:="
                    + "\n\025goproto_enum_stringer\022\034.google.protobu"
                    + "f.EnumOptions\030\305\344\003 \001(\010:5\n\renum_stringer\022\034"
                    + ".google.protobuf.EnumOptions\030\306\344\003 \001(\010:7\n\017"
                    + "enum_customname\022\034.google.protobuf.EnumOp"
                    + "tions\030\307\344\003 \001(\t:0\n\010enumdecl\022\034.google.proto"
                    + "buf.EnumOptions\030\310\344\003 \001(\010:A\n\024enumvalue_cus"
                    + "tomname\022!.google.protobuf.EnumValueOptio"
                    + "ns\030\321\203\004 \001(\t:;\n\023goproto_getters_all\022\034.goog"
                    + "le.protobuf.FileOptions\030\231\354\003 \001(\010:?\n\027gopro"
                    + "to_enum_prefix_all\022\034.google.protobuf.Fil"
                    + "eOptions\030\232\354\003 \001(\010:<\n\024goproto_stringer_all"
                    + "\022\034.google.protobuf.FileOptions\030\233\354\003 \001(\010:9"
                    + "\n\021verbose_equal_all\022\034.google.protobuf.Fi"
                    + "leOptions\030\234\354\003 \001(\010:0\n\010face_all\022\034.google.p"
                    + "rotobuf.FileOptions\030\235\354\003 \001(\010:4\n\014gostring_"
                    + "all\022\034.google.protobuf.FileOptions\030\236\354\003 \001("
                    + "\010:4\n\014populate_all\022\034.google.protobuf.File"
                    + "Options\030\237\354\003 \001(\010:4\n\014stringer_all\022\034.google"
                    + ".protobuf.FileOptions\030\240\354\003 \001(\010:3\n\013onlyone"
                    + "_all\022\034.google.protobuf.FileOptions\030\241\354\003 \001"
                    + "(\010:1\n\tequal_all\022\034.google.protobuf.FileOp"
                    + "tions\030\245\354\003 \001(\010:7\n\017description_all\022\034.googl"
                    + "e.protobuf.FileOptions\030\246\354\003 \001(\010:3\n\013testge"
                    + "n_all\022\034.google.protobuf.FileOptions\030\247\354\003 "
                    + "\001(\010:4\n\014benchgen_all\022\034.google.protobuf.Fi"
                    + "leOptions\030\250\354\003 \001(\010:5\n\rmarshaler_all\022\034.goo"
                    + "gle.protobuf.FileOptions\030\251\354\003 \001(\010:7\n\017unma"
                    + "rshaler_all\022\034.google.protobuf.FileOption"
                    + "s\030\252\354\003 \001(\010:<\n\024stable_marshaler_all\022\034.goog"
                    + "le.protobuf.FileOptions\030\253\354\003 \001(\010:1\n\tsizer"
                    + "_all\022\034.google.protobuf.FileOptions\030\254\354\003 \001"
                    + "(\010:A\n\031goproto_enum_stringer_all\022\034.google"
                    + ".protobuf.FileOptions\030\255\354\003 \001(\010:9\n\021enum_st"
                    + "ringer_all\022\034.google.protobuf.FileOptions"
                    + "\030\256\354\003 \001(\010:<\n\024unsafe_marshaler_all\022\034.googl"
                    + "e.protobuf.FileOptions\030\257\354\003 \001(\010:>\n\026unsafe"
                    + "_unmarshaler_all\022\034.google.protobuf.FileO"
                    + "ptions\030\260\354\003 \001(\010:B\n\032goproto_extensions_map"
                    + "_all\022\034.google.protobuf.FileOptions\030\261\354\003 \001"
                    + "(\010:@\n\030goproto_unrecognized_all\022\034.google."
                    + "protobuf.FileOptions\030\262\354\003 \001(\010:8\n\020gogoprot"
                    + "o_import\022\034.google.protobuf.FileOptions\030\263"
                    + "\354\003 \001(\010:6\n\016protosizer_all\022\034.google.protob"
                    + "uf.FileOptions\030\264\354\003 \001(\010:3\n\013compare_all\022\034."
                    + "google.protobuf.FileOptions\030\265\354\003 \001(\010:4\n\014t"
                    + "ypedecl_all\022\034.google.protobuf.FileOption"
                    + "s\030\266\354\003 \001(\010:4\n\014enumdecl_all\022\034.google.proto"
                    + "buf.FileOptions\030\267\354\003 \001(\010:<\n\024goproto_regis"
                    + "tration\022\034.google.protobuf.FileOptions\030\270\354"
                    + "\003 \001(\010:7\n\017messagename_all\022\034.google.protob"
                    + "uf.FileOptions\030\271\354\003 \001(\010:=\n\025goproto_sizeca"
                    + "che_all\022\034.google.protobuf.FileOptions\030\272\354"
                    + "\003 \001(\010:;\n\023goproto_unkeyed_all\022\034.google.pr"
                    + "otobuf.FileOptions\030\273\354\003 \001(\010::\n\017goproto_ge"
                    + "tters\022\037.google.protobuf.MessageOptions\030\201"
                    + "\364\003 \001(\010:;\n\020goproto_stringer\022\037.google.prot"
                    + "obuf.MessageOptions\030\203\364\003 \001(\010:8\n\rverbose_e"
                    + "qual\022\037.google.protobuf.MessageOptions\030\204\364"
                    + "\003 \001(\010:/\n\004face\022\037.google.protobuf.MessageO"
                    + "ptions\030\205\364\003 \001(\010:3\n\010gostring\022\037.google.prot"
                    + "obuf.MessageOptions\030\206\364\003 \001(\010:3\n\010populate\022"
                    + "\037.google.protobuf.MessageOptions\030\207\364\003 \001(\010"
                    + ":3\n\010stringer\022\037.google.protobuf.MessageOp"
                    + "tions\030\300\213\004 \001(\010:2\n\007onlyone\022\037.google.protob"
                    + "uf.MessageOptions\030\211\364\003 \001(\010:0\n\005equal\022\037.goo"
                    + "gle.protobuf.MessageOptions\030\215\364\003 \001(\010:6\n\013d"
                    + "escription\022\037.google.protobuf.MessageOpti"
                    + "ons\030\216\364\003 \001(\010:2\n\007testgen\022\037.google.protobuf"
                    + ".MessageOptions\030\217\364\003 \001(\010:3\n\010benchgen\022\037.go"
                    + "ogle.protobuf.MessageOptions\030\220\364\003 \001(\010:4\n\t"
                    + "marshaler\022\037.google.protobuf.MessageOptio"
                    + "ns\030\221\364\003 \001(\010:6\n\013unmarshaler\022\037.google.proto"
                    + "buf.MessageOptions\030\222\364\003 \001(\010:;\n\020stable_mar"
                    + "shaler\022\037.google.protobuf.MessageOptions\030"
                    + "\223\364\003 \001(\010:0\n\005sizer\022\037.google.protobuf.Messa"
                    + "geOptions\030\224\364\003 \001(\010:;\n\020unsafe_marshaler\022\037."
                    + "google.protobuf.MessageOptions\030\227\364\003 \001(\010:="
                    + "\n\022unsafe_unmarshaler\022\037.google.protobuf.M"
                    + "essageOptions\030\230\364\003 \001(\010:A\n\026goproto_extensi"
                    + "ons_map\022\037.google.protobuf.MessageOptions"
                    + "\030\231\364\003 \001(\010:?\n\024goproto_unrecognized\022\037.googl"
                    + "e.protobuf.MessageOptions\030\232\364\003 \001(\010:5\n\npro"
                    + "tosizer\022\037.google.protobuf.MessageOptions"
                    + "\030\234\364\003 \001(\010:2\n\007compare\022\037.google.protobuf.Me"
                    + "ssageOptions\030\235\364\003 \001(\010:3\n\010typedecl\022\037.googl"
                    + "e.protobuf.MessageOptions\030\236\364\003 \001(\010:6\n\013mes"
                    + "sagename\022\037.google.protobuf.MessageOption"
                    + "s\030\241\364\003 \001(\010:<\n\021goproto_sizecache\022\037.google."
                    + "protobuf.MessageOptions\030\242\364\003 \001(\010::\n\017gopro"
                    + "to_unkeyed\022\037.google.protobuf.MessageOpti"
                    + "ons\030\243\364\003 \001(\010:1\n\010nullable\022\035.google.protobu"
                    + "f.FieldOptions\030\351\373\003 \001(\010:.\n\005embed\022\035.google"
                    + ".protobuf.FieldOptions\030\352\373\003 \001(\010:3\n\ncustom"
                    + "type\022\035.google.protobuf.FieldOptions\030\353\373\003 "
                    + "\001(\t:3\n\ncustomname\022\035.google.protobuf.Fiel"
                    + "dOptions\030\354\373\003 \001(\t:0\n\007jsontag\022\035.google.pro"
                    + "tobuf.FieldOptions\030\355\373\003 \001(\t:1\n\010moretags\022\035"
                    + ".google.protobuf.FieldOptions\030\356\373\003 \001(\t:1\n"
                    + "\010casttype\022\035.google.protobuf.FieldOptions"
                    + "\030\357\373\003 \001(\t:0\n\007castkey\022\035.google.protobuf.Fi"
                    + "eldOptions\030\360\373\003 \001(\t:2\n\tcastvalue\022\035.google"
                    + ".protobuf.FieldOptions\030\361\373\003 \001(\t:0\n\007stdtim"
                    + "e\022\035.google.protobuf.FieldOptions\030\362\373\003 \001(\010"
                    + ":4\n\013stdduration\022\035.google.protobuf.FieldO"
                    + "ptions\030\363\373\003 \001(\010:3\n\nwktpointer\022\035.google.pr"
                    + "otobuf.FieldOptions\030\364\373\003 \001(\010BE\n\023com.googl"
                    + "e.protobufB\nGoGoProtosZ\"github.com/gogo/"
                    + "protobuf/gogoproto"
        };
        descriptor =
                com.google.protobuf.Descriptors.FileDescriptor.internalBuildGeneratedFileFrom(
                        descriptorData,
                        new com.google.protobuf.Descriptors.FileDescriptor[] {
                            com.google.protobuf.DescriptorProtos.getDescriptor(),
                        });
        goprotoEnumPrefix.internalInit(descriptor.getExtensions().get(0));
        goprotoEnumStringer.internalInit(descriptor.getExtensions().get(1));
        enumStringer.internalInit(descriptor.getExtensions().get(2));
        enumCustomname.internalInit(descriptor.getExtensions().get(3));
        enumdecl.internalInit(descriptor.getExtensions().get(4));
        enumvalueCustomname.internalInit(descriptor.getExtensions().get(5));
        goprotoGettersAll.internalInit(descriptor.getExtensions().get(6));
        goprotoEnumPrefixAll.internalInit(descriptor.getExtensions().get(7));
        goprotoStringerAll.internalInit(descriptor.getExtensions().get(8));
        verboseEqualAll.internalInit(descriptor.getExtensions().get(9));
        faceAll.internalInit(descriptor.getExtensions().get(10));
        gostringAll.internalInit(descriptor.getExtensions().get(11));
        populateAll.internalInit(descriptor.getExtensions().get(12));
        stringerAll.internalInit(descriptor.getExtensions().get(13));
        onlyoneAll.internalInit(descriptor.getExtensions().get(14));
        equalAll.internalInit(descriptor.getExtensions().get(15));
        descriptionAll.internalInit(descriptor.getExtensions().get(16));
        testgenAll.internalInit(descriptor.getExtensions().get(17));
        benchgenAll.internalInit(descriptor.getExtensions().get(18));
        marshalerAll.internalInit(descriptor.getExtensions().get(19));
        unmarshalerAll.internalInit(descriptor.getExtensions().get(20));
        stableMarshalerAll.internalInit(descriptor.getExtensions().get(21));
        sizerAll.internalInit(descriptor.getExtensions().get(22));
        goprotoEnumStringerAll.internalInit(descriptor.getExtensions().get(23));
        enumStringerAll.internalInit(descriptor.getExtensions().get(24));
        unsafeMarshalerAll.internalInit(descriptor.getExtensions().get(25));
        unsafeUnmarshalerAll.internalInit(descriptor.getExtensions().get(26));
        goprotoExtensionsMapAll.internalInit(descriptor.getExtensions().get(27));
        goprotoUnrecognizedAll.internalInit(descriptor.getExtensions().get(28));
        gogoprotoImport.internalInit(descriptor.getExtensions().get(29));
        protosizerAll.internalInit(descriptor.getExtensions().get(30));
        compareAll.internalInit(descriptor.getExtensions().get(31));
        typedeclAll.internalInit(descriptor.getExtensions().get(32));
        enumdeclAll.internalInit(descriptor.getExtensions().get(33));
        goprotoRegistration.internalInit(descriptor.getExtensions().get(34));
        messagenameAll.internalInit(descriptor.getExtensions().get(35));
        goprotoSizecacheAll.internalInit(descriptor.getExtensions().get(36));
        goprotoUnkeyedAll.internalInit(descriptor.getExtensions().get(37));
        goprotoGetters.internalInit(descriptor.getExtensions().get(38));
        goprotoStringer.internalInit(descriptor.getExtensions().get(39));
        verboseEqual.internalInit(descriptor.getExtensions().get(40));
        face.internalInit(descriptor.getExtensions().get(41));
        gostring.internalInit(descriptor.getExtensions().get(42));
        populate.internalInit(descriptor.getExtensions().get(43));
        stringer.internalInit(descriptor.getExtensions().get(44));
        onlyone.internalInit(descriptor.getExtensions().get(45));
        equal.internalInit(descriptor.getExtensions().get(46));
        description.internalInit(descriptor.getExtensions().get(47));
        testgen.internalInit(descriptor.getExtensions().get(48));
        benchgen.internalInit(descriptor.getExtensions().get(49));
        marshaler.internalInit(descriptor.getExtensions().get(50));
        unmarshaler.internalInit(descriptor.getExtensions().get(51));
        stableMarshaler.internalInit(descriptor.getExtensions().get(52));
        sizer.internalInit(descriptor.getExtensions().get(53));
        unsafeMarshaler.internalInit(descriptor.getExtensions().get(54));
        unsafeUnmarshaler.internalInit(descriptor.getExtensions().get(55));
        goprotoExtensionsMap.internalInit(descriptor.getExtensions().get(56));
        goprotoUnrecognized.internalInit(descriptor.getExtensions().get(57));
        protosizer.internalInit(descriptor.getExtensions().get(58));
        compare.internalInit(descriptor.getExtensions().get(59));
        typedecl.internalInit(descriptor.getExtensions().get(60));
        messagename.internalInit(descriptor.getExtensions().get(61));
        goprotoSizecache.internalInit(descriptor.getExtensions().get(62));
        goprotoUnkeyed.internalInit(descriptor.getExtensions().get(63));
        nullable.internalInit(descriptor.getExtensions().get(64));
        embed.internalInit(descriptor.getExtensions().get(65));
        customtype.internalInit(descriptor.getExtensions().get(66));
        customname.internalInit(descriptor.getExtensions().get(67));
        jsontag.internalInit(descriptor.getExtensions().get(68));
        moretags.internalInit(descriptor.getExtensions().get(69));
        casttype.internalInit(descriptor.getExtensions().get(70));
        castkey.internalInit(descriptor.getExtensions().get(71));
        castvalue.internalInit(descriptor.getExtensions().get(72));
        stdtime.internalInit(descriptor.getExtensions().get(73));
        stdduration.internalInit(descriptor.getExtensions().get(74));
        wktpointer.internalInit(descriptor.getExtensions().get(75));
        com.google.protobuf.DescriptorProtos.getDescriptor();
    }

    // @@protoc_insertion_point(outer_class_scope)
}
