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
package org.apache.seatunnel.transform.structevolution;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.type.SqlType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

public class StructEvolutionConfig implements Serializable {

    public static final Option<List<SpecificModify>> SPECIFIC =
            Options.key("specific")
                    .listType(SpecificModify.class)
                    .noDefaultValue()
                    .withDescription("The specific modify content");

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SpecificModify implements Serializable {
        @JsonAlias("input_name")
        private String inputName;

        @JsonAlias("output_name")
        private String outputName;

        @JsonAlias("columns")
        private List<Column> columns;

        @JsonAlias("primary_key")
        private Primarykey primaryKey;

        @JsonAlias("indexes")
        private List<Index> indexes;

        @JsonAlias("partition")
        private PartitionConfig partition;

        @JsonAlias("comment")
        private Comment comment;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Column implements Serializable {

        @JsonAlias("position")
        private Integer position;

        @JsonAlias("input_name")
        private String inputName;

        @JsonAlias("output_name")
        private String outputName;

        @JsonAlias("data_type")
        private SqlType dataType;

        @JsonAlias("length")
        private Long length;

        @JsonAlias("scale")
        private Integer scale;

        @JsonAlias("nullable")
        private boolean nullable;

        /**
         * Field type in the database For example : varchar is varchar(50),DECIMAL is DECIMAL(20,5)
         */
        @JsonAlias("output_type")
        private String outputType;

        @JsonAlias("default_value")
        private Object defaultValue;

        @JsonAlias("comment")
        private String comment;

        @JsonAlias("action")
        private Action action;

        public Column copy() {
            Column column = new Column();
            column.setPosition(this.position);
            column.setInputName(this.inputName);
            column.setOutputName(this.outputName);
            column.setDataType(this.dataType);
            column.setLength(this.length);
            column.setScale(this.scale);
            column.setNullable(this.nullable);
            column.setOutputType(this.outputType);
            column.setDefaultValue(this.defaultValue);
            column.setComment(this.comment);
            column.setAction(this.action);
            return column;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Primarykey {
        @JsonAlias("input_name")
        private String inputName;

        @JsonAlias("output_name")
        private String outputName;

        @JsonAlias("columns")
        private List<ReferenceColumn> columns;

        @JsonAlias("action")
        private Action action;

        public Primarykey copy() {
            Primarykey primarykey = new Primarykey();
            primarykey.setInputName(this.inputName);
            primarykey.setOutputName(this.outputName);
            primarykey.setColumns(this.columns);
            primarykey.setAction(this.action);
            return primarykey;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Index {

        @JsonAlias("name")
        private String name;

        @JsonAlias("is_unique")
        private boolean isUnique;

        @JsonAlias("columns")
        private List<ReferenceColumn> columns;

        @JsonAlias("action")
        private Action action;

        public Index copy() {
            Index index = new Index();
            index.setName(this.name);
            index.setUnique(this.isUnique);
            index.setColumns(this.columns);
            index.setAction(this.action);
            return index;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Comment {

        @JsonAlias("content")
        private String content;

        @JsonAlias("action")
        private Action action;

        public Comment copy() {
            Comment comment = new Comment();
            comment.setContent(this.content);
            comment.setAction(this.action);
            return comment;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ReferenceColumn {
        @JsonAlias("reference_name")
        private String referenceName;

        @JsonAlias("sort_type")
        private ConstraintKey.ColumnSortType sortType;

        public ConstraintKey.ConstraintKeyColumn toConstraintKeyColumn() {
            return new ConstraintKey.ConstraintKeyColumn(referenceName, sortType);
        }

        public ReferenceColumn copy() {
            ReferenceColumn referenceColumn = new ReferenceColumn();
            referenceColumn.setReferenceName(this.referenceName);
            referenceColumn.setSortType(this.sortType);
            return referenceColumn;
        }
    }

    public static class PartitionConfig {}

    public enum Action {
        ADD,
        DROP,
        MODIFY
    }
}
